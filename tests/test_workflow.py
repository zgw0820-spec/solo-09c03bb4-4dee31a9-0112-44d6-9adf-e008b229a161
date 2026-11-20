"""Behavioural tests for rotation, late appends, restart and cleanup retries."""

from __future__ import annotations

import os
import sqlite3
from pathlib import Path

import pytest

from app.db import Database
from app.service import RotationService, WorkflowError
from tests.conftest import build_service


def contents(service: RotationService, generation: int | None = None) -> list[str]:
    rows = service.entries(limit=1000, generation=generation)
    return [r["content"] for r in rows]


def count_entries(db_path: Path) -> int:
    conn = sqlite3.connect(db_path)
    try:
        return int(conn.execute("SELECT count(*) FROM log_entries").fetchone()[0])
    finally:
        conn.close()


# --------------------------------------------------------------- basic flow


def test_collect_is_incremental_and_idempotent(workspace):
    settings, writer = workspace
    service = build_service(settings, writer)
    writer.append("one", "two")

    first = service.collect_active()
    assert first["lines_inserted"] == 2
    assert first["offset_after"] == len("one\ntwo\n")

    # No new bytes: a repeat call must not duplicate anything.
    second = service.collect_active()
    assert second["lines_inserted"] == 0
    assert contents(service) == ["one", "two"]

    writer.append("three")
    third = service.collect_active()
    assert third["lines_inserted"] == 1
    assert contents(service) == ["one", "two", "three"]


def test_long_line_larger_than_batch_is_ingested_whole(workspace):
    settings, writer = workspace
    service = build_service(settings, writer)
    service.collector.max_batch_bytes = 64
    line = "x" * 500
    writer.append(line)
    service.collect_active()
    assert contents(service) == [line]


def test_partial_tail_after_restart_is_completed(workspace):
    settings, writer = workspace
    first = build_service(settings, writer)
    writer.append("line-1")
    first.collect_active()
    # Half a line is written, then the collector restarts.
    with open(settings.active_path, "ab") as fh:
        fh.write(b"line-2-part")
        fh.flush()
        os.fsync(fh.fileno())
    second = build_service(settings, writer)
    assert second.collect_active()["lines_inserted"] == 0
    with open(settings.active_path, "ab") as fh:
        fh.write(b"-done\n")
        fh.flush()
        os.fsync(fh.fileno())
    assert second.collect_active()["lines_inserted"] == 1
    assert contents(second) == ["line-1", "line-2-part-done"]


def test_partial_tail_is_not_split(workspace):
    settings, writer = workspace
    service = build_service(settings, writer)
    writer.append("complete")
    service.collect_active()

    # Simulate a write cut in half mid-line.
    with open(settings.active_path, "ab") as fh:
        fh.write(b"half-a-line")
        fh.flush()
        os.fsync(fh.fileno())
    result = service.collect_active()
    assert result["lines_inserted"] == 0
    assert contents(service) == ["complete"]

    # Writer finishes the line later.
    with open(settings.active_path, "ab") as fh:
        fh.write(b"-finished\n")
        fh.flush()
        os.fsync(fh.fileno())
    result = service.collect_active()
    assert result["lines_inserted"] == 1
    assert contents(service) == ["complete", "half-a-line-finished"]


# ------------------------------------------------------- late appends/rename


def test_late_appends_after_rename_are_collected_in_old_generation(workspace):
    settings, writer = workspace
    service = build_service(settings, writer)
    writer.append("before-1", "before-2")
    service.collect_active()

    rotation = service.rotate()
    generation = rotation["generation"]
    archive = Path(rotation["archive_path"])
    assert archive.exists()
    assert not settings.active_path.exists()

    # Old descriptor still open: these bytes land in the archive, not the new file.
    writer.append("late-1", "late-2")
    assert archive.stat().st_size > rotation["size_at_rotate"]

    drain = service.drain(generation)
    assert drain["handle_closed"] is True
    assert drain["complete"] is True
    assert drain["final_size"] == archive.stat().st_size
    assert contents(service, generation) == [
        "before-1",
        "before-2",
        "late-1",
        "late-2",
    ]
    # The new active file is empty and starts in a different generation.
    assert settings.active_path.exists()
    writer.append("new-1")
    service.collect_active()
    assert contents(service, generation) == [
        "before-1",
        "before-2",
        "late-1",
        "late-2",
    ]
    assert "new-1" in contents(service)


def test_drain_without_close_does_not_finalize(workspace):
    settings, writer = workspace
    service = build_service(settings, writer)
    writer.append("a")
    service.collect_active()
    generation = service.rotate()["generation"]

    writer.append("b")
    partial = service.drain(generation, close_handle=False)
    assert partial["handle_closed"] is False
    assert partial["complete"] is False
    with pytest.raises(WorkflowError) as exc:
        service.request_delete(generation)
    assert exc.value.status_code == 409

    final = service.drain(generation)
    assert final["complete"] is True
    assert contents(service, generation) == ["a", "b"]


# ---------------------------------------------------------- restart/recovery


def test_collector_restart_resumes_without_duplicates(workspace):
    settings, writer = workspace
    first = build_service(settings, writer)
    writer.append("x", "y")
    first.collect_active()
    generation = first.rotate()["generation"]
    writer.append("late-after-rotate")

    # Process "crashes" here: nothing is drained, then a new service instance
    # starts against the same database and directory.
    second = build_service(settings, writer)
    # Bootstrap adopted the on-disk archive rather than losing it.
    generations = {a["generation"] for a in second.archives()}
    assert generation in generations
    assert second.archive(generation)["status"] == "rotating"

    drain = second.drain(generation)
    assert drain["complete"] is True
    assert contents(second, generation) == ["x", "y", "late-after-rotate"]


def test_restart_after_drain_does_not_duplicate_on_reread(workspace):
    settings, writer = workspace
    first = build_service(settings, writer)
    writer.append("a", "b")
    first.collect_active()
    generation = first.rotate()["generation"]
    writer.append("c")
    first.drain(generation)
    assert count_entries(settings.db_path) == 3

    second = build_service(settings, writer)
    again = second.drain(generation)
    assert again["lines_inserted"] == 0
    assert again["duplicates_ignored"] == 0
    assert count_entries(settings.db_path) == 3


def test_restart_then_late_append_is_collected(workspace):
    """Collector restarts while the writer still holds the old handle."""
    settings, writer = workspace
    first = build_service(settings, writer)
    writer.append("early")
    first.collect_active()
    generation = first.rotate()["generation"]
    first.collect_active()  # nothing to read; no active file during rotation

    # Collector process restarts; the writer (separate process in reality)
    # keeps the stale descriptor open.
    second = build_service(settings, writer)
    writer.append("late-after-restart-1", "late-after-restart-2")
    drained = second.drain(generation)
    assert drained["complete"] is True
    assert contents(second, generation) == [
        "early",
        "late-after-restart-1",
        "late-after-restart-2",
    ]
    # And a second restart re-draining stays exactly-once.
    third = build_service(settings, writer)
    again = third.drain(generation)
    assert again["lines_inserted"] == 0
    assert count_entries(settings.db_path) == 3


def test_writer_restart_loses_old_handle_and_archive_becomes_final(workspace):
    """If the writer process dies, its old descriptor dies with it."""
    settings, writer = workspace
    service = build_service(settings, writer)
    writer.append("a")
    service.collect_active()
    generation = service.rotate()["generation"]
    writer.append("before-writer-crash")
    archive_path = Path(service.archive(generation)["path"])
    expected = archive_path.read_bytes()

    # Writer process exits: fd is gone, but the archive file remains.
    writer.close()
    dead_writer = type("DeadWriter", (), {"status": lambda self: {"handles": []}})()

    restarted = build_service(settings, dead_writer)
    drained = restarted.drain(generation)
    assert drained["handle_closed"] is True
    assert drained["complete"] is True
    assert contents(restarted, generation) == ["a", "before-writer-crash"]
    assert archive_path.read_bytes() == expected

    deleted = restarted.request_delete(generation)
    assert deleted["status"] == "deleted"


def test_drain_refuses_to_close_another_generation_handle(workspace):
    settings, writer = workspace
    service = build_service(settings, writer)
    writer.append("a")
    service.collect_active()
    gen1 = service.rotate()["generation"]
    service.drain(gen1)
    service.request_delete(gen1)

    writer.append("b")
    service.collect_active()
    gen2 = service.rotate()["generation"]
    archive2 = service.archive(gen2)

    # Pretend the writer's only stale handle is still gen1's (e.g. a buggy or
    # racing writer).  Draining gen2 must not close it.
    closed: list[dict] = []

    def status() -> dict:
        return {
            "state": "rotated",
            "active_path": str(settings.active_path),
            "handles": [
                {
                    "path": str(service.archive(gen1)["path"]),
                    "device": service.archive(gen1)["device"],
                    "inode": service.archive(gen1)["inode"],
                }
            ],
        }

    writer.status = status
    writer.close_handle = lambda: closed.append({"called": True}) or {"closed": True}
    with pytest.raises(WorkflowError) as exc:
        service.drain(gen2)
    assert exc.value.status_code == 409
    assert closed == []
    assert Path(archive2["path"]).exists()


def test_reused_inode_gets_a_new_epoch(tmp_path):
    """An inode reused by a later file must not inherit the old cursor."""
    db = Database(tmp_path / "db.sqlite3")
    db.initialize()
    from app.identity import IdentityRegistry

    registry = IdentityRegistry(db)
    with db.transaction() as conn:
        key1 = registry.resolve(conn, "s", 1, 42)
        registry.retire(conn, key1)
        key2 = registry.resolve(conn, "s", 1, 42)
    assert key1 != key2
    assert key2.endswith(":2")


# ------------------------------------------------------------ cleanup rules


def test_delete_requires_closed_handle_and_ignested_archive(workspace):
    settings, writer = workspace
    service = build_service(settings, writer)
    writer.append("a")
    service.collect_active()
    generation = service.rotate()["generation"]

    with pytest.raises(WorkflowError) as exc:
        service.request_delete(generation)
    assert exc.value.status_code == 409
    assert "not ready" in str(exc.value)

    service.drain(generation)
    result = service.request_delete(generation)
    assert result["status"] == "deleted"
    archive = service.archive(generation)
    assert archive["status"] == "deleted"
    assert not Path(archive["path"]).exists()


def test_next_rotation_waits_for_previous_cleanup(workspace):
    settings, writer = workspace
    service = build_service(settings, writer)
    writer.append("a")
    service.collect_active()
    generation = service.rotate()["generation"]
    writer.append("late")
    service.drain(generation)

    with pytest.raises(WorkflowError) as exc:
        service.rotate()
    assert exc.value.status_code == 409
    assert "cleaned up" in str(exc.value)

    service.request_delete(generation)
    second = service.rotate()
    assert second["generation"] == generation + 1


def test_delete_refuses_retargeted_path(workspace, monkeypatch):
    settings, writer = workspace
    service = build_service(settings, writer)
    writer.append("a")
    service.collect_active()
    generation = service.rotate()["generation"]
    service.drain(generation)
    archive_path = Path(service.archive(generation)["path"])

    # Replace the archive path with a *different* file (new inode).
    os.unlink(archive_path)
    archive_path.write_text("impostor\n")

    with pytest.raises(WorkflowError) as exc:
        service.request_delete(generation)
    assert exc.value.status_code == 409
    assert archive_path.exists()


def test_delete_refuses_archive_that_grew_after_ingestion(workspace):
    settings, writer = workspace
    service = build_service(settings, writer)
    writer.append("a")
    service.collect_active()
    generation = service.rotate()["generation"]
    service.drain(generation)
    path = Path(service.archive(generation)["path"])

    # Bytes arriving after the writer closed the handle must be caught by the
    # expected-size check rather than silently deleted.
    with open(path, "ab") as fh:
        fh.write(b"unexpected\n")
    with pytest.raises(WorkflowError) as exc:
        service.request_delete(generation)
    assert exc.value.status_code == 409
    assert "grew" in str(exc.value) or "expected" in str(exc.value)
    assert path.exists()


def test_stale_request_cannot_delete_next_generation(workspace):
    settings, writer = workspace
    service = build_service(settings, writer)
    writer.append("a")
    service.collect_active()
    gen1 = service.rotate()["generation"]
    service.drain(gen1)
    service.request_delete(gen1)

    writer.append("b")
    service.collect_active()
    gen2 = service.rotate()["generation"]
    service.drain(gen2)
    path2 = Path(service.archive(gen2)["path"])

    # A retry aimed at gen1 must not touch gen2's file.
    retried = service.request_delete(gen1)
    assert retried["already_deleted"] is True
    assert path2.exists()


# ------------------------------------------------------ interrupted cleanup


def test_interrupted_delete_is_retryable(workspace, monkeypatch):
    settings, writer = workspace
    service = build_service(settings, writer)
    writer.append("a", "b")
    service.collect_active()
    generation = service.rotate()["generation"]
    service.drain(generation)

    real_unlink = os.unlink

    def crash_after_unlink(path):
        real_unlink(path)
        raise OSError("simulated crash after unlink")

    monkeypatch.setattr(os, "unlink", crash_after_unlink)
    with pytest.raises(OSError):
        service.request_delete(generation)
    monkeypatch.setattr(os, "unlink", real_unlink)

    # The file is already gone; operation is marked failed, not completed.
    op = service._operation_row(generation)
    assert op["status"] == "failed"
    assert not Path(service.archive(generation)["path"]).exists()

    # Retry is idempotent and finishes the operation.
    result = service.run_delete(generation)
    assert result["status"] == "deleted"
    assert service._operation_row(generation)["status"] == "completed"
    assert service.archive(generation)["status"] == "deleted"


def test_failed_verification_can_be_retried_after_fixing(workspace):
    settings, writer = workspace
    service = build_service(settings, writer)
    writer.append("a")
    service.collect_active()
    generation = service.rotate()["generation"]
    service.drain(generation)
    archive = service.archive(generation)
    path = Path(archive["path"])

    # Make the writer claim the archive is still open -> verification fails.
    original_status = writer.status
    writer.status = lambda: {
        **original_status(),
        "handles": [
            {
                "path": str(path),
                "device": archive["device"],
                "inode": archive["inode"],
            }
        ],
    }
    with pytest.raises(WorkflowError):
        service.request_delete(generation)
    assert service._operation_row(generation)["status"] == "failed"
    assert path.exists()

    # Handle goes away; the retry succeeds and deletes only this file.
    writer.status = original_status
    result = service.run_delete(generation)
    assert result["status"] == "deleted"
    assert not path.exists()


def test_restart_marks_interrupted_operation_retryable(workspace):
    settings, writer = workspace
    service = build_service(settings, writer)
    writer.append("a")
    service.collect_active()
    generation = service.rotate()["generation"]
    service.drain(generation)

    # Simulate a crash while the delete was in progress.
    service.request_delete(generation)  # completes normally first
    db = Database(settings.db_path)
    with db.transaction() as conn:
        conn.execute(
            "UPDATE operations SET status = 'in_progress', completed_at = NULL WHERE generation = ?",
            (generation,),
        )
        conn.execute(
            "UPDATE archived_files SET status = 'ingested', deleted_at = NULL WHERE generation = ?",
            (generation,),
        )
    # The file is gone; the retry path must cope with that.
    recovered = db.recover_interrupted_operations()
    assert recovered == 1
    assert service._operation_row(generation)["status"] == "failed"

    result = service.run_delete(generation)
    assert result["status"] == "deleted"
