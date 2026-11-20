"""Tests for post-collection space reclamation (hole punching).

The tests use real files and the real ``FALLOC_FL_PUNCH_HOLE`` ioctl on the
pytest temp filesystem (ext4/overlay in CI), so ``st_blocks`` actually drops.
A monkeypatched reclaimer additionally exercises the failure/unsupported paths
deterministically.
"""

from __future__ import annotations

import errno
import importlib
import os
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from app.db import Database
from app.reclaim import HolePunchUnsupported, SpaceReclaimer
from app.service import RotationService
from tests.conftest import build_service


def _allocated_blocks(path: Path) -> int:
    return path.stat().st_blocks


def _apparent_size(path: Path) -> int:
    return path.stat().st_size


def _cursor(db_path: Path, file_key: str) -> dict:
    import sqlite3

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        return dict(
            conn.execute(
                "SELECT * FROM cursors WHERE file_key = ?", (file_key,)
            ).fetchone()
        )
    finally:
        conn.close()


# ------------------------------------------------------------- happy paths


def test_reclaim_releases_blocks_but_keeps_length_and_offsets(workspace):
    settings, writer = workspace
    service = build_service(settings, writer)
    # Enough lines to span many 4 KiB blocks.
    writer.append(*[f"line-{i:05d}-" + "x" * 80 for i in range(400)])

    result = service.collect_active(reclaim=True)
    assert result["lines_inserted"] == 400
    reclaim = result["reclaim"]
    assert reclaim["requested"] is True
    assert reclaim["performed"] is True
    assert reclaim["supported"] is True
    assert reclaim["error"] is None

    active = settings.active_path
    blocks = _allocated_blocks(active)
    size = _apparent_size(active)
    # Length and offsets are untouched while allocated blocks dropped sharply.
    assert size == result["offset_after"]
    assert blocks * 512 < size
    # The punched prefix reads back as zeroes; later bytes remain intact.
    with open(active, "rb") as fh:
        head = fh.read(reclaim["block_size"])
        assert head == b"\x00" * reclaim["block_size"]
        # Read past the boundary and into the following complete line.
        fh.seek(reclaim["punched_offset_after"])
        tail = fh.read(reclaim["block_size"])
        assert b"line-" in tail

    # Cursor bookkeeping persisted.
    row = _cursor(settings.db_path, result["file_key"])
    assert row["punched_bytes"] == reclaim["punched_offset_after"]
    assert row["punched_bytes"] <= row["offset_bytes"]
    assert row["punch_last_error"] is None


def test_reclaim_without_flag_does_nothing(workspace):
    settings, writer = workspace
    service = build_service(settings, writer)
    writer.append(*[f"y{i}-" + "z" * 100 for i in range(200)])
    before = _allocated_blocks(settings.active_path)
    result = service.collect_active()  # default: reclaim off
    assert "reclaim" not in result
    assert _allocated_blocks(settings.active_path) == before


def test_recollect_with_no_new_logs_still_reclaims(workspace):
    settings, writer = workspace
    service = build_service(settings, writer)
    # First collect ingests a partial-line tail that pins the cursor inside a
    # block, so the first reclaim has little to release.
    writer.append(*[f"a{i}-" + "q" * 60 for i in range(60)])
    first = service.collect_active(reclaim=True)
    assert first["reclaim"]["requested"] is True
    blocks_after_first = _allocated_blocks(settings.active_path)

    # No new writes at all: a second collection must still make progress (or
    # stay stable) and never duplicate an entry.
    second = service.collect_active(reclaim=True)
    assert second["lines_inserted"] == 0
    assert second["offset_after"] == first["offset_after"]
    assert second["reclaim"]["requested"] is True
    assert _allocated_blocks(settings.active_path) <= blocks_after_first
    entries = service.entries(limit=1000)
    assert len(entries) == 60


def test_partial_tail_block_is_never_punched(workspace):
    settings, writer = workspace
    service = build_service(settings, writer)
    writer.append(*[f"b{i}-" + "w" * 60 for i in range(80)])
    # An unterminated tail: the cursor stops before it; its block stays live.
    with open(settings.active_path, "ab") as fh:
        fh.write(b"half-written-line-without-newline")
        fh.flush()
        os.fsync(fh.fileno())
    result = service.collect_active(reclaim=True)
    row = _cursor(settings.db_path, result["file_key"])
    block_size = result["reclaim"]["block_size"]
    # The punch boundary is block-aligned and strictly before the cursor.
    assert row["punched_bytes"] % block_size == 0
    assert row["punched_bytes"] <= row["offset_bytes"]
    # The block containing the partial tail is still allocated and readable.
    tail_block_start = (row["offset_bytes"] // block_size) * block_size
    with open(settings.active_path, "rb") as fh:
        fh.seek(tail_block_start)
        data = fh.read(block_size)
    assert b"half-written-line" in data

    # Finish the line, collect again: that block can now be reclaimed up to
    # the new committed boundary on a subsequent call.
    with open(settings.active_path, "ab") as fh:
        fh.write(b"-done\n")
        fh.flush()
        os.fsync(fh.fileno())
    again = service.collect_active(reclaim=True)
    assert again["lines_inserted"] == 1
    again_row = _cursor(settings.db_path, result["file_key"])
    assert again_row["punched_bytes"] >= row["punched_bytes"]


def test_writer_keeps_appending_through_same_handle_after_punch(workspace):
    settings, writer = workspace
    service = build_service(settings, writer)
    writer.append(*[f"c{i}-" + "v" * 90 for i in range(300)])
    collected = service.collect_active(reclaim=True)
    assert collected["reclaim"]["performed"] is True
    size_before = _apparent_size(settings.active_path)

    # The original O_APPEND descriptor keeps working; new bytes land after the
    # existing length and are ingested at their original offsets.
    writer.append("after-punch-1", "after-punch-2")
    followup = service.collect_active(reclaim=True)
    assert followup["lines_inserted"] == 2
    assert _apparent_size(settings.active_path) > size_before
    contents = [r["content"] for r in service.entries(limit=1000)]
    assert contents[-2:] == ["after-punch-1", "after-punch-2"]
    # Offsets are contiguous: the unique key guarantees exactly-once.
    row = _cursor(settings.db_path, followup["file_key"])
    assert row["offset_bytes"] == _apparent_size(settings.active_path)


def test_reclaim_during_rotation_targets_the_original_inode_only(workspace):
    settings, writer = workspace
    service = build_service(settings, writer)
    writer.append(*[f"r{i}-" + "u" * 90 for i in range(300)])
    service.collect_active()
    rotation = service.rotate()
    generation = rotation["generation"]
    archive = Path(rotation["archive_path"])
    old_blocks = _allocated_blocks(archive)

    # No active file between rename and close-old-handle.  The reclaimer must
    # follow the identity onto the archive path, not touch anything else.
    assert not settings.active_path.exists()
    response = service.collect_active(reclaim=True)
    assert response["active_file_exists"] is False
    reclaim = response["reclaim"]
    assert reclaim["performed"] is True
    assert Path(reclaim["path"]).resolve() == archive.resolve()
    assert _allocated_blocks(archive) < old_blocks
    assert _apparent_size(archive) == rotation["size_at_rotate"]

    # Late append through the stale handle still works; drain stays exact.
    writer.append("late-after-punch")
    drained = service.drain(generation)
    assert drained["complete"] is True
    contents = [r["content"] for r in service.entries(limit=1000, generation=generation)]
    assert contents[-1] == "late-after-punch"
    assert len(contents) == 301

    # After drain the writer owns a brand-new active file.  The next
    # collection's original file is that new file, so reclaim must follow the
    # current cursor rather than the retired archive (the archive is reclaimed
    # wholesale by cleanup/delete).  Nothing is mis-punched and the archive's
    # apparent length stays equal to its recorded final size.
    assert settings.active_path.exists()
    followup = service.collect_active(reclaim=True)
    assert followup["reclaim"]["requested"] is True
    assert followup["reclaim"]["path"] in (
        str(settings.active_path),
        str(settings.active_path.resolve()),
    )
    assert _apparent_size(archive) == drained["final_size"]


def test_reclaim_survives_collector_restart(workspace):
    settings, writer = workspace
    first = build_service(settings, writer)
    writer.append(*[f"s{i}-" + "t" * 90 for i in range(300)])
    first.collect_active(reclaim=True)
    punched_before = _cursor(
        settings.db_path,
        _cursor_keys(settings.db_path)[0],
    )["punched_bytes"]
    blocks_before = _allocated_blocks(settings.active_path)

    # Restart: a fresh service re-reads the persisted punched boundary and
    # neither re-punches released bytes nor loses the cursor.
    second = build_service(settings, writer)
    result = second.collect_active(reclaim=True)
    assert result["lines_inserted"] == 0
    assert result["reclaim"]["requested"] is True
    keys = _cursor_keys(settings.db_path)
    row = _cursor(settings.db_path, keys[0])
    assert row["punched_bytes"] >= punched_before
    assert _allocated_blocks(settings.active_path) <= blocks_before
    assert len(second.entries(limit=1000)) == 300


def _cursor_keys(db_path: Path) -> list[str]:
    import sqlite3

    conn = sqlite3.connect(db_path)
    try:
        return [r[0] for r in conn.execute("SELECT file_key FROM cursors")]
    finally:
        conn.close()


# ------------------------------------------------------------- failure paths


def test_reclaim_failure_is_reported_but_records_stay_queryable(workspace, monkeypatch):
    settings, writer = workspace
    service = build_service(settings, writer)
    writer.append(*[f"f{i}-" + "m" * 90 for i in range(200)])

    def failing_punch(self, fd, offset, length):
        raise OSError(errno.EIO, "simulated io error")

    monkeypatch.setattr(SpaceReclaimer, "_punch", failing_punch)
    result = service.collect_active(reclaim=True)
    reclaim = result["reclaim"]
    # Ingestion still committed.
    assert result["lines_inserted"] == 200
    assert reclaim["performed"] is False
    assert reclaim["supported"] is True
    assert "simulated io error" in (reclaim["error"] or "")
    assert len(service.entries(limit=1000)) == 200

    # The failure is persisted ...
    row = _cursor(settings.db_path, result["file_key"])
    assert row["punched_bytes"] == 0
    assert "simulated io error" in (row["punch_last_error"] or "")

    # ... and a later retry (failure gone) succeeds, without re-reading logs.
    monkeypatch.undo()
    retry = service.collect_active(reclaim=True)
    assert retry["lines_inserted"] == 0
    assert retry["reclaim"]["performed"] is True
    assert retry["reclaim"]["error"] is None
    row2 = _cursor(settings.db_path, result["file_key"])
    assert row2["punched_bytes"] > 0
    assert row2["punch_last_error"] is None


def test_read_only_file_reports_reclaim_failure_without_a_500(workspace):
    settings, writer = workspace
    service = build_service(settings, writer)
    writer.append(*[f"ro{i}-" + "p" * 90 for i in range(200)])
    os.chmod(settings.active_path, 0o444)
    try:
        result = service.collect_active(reclaim=True)
    finally:
        os.chmod(settings.active_path, 0o644)
    # Ingestion still succeeded; the reclaim section carries the open error.
    assert result["lines_inserted"] == 200
    assert result["reclaim"]["performed"] is False
    assert "O_RDWR" in (result["reclaim"]["error"] or "")
    assert len(service.entries(limit=500)) == 200
    row = _cursor(settings.db_path, result["file_key"])
    assert row["punched_bytes"] == 0
    assert "O_RDWR" in (row["punch_last_error"] or "")

    # Once writable again, a no-new-log collection reclaims successfully.
    retry = service.collect_active(reclaim=True)
    assert retry["reclaim"]["performed"] is True
    assert retry["reclaim"]["error"] is None


def test_unsupported_filesystem_is_marked_and_skips_syscall(workspace, monkeypatch):
    settings, writer = workspace
    service = build_service(settings, writer)
    writer.append(*[f"u{i}-" + "n" * 90 for i in range(200)])

    calls = 0

    def unsupported(self, fd, offset, length):
        nonlocal calls
        calls += 1
        raise HolePunchUnsupported("filesystem does not support punching")

    monkeypatch.setattr(SpaceReclaimer, "_punch", unsupported)
    first = service.collect_active(reclaim=True)
    assert first["reclaim"]["supported"] is False
    assert first["reclaim"]["performed"] is False
    assert calls == 1

    # Second attempt short-circuits from persisted state: no repeated syscall.
    second = service.collect_active(reclaim=True)
    assert second["reclaim"]["supported"] is False
    assert calls == 1
    assert second["lines_inserted"] == 0
    # Ingestion unaffected.
    assert len(service.entries(limit=1000)) == 200


def test_identity_pin_blocks_punch_after_inode_replacement(workspace):
    settings, writer = workspace
    service = build_service(settings, writer)
    writer.append(*[f"p{i}-" + "o" * 90 for i in range(100)])
    collected = service.collect_active(reclaim=True)
    file_key = collected["file_key"]

    # Replace the active path with a different file (new inode): the reclaimer
    # must refuse rather than punch an unrelated file.
    os.unlink(settings.active_path)
    settings.active_path.write_text("brand-new-file\n" * 50)
    response = service.collect_active(reclaim=True)
    # A brand-new file gets its own cursor/key; the old key finds no matching
    # path and reports a skip instead of punching.
    reclaim = response["reclaim"]
    assert reclaim["performed"] is False
    new_size = _apparent_size(settings.active_path)
    assert new_size == len("brand-new-file\n" * 50)
    assert _cursor(settings.db_path, file_key)["punched_bytes"] <= _cursor(
        settings.db_path, file_key
    )["offset_bytes"]


# ------------------------------------------------------------- concurrency


def test_parallel_collections_punch_each_block_once(workspace):
    import threading

    settings, writer = workspace
    service = build_service(settings, writer)
    writer.append(*[f"z{i}-" + "k" * 90 for i in range(400)])

    results: list[dict] = []
    errors: list[BaseException] = []

    def worker() -> None:
        try:
            results.append(service.collect_active(reclaim=True))
        except BaseException as exc:  # noqa: BLE001
            errors.append(exc)

    threads = [threading.Thread(target=worker) for _ in range(6)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert errors == []
    # Exactly one worker inserted lines; others saw zero new lines.
    total_inserted = sum(r["lines_inserted"] for r in results)
    assert total_inserted == 400
    # Every log line exists exactly once.
    entries = service.entries(limit=1000)
    assert len(entries) == 400
    offsets = [e["byte_offset"] for e in entries]
    assert len(offsets) == len(set(offsets))
    # The persisted boundary never passed the committed cursor.
    for key in _cursor_keys(settings.db_path):
        row = _cursor(settings.db_path, key)
        assert row["punched_bytes"] <= row["offset_bytes"]
    assert _allocated_blocks(settings.active_path) * 512 < _apparent_size(
        settings.active_path
    )


def test_two_reclaimer_instances_sharing_one_db_never_regress_boundary(workspace):
    """Two service objects on one SQLite file model separate collector
    processes. Even if both punch, the persisted boundary only advances and
    never passes the committed cursor."""
    settings, writer = workspace
    first = build_service(settings, writer)
    writer.append(*[f"m{i}-" + "q" * 90 for i in range(400)])
    first.collect_active(reclaim=True)

    # A second, independent instance (fresh in-memory state, same database).
    second = build_service(settings, writer)
    key = _cursor_keys(settings.db_path)[0]
    boundary_after_first = _cursor(settings.db_path, key)["punched_bytes"]
    assert boundary_after_first > 0

    # No new logs: the second instance must not move the boundary backwards or
    # past the cursor.
    second.collect_active(reclaim=True)
    row = _cursor(settings.db_path, key)
    assert row["punched_bytes"] >= boundary_after_first
    assert row["punched_bytes"] <= row["offset_bytes"]

    # And after more writes both instances keep the invariant.
    writer.append(*[f"n{i}-" + "r" * 90 for i in range(200)])
    second.collect_active(reclaim=True)
    first.collect_active(reclaim=True)
    row = _cursor(settings.db_path, key)
    assert row["punched_bytes"] <= row["offset_bytes"]
    assert len(first.entries(limit=1000)) == 600


# ------------------------------------------------------------------- HTTP


@pytest.fixture()
def client(workspace, monkeypatch):
    settings, writer = workspace
    db = Database(settings.db_path)
    db.initialize()
    service = RotationService(settings, db, writer)

    main = importlib.import_module("app.main")
    monkeypatch.setattr(main, "settings", settings)
    monkeypatch.setattr(main, "db", db)
    monkeypatch.setattr(main, "service", service)
    with TestClient(main.app) as test_client:
        yield test_client, writer, settings, service


def test_collect_reclaim_query_parameter_and_status_endpoint(client):
    api, writer, settings, _service = client
    writer.append(*[f"h{i}-" + "g" * 90 for i in range(200)])

    plain = api.post("/collect")
    assert "reclaim" not in plain.json()

    writer.append(*[f"h2-{i}-" + "g" * 90 for i in range(200)])
    enabled = api.post("/collect", params={"reclaim": "true"})
    body = enabled.json()
    assert body["reclaim"]["requested"] is True
    assert body["reclaim"]["performed"] is True

    status = api.get("/reclaim").json()
    assert status["count"] >= 1
    item = status["items"][0]
    assert item["punched_bytes"] > 0
    assert item["disk_blocks"] is not None
    assert item["file_size"] == settings.active_path.stat().st_size


def test_http_reclaim_failure_still_returns_records(client, monkeypatch):
    api, writer, _settings, _service = client
    writer.append(*[f"e{i}-" + "d" * 90 for i in range(150)])

    def failing_punch(self, fd, offset, length):
        raise OSError(errno.EROFS, "read-only filesystem simulation")

    monkeypatch.setattr(SpaceReclaimer, "_punch", failing_punch)
    response = api.post("/collect", params={"reclaim": "true"})
    assert response.status_code == 200
    body = response.json()
    assert body["lines_inserted"] == 150
    assert body["reclaim"]["performed"] is False
    assert "read-only" in body["reclaim"]["error"]
    entries = api.get("/entries", params={"limit": 500}).json()
    assert len(entries["items"]) == 150


# ------------------------------------------------------------- old database


def test_schema_migration_adds_reclaim_columns(tmp_path):
    import sqlite3

    db_path = tmp_path / "old.sqlite3"
    # Build a database with the pre-feature cursors layout.
    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            "CREATE TABLE cursors (file_key TEXT PRIMARY KEY, source_id TEXT NOT NULL, "
            "path TEXT, offset_bytes INTEGER NOT NULL DEFAULT 0, "
            "line_number INTEGER NOT NULL DEFAULT 0, updated_at TEXT)"
        )
        conn.execute(
            "INSERT INTO cursors(file_key, source_id, offset_bytes) VALUES ('k', 's', 12345)"
        )
        conn.commit()
    finally:
        conn.close()

    db = Database(db_path)
    db.initialize()
    with db.connect() as migrated:
        row = dict(
            migrated.execute("SELECT * FROM cursors WHERE file_key = 'k'").fetchone()
        )
    assert row["punched_bytes"] == 0
    assert row["punch_supported"] == 1
    assert row["punch_last_error"] is None
