"""Rotation, ingestion and cleanup workflow.

Invariants
----------
1. An archive is registered with the *inode identity* observed at rename time
   and with the size it had then.  Late appends through the stale descriptor
   therefore land in that same identity and are collected into that generation.
2. An archive is drained until it is provably quiescent: the writer reports the
   old handle closed **and** the committed offset equals the final file size.
3. Only then may a delete operation be created.  The operation is bound to one
   generation and pins ``(device, inode, expected_final_size)``.  Immediately
   before unlinking, all three are re-verified plus the writer handle table, so
   a retried request can only ever remove its original file.
4. A new rotation is refused while any earlier archive is not deleted, which is
   what makes "clean up the previous one before starting the next round" true.
5. Deletes are idempotent when the file is already gone, so an interrupted
   cleanup can simply be retried.
6. Optional space reclamation (``POST /collect?reclaim=true``) punches holes
   only into the *original* file of the current collection, only over
   filesystem blocks fully before the SQLite-committed cursor, never changes
   the file's length or offsets, and persists its own advancing boundary so
   interleaved calls, rotations and restarts cannot double-release or mis-aim
   it.  Reclaim failures never roll ingestion back.
"""

from __future__ import annotations

import os
import sqlite3
from pathlib import Path

from .collector import (
    Collector,
    FileIdentityChanged,
    FileShrankError,
    LineTooLongError,
)
from .config import Settings
from .db import Database
from .identity import IdentityRegistry
from .reclaim import SpaceReclaimer
from .writer_client import WriterClient


class WorkflowError(RuntimeError):
    def __init__(self, message: str, status_code: int = 409) -> None:
        super().__init__(message)
        self.status_code = status_code


class RotationService:
    def __init__(self, settings: Settings, db: Database, writer: WriterClient) -> None:
        self.settings = settings
        self.db = db
        self.writer = writer
        self.identities = IdentityRegistry(db)
        self.collector = Collector(db, self.identities, settings.max_batch_bytes)
        self.reclaimer = SpaceReclaimer(db)

    # ---------------------------------------------------------------- setup

    def bootstrap(self) -> None:
        self.settings.archive_dir.mkdir(parents=True, exist_ok=True)
        self.settings.active_path.parent.mkdir(parents=True, exist_ok=True)
        self.collector.ensure_source(self.settings.source_id, self.settings.active_path)
        self._adopt_existing_archives()

    def _adopt_existing_archives(self) -> None:
        """Register archives left on disk by a previous (crashed) run.

        They are registered as ``rotating``; they still have to be drained and
        confirmed closed before they can be deleted.  Existing rows (with their
        cursor state) are preserved, so ingestion resumes exactly where it
        stopped.
        """
        for path in sorted(self.settings.archive_dir.glob(self.settings.archive_glob)):
            if not path.is_file():
                continue
            resolved = str(path.resolve())
            with self.db.connect() as conn:
                if conn.execute(
                    "SELECT 1 FROM archived_files WHERE path = ?", (resolved,)
                ).fetchone():
                    # Already known: its file_key and cursor are authoritative.
                    continue
            try:
                st = os.stat(path)
            except FileNotFoundError:
                continue
            file_key = self.collector.resolve_file_key(
                self.settings.source_id, path, st.st_dev, st.st_ino
            )
            with self.db.transaction() as conn:
                conn.execute(
                    """INSERT OR IGNORE INTO archived_files
                       (source_id, path, original_path, device, inode, file_key,
                        status, size_at_rotate, final_size)
                       VALUES(?, ?, ?, ?, ?, ?, 'rotating', ?, NULL)""",
                    (
                        self.settings.source_id,
                        resolved,
                        str(self.settings.active_path.resolve()),
                        st.st_dev,
                        st.st_ino,
                        file_key,
                        st.st_size,
                    ),
                )

    # -------------------------------------------------------------- helpers

    def _writer_status(self) -> dict:
        try:
            return self.writer.status()
        except Exception as exc:  # noqa: BLE001
            raise WorkflowError(f"writer unavailable: {exc}", 503) from exc

    @staticmethod
    def _writer_holds(device: int, inode: int, writer_status: dict) -> bool:
        for handle in writer_status.get("handles") or []:
            if int(handle.get("device", -1)) == device and int(
                handle.get("inode", -1)
            ) == inode:
                return True
        return False

    def _archive_row(self, generation: int) -> dict:
        with self.db.connect() as conn:
            row = conn.execute(
                "SELECT * FROM archived_files WHERE generation = ?", (generation,)
            ).fetchone()
        if row is None:
            raise WorkflowError(f"archive generation {generation} is not registered", 404)
        return dict(row)

    def _operation_row(self, generation: int) -> dict | None:
        with self.db.connect() as conn:
            row = conn.execute(
                "SELECT * FROM operations WHERE generation = ?", (generation,)
            ).fetchone()
        return dict(row) if row else None

    def _unfinished_archives(self) -> list[dict]:
        with self.db.connect() as conn:
            rows = conn.execute(
                """SELECT generation, status FROM archived_files
                    WHERE status != 'deleted' ORDER BY generation"""
            ).fetchall()
        return [dict(r) for r in rows]

    # ------------------------------------------------------------- rotation

    def rotate(self) -> dict:
        pending = self._unfinished_archives()
        if pending:
            raise WorkflowError(
                "previous archive(s) must be cleaned up before the next rotation: "
                + ", ".join(
                    f"generation {r['generation']} ({r['status']})" for r in pending
                ),
                409,
            )
        try:
            result = self.writer.rotate()
        except Exception as exc:  # noqa: BLE001
            raise WorkflowError(f"writer rotation failed: {exc}", 503) from exc

        active = self.settings.active_path.resolve()
        rotated_from = Path(result["previous_path"]).resolve()
        rotated_to = Path(result["archive_path"]).resolve()
        if rotated_from != active:
            raise WorkflowError(
                f"writer rotated {rotated_from}, expected active path {active}", 409
            )
        if rotated_to.parent != self.settings.archive_dir.resolve() or not rotated_to.is_file():
            raise WorkflowError(f"writer reported unusable archive path {rotated_to}", 409)

        st = os.stat(rotated_to)
        file_key = self.collector.resolve_file_key(
            self.settings.source_id, rotated_to, st.st_dev, st.st_ino
        )
        try:
            with self.db.transaction() as conn:
                cur = conn.execute(
                    """INSERT INTO archived_files
                       (source_id, path, original_path, device, inode, file_key,
                        status, size_at_rotate, final_size)
                       VALUES(?, ?, ?, ?, ?, ?, 'rotating', ?, NULL)""",
                    (
                        self.settings.source_id,
                        str(rotated_to),
                        str(active),
                        st.st_dev,
                        st.st_ino,
                        file_key,
                        st.st_size,
                    ),
                )
                generation = int(cur.lastrowid)
        except sqlite3.IntegrityError as exc:
            # Rotation happened but registration did not (e.g. the same path was
            # already adopted after a crash).  Reuse that row instead of losing
            # the archive.
            with self.db.connect() as conn:
                row = conn.execute(
                    "SELECT * FROM archived_files WHERE path = ?",
                    (str(rotated_to),),
                ).fetchone()
            if row is None:
                raise WorkflowError(f"archive registration failed: {exc}", 500) from exc
            generation = int(row["generation"])
            file_key = str(row["file_key"])
        return {
            "generation": generation,
            "archive_path": str(rotated_to),
            "device": st.st_dev,
            "inode": st.st_ino,
            "size_at_rotate": st.st_size,
            "file_key": file_key,
            "old_handle_still_open": True,
            "next_step": f"POST /archives/{generation}/drain",
        }

    # -------------------------------------------------------------- collect

    def collect_active(self, *, reclaim: bool = False) -> dict:
        """Collect newly appended bytes of the active log, then optionally
        release the blocks of already-collected bytes by hole punching.

        Reclaim is best-effort and fully decoupled from ingestion: the cursor
        commit happens first, and a punch failure is reported on the response
        (and persisted on the cursor row) while every ingested record remains
        queryable.  A collection with no new logs is still useful: with
        ``reclaim=True`` it catches up on blocks a previous call could not
        release, which also works after a process restart.
        """
        path = self.settings.active_path
        if not path.exists():
            # Between a rename and the explicit close-old-handle there is no
            # active file yet.  That is not an error; report it plainly.  A
            # reclaim request can still target the rotated original file via
            # its persisted cursor/identity, so late-appended-and-collected
            # blocks are not stranded just because the active path is absent.
            response = {
                "file_key": None,
                "offset_before": 0,
                "offset_after": 0,
                "size": 0,
                "lines_inserted": 0,
                "duplicates_ignored": 0,
                "complete": True,
                "active_file_exists": False,
                "hint": "writer has not switched to a new file yet; call "
                "/archives/{generation}/drain",
            }
            if reclaim:
                response["reclaim"] = self._reclaim_active_file(collect_result=None)
            return response
        try:
            result = self.collector.ingest_file(self.settings.source_id, path)
        except FileShrankError as exc:
            raise WorkflowError(str(exc), 409) from exc
        except LineTooLongError as exc:
            raise WorkflowError(str(exc), 422) from exc
        response = {
            "file_key": result.file_key,
            "offset_before": result.offset_before,
            "offset_after": result.offset_after,
            "size": result.size,
            "lines_inserted": result.lines_inserted,
            "duplicates_ignored": result.duplicates_ignored,
            "complete": result.complete,
            "active_file_exists": True,
        }
        if reclaim:
            response["reclaim"] = self._reclaim_active_file(collect_result=result)
        return response

    def _reclaim_active_file(self, *, collect_result) -> dict:
        """Reclaim blocks of *this collection's original* active file only.

        The file key and identity come from the just-finished ingestion (or,
        when no active file exists, from the latest live cursor), never from a
        path guess.  During a rotation the original inode keeps living under
        its archive name; registered archive paths for the same identity are
        therefore included as candidates.  Identity pinning inside the
        reclaimer guarantees the punch lands on that inode and no other.
        """
        if collect_result is not None:
            file_key = collect_result.file_key
            expected_identity = IdentityRegistry.make_key_parts(file_key)
        else:
            with self.db.connect() as conn:
                # Prefer a cursor whose identity currently names a registered
                # archive (the file that was just rotated away under its still
                # open handle); only then fall back to the most recently
                # updated live cursor.  This keeps reclaim aimed at the
                # original file of the in-flight collection rather than at an
                # empty successor file the writer may already have created.
                row = conn.execute(
                    """SELECT c.file_key
                         FROM cursors c
                         JOIN file_epochs e ON e.file_key = c.file_key
                         JOIN archived_files a
                           ON a.device = e.device AND a.inode = e.inode
                        WHERE c.source_id = ? AND e.status = 'live'
                          AND a.status != 'deleted'
                        ORDER BY c.updated_at DESC LIMIT 1""",
                    (self.settings.source_id,),
                ).fetchone()
                if row is None:
                    row = conn.execute(
                        """SELECT c.file_key
                             FROM cursors c
                             JOIN file_epochs e ON e.file_key = c.file_key
                            WHERE c.source_id = ? AND e.status = 'live'
                            ORDER BY c.updated_at DESC LIMIT 1""",
                        (self.settings.source_id,),
                    ).fetchone()
            if row is None:
                return {
                    "requested": True,
                    "performed": False,
                    "supported": True,
                    "reason": "no cursor exists for this source yet",
                    "file_key": None,
                    "path": None,
                }
            file_key = str(row["file_key"])
            expected_identity = IdentityRegistry.make_key_parts(file_key)

        candidates = [self.settings.active_path]
        if expected_identity is not None:
            device, inode = expected_identity
            with self.db.connect() as conn:
                archive_rows = conn.execute(
                    "SELECT path FROM archived_files WHERE device = ? AND inode = ?",
                    (device, inode),
                ).fetchall()
            candidates.extend(Path(r["path"]) for r in archive_rows)
        return self.reclaimer.reclaim_file(
            file_key=file_key,
            candidate_paths=candidates,
            expected_identity=expected_identity,
        ).as_dict()

    # --------------------------------------------------------------- drain

    def drain(self, generation: int, *, close_handle: bool = True) -> dict:
        archive = self._archive_row(generation)
        if archive["status"] == "deleted":
            raise WorkflowError(f"generation {generation} was already deleted", 409)
        path = Path(archive["path"])
        device, inode = int(archive["device"]), int(archive["inode"])

        if not path.exists():
            # The file vanished (e.g. an interrupted delete unlinked it before
            # the DB commit, or it was removed out of band).  If we already
            # recorded a closed handle and a committed ingestion, there is
            # nothing left to drain.
            if archive["status"] == "ingested" and archive["handle_closed"]:
                return {
                    "generation": generation,
                    "handle_closed_now": False,
                    "handle_closed": True,
                    "final_size": int(archive["final_size"] or 0),
                    "ingested_offset": int(archive["final_size"] or 0),
                    "file_key": archive["file_key"],
                    "complete": True,
                    "lines_inserted": 0,
                    "duplicates_ignored": 0,
                    "close_result": None,
                    "archive_file_exists": False,
                }
            raise WorkflowError(
                f"archive file {path} is missing; cannot drain generation {generation}",
                409,
            )

        closed_now = False
        close_info: dict | None = None
        if close_handle:
            # Safety pre-check: only close the descriptor when it is *this*
            # archive's.  Closing the wrong generation would silently end the
            # late-append window for another round.
            pre_status = self._writer_status()
            held = [
                h
                for h in pre_status.get("handles") or []
                if int(h.get("device", -1)) == device
                and int(h.get("inode", -1)) == inode
            ]
            other_archives = [
                h
                for h in pre_status.get("handles") or []
                if not (
                    int(h.get("device", -1)) == device
                    and int(h.get("inode", -1)) == inode
                )
                and (h.get("path") or "")
                and Path(str(h["path"])).resolve() != self.settings.active_path.resolve()
            ]
            if not held and other_archives:
                raise WorkflowError(
                    "writer's old handle belongs to a different archive "
                    f"({other_archives[0].get('path')}); refusing to close it "
                    f"while draining generation {generation}",
                    409,
                )
            if held:
                try:
                    close_info = self.writer.close_handle()
                except Exception as exc:  # noqa: BLE001
                    raise WorkflowError(f"could not close old handle: {exc}", 503) from exc
                closed_now = bool(close_info.get("closed"))
                if closed_now and (
                    int(close_info.get("closed_device", -1)) != device
                    or int(close_info.get("closed_inode", -1)) != inode
                ):
                    raise WorkflowError(
                        "writer closed a handle that is not this archive "
                        f"({close_info.get('closed_inode')} != {inode}); "
                        "refusing to mark it final",
                        409,
                    )

        writer_status = self._writer_status()
        handle_closed = not self._writer_holds(device, inode, writer_status)

        try:
            result = self.collector.drain_to_end(
                self.settings.source_id,
                path,
                expected_identity=(device, inode),
                final=handle_closed,
                file_key=archive["file_key"],
            )
        except FileShrankError as exc:
            raise WorkflowError(str(exc), 409) from exc
        except FileIdentityChanged as exc:
            raise WorkflowError(str(exc), 409) from exc
        except LineTooLongError as exc:
            raise WorkflowError(str(exc), 422) from exc

        final_size = path.stat().st_size if path.exists() else result.size
        complete = (
            handle_closed
            and result.complete
            and result.offset_after == final_size
            and result.offset_after == result.size
        )
        with self.db.transaction() as conn:
            conn.execute(
                """UPDATE archived_files SET
                       final_size = ?,
                       handle_closed = ?,
                       handle_closed_at = CASE WHEN ? THEN
                           coalesce(handle_closed_at, strftime('%Y-%m-%dT%H:%M:%fZ','now'))
                           ELSE handle_closed_at END,
                       ingested_at = CASE WHEN ? THEN
                           coalesce(ingested_at, strftime('%Y-%m-%dT%H:%M:%fZ','now'))
                           ELSE ingested_at END,
                       status = CASE WHEN ? THEN 'ingested' ELSE status END
                     WHERE generation = ?""",
                (
                    final_size,
                    1 if handle_closed else 0,
                    1 if handle_closed else 0,
                    1 if complete else 0,
                    1 if complete else 0,
                    generation,
                ),
            )
        return {
            "generation": generation,
            "handle_closed_now": closed_now,
            "handle_closed": handle_closed,
            "final_size": final_size,
            "ingested_offset": result.offset_after,
            "file_key": result.file_key,
            "complete": complete,
            "lines_inserted": result.lines_inserted,
            "duplicates_ignored": result.duplicates_ignored,
            "close_result": close_info,
        }

    # -------------------------------------------------------------- delete

    def request_delete(self, generation: int) -> dict:
        archive = self._archive_row(generation)
        if archive["status"] == "deleted":
            return {
                "generation": generation,
                "status": "deleted",
                "already_deleted": True,
                "path": archive["path"],
            }
        if not archive["handle_closed"] or archive["status"] != "ingested":
            raise WorkflowError(
                f"generation {generation} is not ready for deletion "
                f"(handle_closed={bool(archive['handle_closed'])}, "
                f"status={archive['status']}); drain it first",
                409,
            )
        older = [
            r
            for r in self._unfinished_archives()
            if int(r["generation"]) < generation
        ]
        if older:
            raise WorkflowError(
                "older archive generations must be deleted first: "
                + ", ".join(str(r["generation"]) for r in older),
                409,
            )
        with self.db.transaction() as conn:
            # Path and identity are pinned forever.  The expected size may only
            # advance when the archive has been ingested again (a re-drain
            # updates ``final_size``); it is never taken from a fresh stat, so a
            # file that grew without being collected still fails verification.
            conn.execute(
                """INSERT INTO operations
                   (generation, source_id, path, device, inode,
                    expected_final_size, status)
                   VALUES(?, ?, ?, ?, ?, ?, 'pending')
                   ON CONFLICT(generation) DO UPDATE SET
                       status = CASE WHEN operations.status = 'completed'
                                     THEN operations.status ELSE 'pending' END,
                       expected_final_size = CASE
                           WHEN operations.status = 'completed'
                           THEN operations.expected_final_size
                           ELSE excluded.expected_final_size END,
                       last_error = NULL,
                       updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')""",
                (
                    generation,
                    archive["source_id"],
                    archive["path"],
                    archive["device"],
                    archive["inode"],
                    int(archive["final_size"] or 0),
                ),
            )
        return self.run_delete(generation)

    def run_delete(self, generation: int) -> dict:
        archive = self._archive_row(generation)
        op = self._operation_row(generation)
        if op is None:
            raise WorkflowError(
                f"no delete operation exists for generation {generation}", 409
            )
        if op["status"] == "completed":
            return {
                "generation": generation,
                "operation_id": op["id"],
                "status": "deleted",
                "retried": False,
                "already_deleted": True,
                "path": op["path"],
            }
        with self.db.transaction() as conn:
            conn.execute(
                """UPDATE operations SET status = 'in_progress',
                          attempts = attempts + 1,
                          updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')
                     WHERE id = ?""",
                (op["id"],),
            )
        try:
            self._unlink_verified(op)
        except WorkflowError as exc:
            with self.db.transaction() as conn:
                conn.execute(
                    """UPDATE operations SET status = 'failed', last_error = ?,
                              updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')
                         WHERE id = ?""",
                    (str(exc), op["id"]),
                )
            raise
        except Exception as exc:  # noqa: BLE001
            with self.db.transaction() as conn:
                conn.execute(
                    """UPDATE operations SET status = 'failed', last_error = ?,
                              updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')
                         WHERE id = ?""",
                    (str(exc), op["id"]),
                )
            raise
        with self.db.transaction() as conn:
            conn.execute(
                """UPDATE operations SET status = 'completed', last_error = NULL,
                          completed_at = strftime('%Y-%m-%dT%H:%M:%fZ','now'),
                          updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')
                     WHERE id = ?""",
                (op["id"],),
            )
            conn.execute(
                """UPDATE archived_files SET status = 'deleted',
                          deleted_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')
                     WHERE generation = ?""",
                (generation,),
            )
            self.identities.retire(conn, archive["file_key"])
        return {
            "generation": generation,
            "operation_id": op["id"],
            "status": "deleted",
            "retried": int(op["attempts"]) > 0,
            "path": op["path"],
        }

    def _unlink_verified(self, op: dict) -> None:
        path = Path(op["path"])
        try:
            st = os.stat(path)
        except FileNotFoundError:
            return  # a previous attempt already unlinked it: idempotent success
        if st.st_dev != int(op["device"]) or st.st_ino != int(op["inode"]):
            raise WorkflowError(
                "path now refers to a different file than this operation's "
                "generation; refusing to delete",
                409,
            )
        if st.st_size != int(op["expected_final_size"]):
            raise WorkflowError(
                f"archive grew after ingestion (expected {op['expected_final_size']} "
                f"bytes, found {st.st_size}); refusing to delete",
                409,
            )
        writer_status = self._writer_status()
        if self._writer_holds(st.st_dev, st.st_ino, writer_status):
            raise WorkflowError("writer still holds the archive open", 409)
        try:
            os.unlink(path)
        except FileNotFoundError:
            return

    # ---------------------------------------------------------- inspection

    def archives(self, limit: int = 100) -> list[dict]:
        with self.db.connect() as conn:
            rows = conn.execute(
                """SELECT a.*, o.id AS operation_id, o.status AS operation_status,
                          o.attempts AS operation_attempts,
                          o.last_error AS operation_last_error
                     FROM archived_files a
                     LEFT JOIN operations o ON o.generation = a.generation
                    ORDER BY a.generation DESC LIMIT ?""",
                (limit,),
            ).fetchall()
        return [dict(r) for r in rows]

    def archive(self, generation: int) -> dict:
        with self.db.connect() as conn:
            row = conn.execute(
                """SELECT a.*, o.id AS operation_id, o.status AS operation_status,
                          o.attempts AS operation_attempts,
                          o.last_error AS operation_last_error
                     FROM archived_files a
                     LEFT JOIN operations o ON o.generation = a.generation
                    WHERE a.generation = ?""",
                (generation,),
            ).fetchone()
        if row is None:
            raise WorkflowError(f"archive generation {generation} is not registered", 404)
        return dict(row)

    def entries(
        self,
        *,
        limit: int = 50,
        after_id: int = 0,
        generation: int | None = None,
        source_id: str | None = None,
    ) -> list[dict]:
        params: list[object] = [after_id]
        sql = "SELECT * FROM log_entries WHERE id > ?"
        if generation is not None:
            archive = self._archive_row(generation)
            sql += " AND file_key = ?"
            params.append(archive["file_key"])
        if source_id is not None:
            sql += " AND source_id = ?"
            params.append(source_id)
        sql += " ORDER BY id ASC LIMIT ?"
        params.append(max(1, min(limit, 1000)))
        with self.db.connect() as conn:
            rows = conn.execute(sql, params).fetchall()
        return [dict(r) for r in rows]

    def reclaim_status(self) -> list[dict]:
        """Persisted hole-punch progress for every known file cursor."""
        with self.db.connect() as conn:
            rows = conn.execute(
                """SELECT c.file_key, c.source_id, c.path,
                          c.offset_bytes AS committed_offset,
                          c.punched_bytes, c.punch_supported,
                          c.punch_last_error, c.updated_at,
                          e.device, e.inode, e.epoch, e.status AS epoch_status
                     FROM cursors c
                     LEFT JOIN file_epochs e ON e.file_key = c.file_key
                    ORDER BY c.updated_at DESC"""
            ).fetchall()
        items = [dict(r) for r in rows]
        for item in items:
            path = Path(item["path"]) if item.get("path") else None
            item["file_exists"] = bool(path and path.exists())
            if path and path.exists():
                try:
                    st = path.stat()
                    item["file_size"] = st.st_size
                    item["disk_blocks"] = st.st_blocks
                    item["disk_bytes"] = st.st_blocks * 512
                except OSError:  # pragma: no cover - racy unlink
                    item["file_size"] = None
                    item["disk_blocks"] = None
                    item["disk_bytes"] = None
            else:
                item["file_size"] = None
                item["disk_blocks"] = None
                item["disk_bytes"] = None
        return items
