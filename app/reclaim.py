"""Space reclamation for already-collected log bytes (hole punching).

When the writer keeps a log file open and keeps appending, the bytes that the
collector has already committed to SQLite still occupy filesystem blocks.  On
Linux filesystems that support ``FALLOC_FL_PUNCH_HOLE`` those blocks can be
released *in place*: the file keeps its length and every byte offset, reads of
the punched range return zeroes, and a writer holding the original
``O_APPEND`` descriptor keeps appending exactly as before.

Safety model
------------
* Only **complete filesystem blocks fully before the committed cursor** are
  punched.  The block straddling the cursor (which may contain bytes not yet
  ingested, including a half-written line) is never touched, and neither is
  anything beyond the cursor.  ``FALLOC_FL_KEEP_SIZE`` additionally guarantees
  the apparent file size never changes.
* The reclaimer opens the target itself and pins it to the cursor's recorded
  ``(device, inode)`` identity before punching, so a path that rotation (or
  inode reuse) repointed at another file can never be punched.
* The released boundary ``punched_bytes`` is persisted in SQLite and can only
  move forward, inside the same transaction that records the attempt.  A
  second, interleaved or restarted collection therefore never re-releases a
  range and a "collect with no new logs" call simply reclaims whatever the
  previous call could not.
* A failed reclaim is reported and persisted; ingestion itself has already
  committed, so every collected record stays queryable.
"""

from __future__ import annotations

import ctypes
import ctypes.util
import errno
import os
import threading
from dataclasses import dataclass
from pathlib import Path

from .db import Database
from .identity import IdentityRegistry

# From linux/falloc.h.
FALLOC_FL_KEEP_SIZE = 0x01
FALLOC_FL_PUNCH_HOLE = 0x02
# Errno values that mean "this filesystem/object cannot do hole punching"; any
# other OSError is a genuine failure worth surfacing.
_UNSUPPORTED_ERRNOS = frozenset(
    {
        errno.EOPNOTSUPP,
        errno.ENOSYS,
        errno.ENOTTY,
        errno.EINVAL,
        errno.ENODEV,
    }
)


class HolePunchUnsupported(RuntimeError):
    """Raised once when the running kernel/filesystem cannot punch holes."""


@dataclass
class ReclaimResult:
    file_key: str | None
    path: str | None
    requested: bool = False
    performed: bool = False
    supported: bool = True
    punched_offset_before: int = 0
    punched_offset_after: int = 0
    block_size: int = 0
    bytes_reclaimed: int = 0
    disk_blocks_before: int = 0
    disk_blocks_after: int = 0
    file_size: int = 0
    reason: str | None = None
    error: str | None = None

    def as_dict(self) -> dict:
        return {
            "requested": self.requested,
            "performed": self.performed,
            "supported": self.supported,
            "file_key": self.file_key,
            "path": self.path,
            "punched_offset_before": self.punched_offset_before,
            "punched_offset_after": self.punched_offset_after,
            "block_size": self.block_size,
            "bytes_reclaimed": self.bytes_reclaimed,
            "disk_blocks_before": self.disk_blocks_before,
            "disk_blocks_after": self.disk_blocks_after,
            "file_size": self.file_size,
            "reason": self.reason,
            "error": self.error,
        }


@dataclass
class _Libc:
    lib: ctypes.CDLL | None = None
    load_error: str | None = None
    probe_done: bool = False
    supported: bool = True
    unsupported_reason: str | None = None


class SpaceReclaimer:
    """Punches holes behind the committed cursor of one file identity."""

    def __init__(self, db: Database) -> None:
        self.db = db
        self._libc = _Libc()
        # Serializes reclaim attempts for one file_key inside this process so
        # interleaved /collect calls cannot duplicate the syscall work.  Other
        # processes are still safe: the database update is conditional and the
        # syscall itself is idempotent.
        self._locks: dict[str, threading.Lock] = {}
        self._locks_guard = threading.Lock()

    # ------------------------------------------------------------- libc setup

    def _ensure_libc(self) -> ctypes.CDLL | None:
        if self._libc.probe_done:
            return self._libc.lib
        self._libc.probe_done = True
        try:
            name = ctypes.util.find_library("c") or "libc.so.6"
            lib = ctypes.CDLL(name, use_errno=True)
        except OSError as exc:  # pragma: no cover - no libc is not a Linux box
            self._libc.lib = None
            self._libc.supported = False
            self._libc.load_error = f"cannot load libc: {exc}"
            self._libc.unsupported_reason = self._libc.load_error
            return None
        if not hasattr(lib, "fallocate"):
            self._libc.supported = False
            self._libc.unsupported_reason = "libc has no fallocate()"
            self._libc.lib = None
            return None
        # off_t is a 64-bit signed integer on every Linux architecture we
        # support (ctypes does not expose a dedicated c_off_t alias).
        lib.fallocate.argtypes = [
            ctypes.c_int,
            ctypes.c_int,
            ctypes.c_int64,
            ctypes.c_int64,
        ]
        lib.fallocate.restype = ctypes.c_int
        self._libc.lib = lib
        return lib

    def _punch(self, fd: int, offset: int, length: int) -> None:
        """Call fallocate(PUNCH_HOLE|KEEP_SIZE); raise HolePunchUnsupported once."""
        lib = self._ensure_libc()
        if lib is None:
            raise HolePunchUnsupported(self._libc.unsupported_reason or "no libc")
        rc = lib.fallocate(
            fd,
            FALLOC_FL_PUNCH_HOLE | FALLOC_FL_KEEP_SIZE,
            ctypes.c_int64(offset),
            ctypes.c_int64(length),
        )
        if rc != 0:
            err = ctypes.get_errno()
            message = os.strerror(err)
            if err in _UNSUPPORTED_ERRNOS:
                raise HolePunchUnsupported(
                    f"FALLOC_FL_PUNCH_HOLE unsupported (errno {err}: {message})"
                )
            raise OSError(err, f"fallocate(FALLOC_FL_PUNCH_HOLE) failed: {message}")

    def _lock_for(self, file_key: str) -> threading.Lock:
        with self._locks_guard:
            lock = self._locks.get(file_key)
            if lock is None:
                lock = threading.Lock()
                self._locks[file_key] = lock
            return lock

    # ------------------------------------------------------------------ reclaim

    def reclaim_file(
        self,
        *,
        file_key: str,
        candidate_paths: list[Path],
        expected_identity: tuple[int, int] | None = None,
    ) -> ReclaimResult:
        """Release blocks fully before the committed cursor for ``file_key``.

        ``candidate_paths`` are tried in order until one exists and matches the
        cursor's recorded identity (or ``expected_identity`` when supplied).
        Never raises for reclaim problems: failures are returned in the result
        and, when a cursor row exists, persisted on that row.
        """
        result = ReclaimResult(file_key=file_key, path=None, requested=True)
        with self._lock_for(file_key):
            return self._reclaim_locked(
                file_key, list(candidate_paths), expected_identity, result
            )

    def _reclaim_locked(
        self,
        file_key: str,
        candidate_paths: list[Path],
        expected_identity: tuple[int, int] | None,
        result: ReclaimResult,
    ) -> ReclaimResult:
        # Re-read authoritative committed state inside a write transaction so
        # an interleaved collection's cursor advance is always visible and the
        # conditional update below can detect a concurrent reclaim.
        with self.db.transaction() as conn:
            row = conn.execute(
                """SELECT file_key, path, offset_bytes, punched_bytes,
                          punch_supported, punch_last_error
                     FROM cursors WHERE file_key = ?""",
                (file_key,),
            ).fetchone()
            if row is None:
                result.supported = False
                result.reason = "no cursor row yet; nothing collected for this file"
                return result
            committed_offset = int(row["offset_bytes"])
            punched_before = int(row["punched_bytes"])
            recorded_path = row["path"]
            result.punched_offset_before = punched_before

            if not int(row["punch_supported"]):
                result.supported = False
                result.reason = row["punch_last_error"] or "hole punching unsupported"
                result.punched_offset_after = punched_before
                return result

            path, identity = self._resolve_target(
                file_key, recorded_path, candidate_paths, expected_identity
            )
            if path is None:
                result.reason = (
                    "original file is not reachable under its recorded identity "
                    "(rotated away or replaced); nothing reclaimed"
                )
                result.punched_offset_after = punched_before
                return result
            device, inode = identity
            result.path = str(path)

            try:
                st = os.stat(path)
                block_size = os.statvfs(path).f_frsize or st.st_blksize
            except OSError as exc:
                return self._fail(
                    conn, row, result, punched_before,
                    f"cannot stat target file: {exc}",
                )
            if st.st_dev != device or st.st_ino != inode:
                result.reason = (
                    "path identity changed before reclaim; skipping this file"
                )
                result.punched_offset_after = punched_before
                return result

            result.block_size = block_size
            result.file_size = st.st_size
            result.disk_blocks_before = st.st_blocks
            result.disk_blocks_after = st.st_blocks

            # Exclusive end of the last full block that lies entirely before
            # the committed cursor.  Anything at/after this boundary, including
            # the block the cursor points into, stays allocated.
            safe_end = (committed_offset // block_size) * block_size
            if safe_end <= punched_before:
                result.reason = (
                    "no fully-collected block beyond the already reclaimed range"
                )
                result.punched_offset_after = punched_before
                result.disk_blocks_after = st.st_blocks
                # A previous failure may be stale: clear it once we catch up.
                if row["punch_last_error"] is not None:
                    conn.execute(
                        "UPDATE cursors SET punch_last_error = NULL WHERE file_key = ?",
                        (file_key,),
                    )
                return result

            # fallocate() requires a writable descriptor even though it never
            # changes the file's visible contents.  An open failure (read-only
            # file, permissions, ...) is a reclaim failure, not a request
            # failure: ingestion has already committed, so report and persist
            # it instead of letting it escape as a 500.
            try:
                fd = os.open(path, os.O_RDWR)
            except OSError as exc:
                return self._fail(
                    conn, row, result, punched_before,
                    f"cannot open file for hole punching (O_RDWR): {exc}",
                )
            try:
                fst = os.fstat(fd)
                if fst.st_dev != device or fst.st_ino != inode:
                    result.reason = (
                        "opened descriptor refers to a different file; skipping"
                    )
                    result.punched_offset_after = punched_before
                    return result
                try:
                    self._punch(fd, punched_before, safe_end - punched_before)
                except HolePunchUnsupported as exc:
                    result.supported = False
                    return self._fail(
                        conn, row, result, punched_before, str(exc),
                        unsupported=True,
                    )
            except OSError as exc:
                return self._fail(
                    conn, row, result, punched_before,
                    f"hole punch could not be performed: {exc}",
                )
            finally:
                os.close(fd)

            try:
                after = os.stat(path)
            except OSError:
                after = st
            result.disk_blocks_after = after.st_blocks
            result.file_size = after.st_size
            result.bytes_reclaimed = safe_end - punched_before
            result.performed = True
            result.punched_offset_after = safe_end

            # Conditional advance: another process may have reclaimed first.
            # Never move the boundary backwards or past the committed cursor.
            cur = conn.execute(
                """UPDATE cursors
                      SET punched_bytes = ?,
                          punch_supported = 1,
                          punch_last_error = NULL,
                          updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')
                    WHERE file_key = ?
                      AND punched_bytes < ?
                      AND ? <= offset_bytes""",
                (safe_end, file_key, safe_end, safe_end),
            )
            if cur.rowcount == 0:
                current = conn.execute(
                    "SELECT punched_bytes FROM cursors WHERE file_key = ?",
                    (file_key,),
                ).fetchone()
                result.punched_offset_after = int(current["punched_bytes"])
                result.bytes_reclaimed = max(
                    0, result.punched_offset_after - punched_before
                )
        return result

    # ------------------------------------------------------------- internals

    @staticmethod
    def _resolve_target(
        file_key: str,
        recorded_path: str | None,
        candidate_paths: list[Path],
        expected_identity: tuple[int, int] | None,
    ) -> tuple[Path | None, tuple[int, int] | None]:
        """Find an existing path that still names this file identity.

        The recorded path is the cursor's source of truth and is checked first
        (it is updated on every ingest, including rename adoption); explicit
        candidates cover the active path handed in by the current request.
        """
        ordered: list[Path] = []
        if recorded_path:
            ordered.append(Path(recorded_path))
        for candidate in candidate_paths:
            candidate = Path(candidate)
            if not ordered or str(candidate) != str(ordered[0]):
                ordered.append(candidate)

        for path in ordered:
            try:
                st = os.stat(path)
            except FileNotFoundError:
                continue
            except OSError:
                continue
            if expected_identity is not None:
                if (st.st_dev, st.st_ino) == expected_identity:
                    return path, expected_identity
                continue
            # Without an explicit pin, derive the identity from the file key
            # (``source:dev:inode:epoch``) so a reused path cannot fool us.
            identity = IdentityRegistry.make_key_parts(file_key)
            if identity is not None and (st.st_dev, st.st_ino) == identity:
                return path, identity
        return None, expected_identity

    def _fail(
        self,
        conn,
        row,
        result: ReclaimResult,
        punched_before: int,
        message: str,
        *,
        unsupported: bool = False,
    ) -> ReclaimResult:
        result.error = message
        result.reason = message
        result.supported = not unsupported
        result.punched_offset_after = punched_before
        conn.execute(
            """UPDATE cursors
                  SET punch_supported = ?,
                      punch_last_error = ?,
                      updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')
                WHERE file_key = ?""",
            (0 if unsupported else int(row["punch_supported"]), message, row["file_key"]),
        )
        return result
