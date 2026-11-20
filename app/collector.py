"""Restart-safe incremental reader for append-only log files.

Identity
--------
A log file is identified by ``(source_id, st_dev, st_ino, epoch)`` where the
epoch is allocated by :class:`app.identity.IdentityRegistry`.  Renaming a file
does not change the identity, so bytes a writer appends through the old handle
after a rename are read at exactly the position reading stopped before the
rename.  A brand-new file gets a new inode, and a reused inode gets a new epoch,
so no byte position is ever attributed to the wrong file.

Exactly-once
------------
``log_entries`` has a unique ``(file_key, byte_offset)`` key.  Every batch of
lines is inserted with ``INSERT OR IGNORE`` in the *same* transaction that
advances the cursor, so a crash can only cause a re-read of a range that was
never committed.

Tail handling
-------------
Without ``final=True`` the reader stops before the last newline-less fragment,
because the writer may be halfway through appending it.  ``final=True`` is used
only after the writer reports that the old handle is closed.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from .db import Database
from .identity import IdentityRegistry

MAX_ATTEMPTS = 4
DRAIN_PASS_LIMIT = 4000
MAX_LINE_BYTES = 16 * 1_048_576


@dataclass
class IngestResult:
    file_key: str
    offset_before: int
    offset_after: int
    line_number: int
    lines_inserted: int
    duplicates_ignored: int
    size: int
    complete: bool
    changed: bool


class FileShrankError(RuntimeError):
    pass


class FileIdentityChanged(RuntimeError):
    """The path no longer points at the inode we were asked to read."""


class LineTooLongError(RuntimeError):
    """A single line exceeds the configured maximum and cannot be ingested."""


def _parse_lines(
    data: bytes, start_offset: int, final: bool
) -> tuple[list[tuple[int, str]], int]:
    """Split ``data`` into ``(absolute_offset, text)`` pairs.

    Returns the pairs and the number of bytes that form complete lines.
    """
    pairs: list[tuple[int, str]] = []
    pos = 0
    length = len(data)
    while True:
        nl = data.find(b"\n", pos)
        if nl == -1:
            break
        raw = data[pos:nl]
        pairs.append(
            (start_offset + pos, raw.decode("utf-8", errors="replace").rstrip("\r"))
        )
        pos = nl + 1
    if final and pos < length:
        raw = data[pos:]
        pairs.append(
            (start_offset + pos, raw.decode("utf-8", errors="replace").rstrip("\r"))
        )
        pos = length
    return pairs, pos


class Collector:
    def __init__(
        self,
        db: Database,
        identities: IdentityRegistry,
        max_batch_bytes: int = 1_048_576,
    ) -> None:
        self.db = db
        self.identities = identities
        self.max_batch_bytes = max(4096, int(max_batch_bytes))

    def ensure_source(self, source_id: str, active_path: Path) -> None:
        with self.db.transaction() as conn:
            conn.execute(
                """INSERT INTO sources(source_id, active_path) VALUES(?, ?)
                   ON CONFLICT(source_id) DO UPDATE SET
                       active_path = excluded.active_path""",
                (source_id, str(Path(active_path).resolve())),
            )

    def resolve_file_key(
        self, source_id: str, path: Path, device: int, inode: int
    ) -> str:
        with self.db.transaction() as conn:
            key = self.identities.resolve(conn, source_id, device, inode)
            conn.execute(
                """INSERT INTO cursors(file_key, source_id, path)
                   VALUES(?, ?, ?)
                   ON CONFLICT(file_key) DO UPDATE SET path = excluded.path""",
                (key, source_id, str(Path(path).resolve())),
            )
            return key

    def ingest_file(
        self,
        source_id: str,
        path: Path,
        *,
        final: bool = False,
        expected_identity: tuple[int, int] | None = None,
        file_key: str | None = None,
    ) -> IngestResult:
        """Read all complete lines appended since the committed cursor.

        ``expected_identity`` pins the read to a specific ``(device, inode)``
        (used when draining an archive) so a path that was reused for another
        file can never be mistaken for the archive.

        ``file_key`` pins the cursor to the archive's recorded identity.  Once
        an epoch has been retired, re-resolving ``(device, inode)`` would
        allocate a new epoch and re-read the file; passing the recorded key
        makes archive ingestion independent of that bookkeeping.
        """
        path = Path(path)
        if expected_identity is None:
            st = os.stat(path)
            device, inode = st.st_dev, st.st_ino
        else:
            device, inode = expected_identity
        if file_key is None:
            file_key = self.resolve_file_key(source_id, path, device, inode)

        for _attempt in range(MAX_ATTEMPTS):
            with self.db.connect() as conn:
                row = conn.execute(
                    "SELECT offset_bytes, line_number FROM cursors WHERE file_key = ?",
                    (file_key,),
                ).fetchone()
            offset_before = int(row["offset_bytes"]) if row else 0
            line_number = int(row["line_number"]) if row else 0

            fd = os.open(path, os.O_RDONLY)
            try:
                st = os.fstat(fd)
                if st.st_dev != device or st.st_ino != inode:
                    raise FileIdentityChanged(
                        f"{path} is no longer the expected file "
                        f"({device}:{inode} != {st.st_dev}:{st.st_ino})"
                    )
                size = st.st_size
                if size < offset_before:
                    raise FileShrankError(
                        f"{path} is {size} bytes but cursor is at {offset_before}; "
                        "refusing to guess a re-read position"
                    )
                read_size = min(size - offset_before, self.max_batch_bytes)
                if read_size <= 0:
                    pairs: list[tuple[int, str]] = []
                    consumed = 0
                else:
                    data = os.pread(fd, read_size, offset_before)
                    # A single line may be longer than one batch.  Grow the read
                    # up to MAX_LINE_BYTES so a long line cannot stall the
                    # cursor forever, but never split it.
                    while True:
                        has_newline = b"\n" in data
                        if has_newline or offset_before + len(data) >= size:
                            break
                        if len(data) >= MAX_LINE_BYTES:
                            raise LineTooLongError(
                                f"line at offset {offset_before} in {path} exceeds "
                                f"{MAX_LINE_BYTES} bytes"
                            )
                        grown = min(
                            size - offset_before, min(len(data) * 2, MAX_LINE_BYTES)
                        )
                        if grown <= len(data):
                            break
                        data = os.pread(fd, grown, offset_before)
                    at_eof = offset_before + len(data) >= size
                    pairs, consumed = _parse_lines(
                        data, offset_before, final and at_eof
                    )
            finally:
                os.close(fd)

            offset_after = offset_before + consumed
            complete = offset_after == size

            with self.db.transaction() as conn:
                row = conn.execute(
                    "SELECT offset_bytes, line_number FROM cursors WHERE file_key = ?",
                    (file_key,),
                ).fetchone()
                current = int(row["offset_bytes"]) if row else 0
                if current != offset_before:
                    continue  # another reader won; retry, never go backwards

                inserted = 0
                duplicates = 0
                next_line = line_number
                for absolute_offset, text in pairs:
                    cur = conn.execute(
                        """INSERT OR IGNORE INTO log_entries
                           (file_key, source_id, byte_offset, line_number, content)
                           VALUES (?, ?, ?, ?, ?)""",
                        (file_key, source_id, absolute_offset, next_line, text),
                    )
                    if cur.rowcount:
                        inserted += 1
                        next_line += 1
                    else:
                        duplicates += 1

                conn.execute(
                    """INSERT INTO cursors(file_key, source_id, path, offset_bytes,
                                           line_number, updated_at)
                       VALUES(?, ?, ?, ?, ?, strftime('%Y-%m-%dT%H:%M:%fZ','now'))
                       ON CONFLICT(file_key) DO UPDATE SET
                           path         = excluded.path,
                           offset_bytes = excluded.offset_bytes,
                           line_number  = excluded.line_number,
                           updated_at   = excluded.updated_at""",
                    (file_key, source_id, str(path.resolve()), offset_after, next_line),
                )

            return IngestResult(
                file_key=file_key,
                offset_before=offset_before,
                offset_after=offset_after,
                line_number=next_line,
                lines_inserted=inserted,
                duplicates_ignored=duplicates,
                size=size,
                complete=complete,
                changed=inserted > 0 or offset_after != offset_before,
            )

        raise RuntimeError(
            f"cursor for {file_key} kept changing; gave up after {MAX_ATTEMPTS} attempts"
        )

    def drain_to_end(
        self,
        source_id: str,
        path: Path,
        *,
        expected_identity: tuple[int, int],
        final: bool,
        file_key: str,
    ) -> IngestResult:
        """Read batches until no further progress is possible.

        ``final=True`` means the writer can no longer append.  In that case the
        loop runs until the committed offset reaches the file size, and a
        trailing fragment is accepted as the closing line.  With
        ``final=False`` the file may still be open, so the loop stops when a
        pass adds nothing.
        """
        last: IngestResult | None = None
        total_inserted = 0
        total_duplicates = 0
        for _ in range(DRAIN_PASS_LIMIT):
            result = self.ingest_file(
                source_id,
                path,
                final=final,
                expected_identity=expected_identity,
                file_key=file_key,
            )
            total_inserted += result.lines_inserted
            total_duplicates += result.duplicates_ignored
            last = result
            if result.complete:
                break
            if result.offset_after == result.offset_before:
                break
        if last is None:  # pragma: no cover - defensive
            raise RuntimeError("drain made no attempt")
        last.lines_inserted = total_inserted
        last.duplicates_ignored = total_duplicates
        return last

    def cursor_for(self, file_key: str) -> tuple[int, int]:
        with self.db.connect() as conn:
            row = conn.execute(
                "SELECT offset_bytes, line_number FROM cursors WHERE file_key = ?",
                (file_key,),
            ).fetchone()
            if row is None:
                return 0, 0
            return int(row["offset_bytes"]), int(row["line_number"])
