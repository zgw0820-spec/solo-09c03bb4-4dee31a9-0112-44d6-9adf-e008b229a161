"""File identity with inode-reuse protection.

A path is not an identity (renames happen), and ``(device, inode)`` alone is not
stable forever (inodes are reused after unlink).  An *epoch* is allocated per
``(source, device, inode)`` observation and retired when the corresponding
archive is deleted.  Cursors and ingested rows are keyed by that epoch, so a
later file that happens to reuse the inode can never inherit an old offset.
"""

from __future__ import annotations

import sqlite3

from .db import Database


class IdentityRegistry:
    def __init__(self, db: Database) -> None:
        self.db = db

    @staticmethod
    def make_key(source_id: str, device: int, inode: int, epoch: int) -> str:
        return f"{source_id}:{device}:{inode}:{epoch}"

    @staticmethod
    def make_key_parts(file_key: str) -> tuple[int, int] | None:
        """Parse ``(device, inode)`` back out of a file key.

        The source id may itself contain ``:``, so the identity is taken from
        the last three ``:``-separated fields.  Returns ``None`` for a key that
        was not produced by :meth:`make_key`.
        """
        parts = str(file_key).rsplit(":", 3)
        if len(parts) != 4:
            return None
        try:
            return int(parts[1]), int(parts[2])
        except ValueError:
            return None

    def resolve(
        self, conn: sqlite3.Connection, source_id: str, device: int, inode: int
    ) -> str:
        """Return the live file key for an identity, allocating an epoch if needed.

        Must be called inside a transaction so concurrent allocations of the
        same identity cannot produce two live epochs.
        """
        row = conn.execute(
            """SELECT file_key FROM file_epochs
                WHERE source_id = ? AND device = ? AND inode = ? AND status = 'live'
                ORDER BY epoch DESC LIMIT 1""",
            (source_id, device, inode),
        ).fetchone()
        if row is not None:
            return str(row["file_key"])
        next_epoch = int(
            conn.execute(
                """SELECT coalesce(max(epoch), 0) + 1 FROM file_epochs
                    WHERE source_id = ? AND device = ? AND inode = ?""",
                (source_id, device, inode),
            ).fetchone()[0]
        )
        file_key = self.make_key(source_id, device, inode, next_epoch)
        conn.execute(
            """INSERT INTO file_epochs(file_key, source_id, device, inode, epoch, status)
               VALUES(?, ?, ?, ?, ?, 'live')""",
            (file_key, source_id, device, inode, next_epoch),
        )
        return file_key

    def retire(self, conn: sqlite3.Connection, file_key: str) -> None:
        conn.execute(
            """UPDATE file_epochs SET status = 'retired',
                      retired_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')
                WHERE file_key = ? AND status = 'live'""",
            (file_key,),
        )
