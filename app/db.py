"""SQLite schema and connection helpers.

All mutable workflow state (cursors, archive generations, cleanup operations)
lives here.  The database is the source of truth so that both the collector and
the cleanup API survive a process restart.
"""

from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator


SCHEMA = """
CREATE TABLE IF NOT EXISTS sources (
    source_id       TEXT PRIMARY KEY,
    active_path     TEXT NOT NULL,
    created_at      TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
);

CREATE TABLE IF NOT EXISTS archived_files (
    generation       INTEGER PRIMARY KEY AUTOINCREMENT,
    source_id        TEXT NOT NULL REFERENCES sources(source_id),
    path             TEXT NOT NULL UNIQUE,
    original_path    TEXT NOT NULL,
    device           INTEGER NOT NULL,
    inode            INTEGER NOT NULL,
    file_key         TEXT NOT NULL,
    status           TEXT NOT NULL DEFAULT 'rotating'
                     CHECK (status IN ('rotating', 'ingested', 'deleted')),
    size_at_rotate   INTEGER,
    final_size       INTEGER,
    handle_closed    INTEGER NOT NULL DEFAULT 0,
    handle_closed_at TEXT,
    ingested_at      TEXT,
    deleted_at       TEXT,
    created_at       TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
);

CREATE INDEX IF NOT EXISTS idx_archives_source
    ON archived_files(source_id, generation);

-- A log file's identity is (device, inode), but an inode can be reused after
-- the previous file is unlinked.  ``file_key`` therefore includes an epoch that
-- is bumped whenever a retired inode is seen again.  A cursor and ingested rows
-- belong to exactly one epoch.
CREATE TABLE IF NOT EXISTS file_epochs (
    file_key        TEXT PRIMARY KEY,
    source_id       TEXT NOT NULL,
    device          INTEGER NOT NULL,
    inode           INTEGER NOT NULL,
    epoch           INTEGER NOT NULL,
    status          TEXT NOT NULL DEFAULT 'live'
                    CHECK (status IN ('live', 'retired')),
    created_at      TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
    retired_at      TEXT,
    UNIQUE (source_id, device, inode, epoch)
);

CREATE INDEX IF NOT EXISTS idx_epochs_identity
    ON file_epochs(source_id, device, inode, epoch);

CREATE TABLE IF NOT EXISTS cursors (
    file_key        TEXT PRIMARY KEY,
    source_id       TEXT NOT NULL,
    path            TEXT,
    offset_bytes    INTEGER NOT NULL DEFAULT 0,
    line_number     INTEGER NOT NULL DEFAULT 0,
    -- Hole-punching bookkeeping (see app/reclaim.py).  ``punched_bytes`` is the
    -- exclusive end offset of the range already released back to the volume;
    -- it can only advance and can never overtake ``offset_bytes``.  A non-NULL
    -- ``punch_last_error`` records the most recent failed reclaim attempt so a
    -- failure is still visible after a process restart.
    punched_bytes   INTEGER NOT NULL DEFAULT 0,
    punch_supported INTEGER NOT NULL DEFAULT 1,
    punch_last_error TEXT,
    updated_at      TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
);

CREATE TABLE IF NOT EXISTS log_entries (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    file_key        TEXT NOT NULL,
    source_id       TEXT NOT NULL,
    byte_offset     INTEGER NOT NULL,
    line_number     INTEGER NOT NULL,
    content         TEXT NOT NULL,
    ingested_at     TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
    UNIQUE (file_key, byte_offset)
);

CREATE INDEX IF NOT EXISTS idx_entries_source
    ON log_entries(source_id, id);
CREATE INDEX IF NOT EXISTS idx_entries_file_offset
    ON log_entries(file_key, byte_offset);

CREATE TABLE IF NOT EXISTS operations (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    op_type             TEXT NOT NULL DEFAULT 'delete_archive',
    generation          INTEGER NOT NULL UNIQUE,
    source_id           TEXT NOT NULL,
    path                TEXT NOT NULL,
    device              INTEGER NOT NULL,
    inode               INTEGER NOT NULL,
    expected_final_size INTEGER NOT NULL,
    status              TEXT NOT NULL DEFAULT 'pending'
                        CHECK (status IN ('pending', 'in_progress', 'completed', 'failed')),
    attempts            INTEGER NOT NULL DEFAULT 0,
    last_error          TEXT,
    created_at          TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
    updated_at          TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
    completed_at        TEXT
);
"""


class Database:
    def __init__(self, path: Path) -> None:
        self.path = path

    @contextmanager
    def connect(self) -> Iterator[sqlite3.Connection]:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(self.path, timeout=30, isolation_level=None)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")
        conn.execute("PRAGMA journal_mode = WAL")
        conn.execute("PRAGMA synchronous = FULL")
        conn.execute("PRAGMA busy_timeout = 30000")
        try:
            yield conn
        finally:
            conn.close()

    def initialize(self) -> None:
        with self.connect() as conn:
            conn.executescript(SCHEMA)
            self._migrate_cursor_reclaim_columns(conn)

    @staticmethod
    def _migrate_cursor_reclaim_columns(conn: sqlite3.Connection) -> None:
        """Add hole-punch columns to databases created before this feature."""
        existing = {row["name"] for row in conn.execute("PRAGMA table_info(cursors)")}
        if "punched_bytes" not in existing:
            conn.execute(
                "ALTER TABLE cursors ADD COLUMN punched_bytes INTEGER NOT NULL DEFAULT 0"
            )
        if "punch_supported" not in existing:
            conn.execute(
                "ALTER TABLE cursors ADD COLUMN punch_supported INTEGER NOT NULL DEFAULT 1"
            )
        if "punch_last_error" not in existing:
            conn.execute("ALTER TABLE cursors ADD COLUMN punch_last_error TEXT")

    @contextmanager
    def transaction(self) -> Iterator[sqlite3.Connection]:
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            try:
                yield conn
            except BaseException:
                conn.execute("ROLLBACK")
                raise
            else:
                conn.execute("COMMIT")

    def recover_interrupted_operations(self) -> int:
        """Mark delete operations interrupted by a crash as retryable.

        The original archive row, identity (device/inode) and expected size are
        untouched, so a retry can only ever act on the same file.
        """
        with self.transaction() as conn:
            cur = conn.execute(
                """UPDATE operations
                      SET status = 'failed',
                          last_error = 'interrupted by process restart',
                          updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')
                    WHERE status = 'in_progress'"""
            )
            return cur.rowcount
