"""Shared fixtures: a real temp directory, real SQLite, real file handles."""

from __future__ import annotations

import os
from pathlib import Path

import pytest

from app.config import Settings
from app.db import Database
from app.service import RotationService


class FakeWriter:
    """In-process stand-in for the writer service that keeps a real descriptor.

    ``rotate`` renames the file but keeps the old fd open; ``close_handle``
    closes it and opens a new active file.  This mirrors the real writer
    exactly, without an HTTP hop, so tests can also simulate a crash by simply
    dropping the service object while keeping the file on disk.
    """

    def __init__(self, active_path: Path, archive_dir: Path) -> None:
        self.active_path = active_path
        self.archive_dir = archive_dir
        self.archive_dir.mkdir(parents=True, exist_ok=True)
        self.fd = os.open(
            active_path, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o644
        )
        self.handle_path = active_path
        self.state = "active"
        self.rotation_seq = 0
        self.last_archive: Path | None = None
        self.crash_before_unlink = False

    # -- writer API used by RotationService --

    def status(self) -> dict:
        handles = []
        if self.fd is not None:
            st = os.fstat(self.fd)
            handles.append(
                {
                    "path": str(self.handle_path),
                    "device": st.st_dev,
                    "inode": st.st_ino,
                    "size": st.st_size,
                }
            )
        return {
            "state": self.state,
            "active_path": str(self.active_path),
            "handles": handles,
        }

    def rotate(self) -> dict:
        self.rotation_seq += 1
        archived = self.archive_dir / f"access.{self.rotation_seq}.log"
        st = os.fstat(self.fd)
        os.rename(self.active_path, archived)
        self.handle_path = archived
        self.state = "rotated"
        self.last_archive = archived
        return {
            "previous_path": str(self.active_path),
            "archive_path": str(archived),
            "device": st.st_dev,
            "inode": st.st_ino,
            "size": st.st_size,
        }

    def close_handle(self) -> dict:
        if self.fd is None or self.state != "rotated":
            return {"closed": False, "reason": f"state={self.state}"}
        st = os.fstat(self.fd)
        os.close(self.fd)
        self.fd = os.open(
            self.active_path, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o644
        )
        self.handle_path = self.active_path
        self.state = "active"
        return {
            "closed": True,
            "closed_path": str(self.handle_path),
            "closed_device": st.st_dev,
            "closed_inode": st.st_ino,
            "closed_size": st.st_size,
        }

    # -- test helpers --

    def append(self, *lines: str) -> None:
        assert self.fd is not None
        for line in lines:
            os.write(self.fd, (line + "\n").encode())
        os.fsync(self.fd)

    def close(self) -> None:
        if self.fd is not None:
            os.close(self.fd)
            self.fd = None


@pytest.fixture()
def workspace(tmp_path: Path):
    active = tmp_path / "logs" / "access.log"
    archives = tmp_path / "logs" / "archive"
    active.parent.mkdir(parents=True)
    archives.mkdir(parents=True)
    settings = Settings(
        db_path=tmp_path / "data" / "collector.sqlite3",
        active_path=active,
        archive_dir=archives,
        writer_base_url="http://unused",
    )
    writer = FakeWriter(active, archives)
    yield settings, writer
    writer.close()


def build_service(settings, writer) -> RotationService:
    db = Database(settings.db_path)
    db.initialize()
    db.recover_interrupted_operations()
    service = RotationService(settings, db, writer)
    service.bootstrap()
    return service
