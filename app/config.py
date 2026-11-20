"""Runtime configuration for the collector/cleanup service."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class Settings:
    db_path: Path
    active_path: Path
    archive_dir: Path
    source_id: str = "access"
    writer_base_url: str = "http://127.0.0.1:8001"
    writer_timeout_seconds: float = 5.0
    max_batch_bytes: int = 1_048_576
    archive_glob: str = "access.*.log"


def _path_from_env(name: str, default: str) -> Path:
    return Path(os.environ.get(name, default)).expanduser()


def load_settings() -> Settings:
    active_path = _path_from_env("LOG_ACTIVE_PATH", "/logs/access.log")
    archive_dir = _path_from_env("LOG_ARCHIVE_DIR", "/logs/archive")
    db_path = _path_from_env("LOG_DB_PATH", "/data/collector.sqlite3")
    return Settings(
        db_path=db_path,
        active_path=active_path,
        archive_dir=archive_dir,
        source_id=os.environ.get("LOG_SOURCE_ID", "access"),
        writer_base_url=os.environ.get("WRITER_BASE_URL", "http://127.0.0.1:8001").rstrip("/"),
        writer_timeout_seconds=float(os.environ.get("WRITER_TIMEOUT_SECONDS", "5")),
        max_batch_bytes=int(os.environ.get("LOG_MAX_BATCH_BYTES", "1048576")),
        archive_glob=os.environ.get("LOG_ARCHIVE_GLOB", "access.*.log"),
    )
