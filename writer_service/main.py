"""Local access-log writer.

It owns the file descriptor, which is what makes this demo faithful to the real
problem:

* ``POST /writer/rotate`` renames the active file into the archive directory but
  deliberately keeps the descriptor open and still pointed at the renamed file.
  Any write that follows therefore lands in the *archive*.
* ``POST /writer/close-old-handle`` is a separate step: it fsyncs and closes the
  old descriptor and opens a fresh one for a brand-new active file.  Only after
  this call can the collector treat the archive as final.
"""

from __future__ import annotations

import os
import threading
import time
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path

from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

ACTIVE_PATH = Path(os.environ.get("LOG_ACTIVE_PATH", "/logs/access.log"))
ARCHIVE_DIR = Path(os.environ.get("LOG_ARCHIVE_DIR", "/logs/archive"))


class WriterState:
    def __init__(self, active_path: Path, archive_dir: Path) -> None:
        self.lock = threading.RLock()
        self.active_path = active_path
        self.archive_dir = archive_dir
        self.fd: int | None = None
        self.handle_path: Path | None = None
        self.state = "unopened"  # unopened | active | rotated
        self.seq = 0
        self.lines_written = 0
        self.rotation_seq = 0
        self.last_rotation: dict | None = None

    # -- primitives ----------------------------------------------------

    def _open_active(self) -> os.stat_result:
        self.active_path.parent.mkdir(parents=True, exist_ok=True)
        self.fd = os.open(
            self.active_path, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o644
        )
        self.handle_path = self.active_path
        st = os.fstat(self.fd)
        self.state = "active"
        return st

    def open(self) -> None:
        if self.fd is not None:
            os.close(self.fd)
            self.fd = None
        self._open_active()

    def _format(self, message: str | None) -> str:
        self.seq += 1
        now = datetime.now(timezone.utc).isoformat(timespec="milliseconds")
        if message is None:
            return f"{now} seq={self.seq} msg=request-{self.seq}"
        return message.rstrip("\n")

    def write(self, message: str | None = None) -> dict:
        if self.fd is None:
            raise RuntimeError("writer has no open handle")
        line = self._format(message)
        data = (line + "\n").encode("utf-8")
        written = os.write(self.fd, data)
        os.fsync(self.fd)
        self.lines_written += 1
        st = os.fstat(self.fd)
        return {
            "seq": self.seq,
            "line": line,
            "bytes": written,
            "written_to": str(self.handle_path),
            "device": st.st_dev,
            "inode": st.st_ino,
            "size": st.st_size,
            "writer_state": self.state,
        }

    def rotate(self) -> dict:
        if self.fd is None or self.state != "active":
            raise RuntimeError(
                f"rotation requires an active writer handle (state={self.state})"
            )
        self.rotation_seq += 1
        self.archive_dir.mkdir(parents=True, exist_ok=True)
        archived = (
            self.archive_dir
            / f"access.{int(time.time())}.{os.getpid()}.{self.rotation_seq}.log"
        )
        previous = self.active_path
        old_st = os.fstat(self.fd)
        os.rename(previous, archived)
        # The descriptor stays open and now refers to the archive.  This is the
        # stale-handle hazard the collector must handle.
        self.handle_path = archived
        self.state = "rotated"
        result = {
            "previous_path": str(previous),
            "archive_path": str(archived),
            "device": old_st.st_dev,
            "inode": old_st.st_ino,
            "size": old_st.st_size,
            "old_handle_still_open": True,
            "old_handle_writes_to": str(archived),
            "new_active_path": str(previous),
            "new_active_exists": previous.exists(),
            "next_step": "POST /writer/close-old-handle",
        }
        self.last_rotation = result
        return result

    def close_old_handle(self) -> dict:
        if self.fd is None or self.state != "rotated":
            return {
                "closed": False,
                "reason": f"no rotated handle to close (state={self.state})",
                "new_active_created": False,
            }
        old_path = self.handle_path
        old_st = os.fstat(self.fd)
        archive_size = old_st.st_size
        os.fsync(self.fd)
        os.close(self.fd)
        self.fd = None
        new_st = self._open_active()
        result = {
            "closed": True,
            "closed_path": str(old_path),
            "closed_device": old_st.st_dev,
            "closed_inode": old_st.st_ino,
            "closed_size": archive_size,
            "new_active_path": str(self.active_path),
            "new_active_device": new_st.st_dev,
            "new_active_inode": new_st.st_ino,
            "new_active_created": True,
        }
        self.last_rotation = None
        return result

    def status(self) -> dict:
        current = None
        if self.fd is not None:
            st = os.fstat(self.fd)
            current = {
                "path": str(self.handle_path),
                "device": st.st_dev,
                "inode": st.st_ino,
                "size": st.st_size,
            }
        # Older descriptors are never kept around: rotation keeps exactly one
        # descriptor, and close-old-handle replaces it.
        return {
            "state": self.state,
            "active_path": str(self.active_path),
            "current_handle": current,
            "handles": [current] if current else [],
            "lines_written": self.lines_written,
            "rotation_seq": self.rotation_seq,
            "active_path_exists": self.active_path.exists(),
        }


state = WriterState(ACTIVE_PATH, ARCHIVE_DIR)


@asynccontextmanager
async def lifespan(_app: FastAPI):
    state.archive_dir.mkdir(parents=True, exist_ok=True)
    with state.lock:
        if state.fd is None:
            state.open()
    yield
    with state.lock:
        if state.fd is not None:
            os.fsync(state.fd)
            os.close(state.fd)
            state.fd = None


app = FastAPI(
    title="Access Log Writer",
    description=(
        "Writer with a real file descriptor. Rotation renames the file and keeps "
        "writing through the old descriptor; closing the old handle and opening "
        "the new active file is a separate explicit call."
    ),
    version="1.0.0",
    lifespan=lifespan,
)


class WriteRequest(BaseModel):
    message: str | None = Field(default=None, max_length=4000)


class WriteBatchRequest(BaseModel):
    count: int = Field(default=1, ge=1, le=1000)
    prefix: str = Field(default="request", max_length=200)


@app.get("/health", tags=["writer"])
def health() -> dict:
    with state.lock:
        return {"status": "ok", "handle_open": state.fd is not None}


@app.get("/writer/status", tags=["writer"])
def status() -> dict:
    with state.lock:
        return state.status()


@app.post("/writer/write", tags=["writer"], summary="Append one line")
def write_one(req: WriteRequest) -> dict:
    """Append through the *current* descriptor.

    If the file was already renamed, the line goes to the archive — this is the
    late-append scenario, not a special test-only path.
    """
    with state.lock:
        try:
            return state.write(req.message)
        except RuntimeError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc


@app.post("/writer/write-batch", tags=["writer"], summary="Append several lines")
def write_batch(req: WriteBatchRequest) -> dict:
    with state.lock:
        try:
            lines = [state.write(f"{req.prefix}-{i + 1}") for i in range(req.count)]
        except RuntimeError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
    return {"count": len(lines), "last": lines[-1], "lines": lines}


@app.post("/writer/rotate", tags=["writer"], summary="Rename log, keep old handle")
def rotate() -> dict:
    with state.lock:
        try:
            return state.rotate()
        except RuntimeError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc


@app.post(
    "/writer/close-old-handle",
    tags=["writer"],
    summary="Close old handle and switch to a new file",
)
def close_old_handle() -> dict:
    with state.lock:
        return state.close_old_handle()


@app.exception_handler(RuntimeError)
def _runtime_error(_request, exc: RuntimeError) -> JSONResponse:
    return JSONResponse(status_code=409, content={"detail": str(exc)})
