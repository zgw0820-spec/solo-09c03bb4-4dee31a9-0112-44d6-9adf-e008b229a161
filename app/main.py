"""FastAPI application: rotation, ingestion, cleanup and inspection API."""

from __future__ import annotations

from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel, Field

from .config import load_settings
from .db import Database
from .service import RotationService, WorkflowError
from .writer_api import build_router
from .writer_client import WriterClient


settings = load_settings()
db = Database(settings.db_path)
writer = WriterClient(settings.writer_base_url, settings.writer_timeout_seconds)
service = RotationService(settings, db, writer)


@asynccontextmanager
async def lifespan(_app: FastAPI):
    db.initialize()
    interrupted = db.recover_interrupted_operations()
    service.bootstrap()
    app.state.interrupted_operations = interrupted
    yield


app = FastAPI(
    title="Log Rotation Collector",
    description=(
        "Collects an append-only access log, survives rename-based rotation "
        "with a stale writer handle, ingests late appends exactly once, and "
        "deletes an archive only after its handle is closed and all bytes are "
        "in SQLite. Cleanup operations are bound to a specific generation and "
        "are safely retryable.\n\n"
        "Demo order: write lines -> POST /collect -> POST /rotation/rotate -> "
        "write a late line -> POST /archives/{generation}/drain -> "
        "POST /cleanup/delete -> GET /entries."
    ),
    version="1.0.0",
    lifespan=lifespan,
)


def _handle_error(exc: WorkflowError) -> HTTPException:
    return HTTPException(status_code=exc.status_code, detail=str(exc))


app.include_router(build_router(lambda: service))


# ------------------------------------------------------------------ collect


@app.post("/collect", tags=["collection"], summary="Collect newly appended lines")
def collect() -> dict:
    """Read lines appended to the active log since the last collected offset.

    Safe to call repeatedly and safe after a restart: the committed cursor and
    the unique ``(file_key, byte_offset)`` key make re-reads idempotent.
    """
    try:
        return service.collect_active()
    except WorkflowError as exc:
        raise _handle_error(exc) from exc


# ----------------------------------------------------------------- rotation


@app.post("/rotation/rotate", tags=["rotation"], summary="Rename the active log")
def rotate() -> dict:
    """Ask the writer to rename the active log and register it as an archive.

    The writer keeps its old descriptor open, so late appends are still
    possible.  A second rotation is refused until the previous generation is
    drained, closed and deleted.
    """
    try:
        return service.rotate()
    except WorkflowError as exc:
        raise _handle_error(exc) from exc


@app.post(
    "/archives/{generation}/drain",
    tags=["rotation"],
    summary="Ingest late appends and close the old handle",
)
def drain(generation: int, close_handle: bool = Query(default=True)) -> dict:
    """Ingest all remaining bytes for one archive generation.

    With ``close_handle=true`` (default) the writer's stale descriptor is
    closed as a separate step before the final read.  The generation is only
    marked *ingested* when the handle is confirmed closed and the collected
    offset has reached the final file size.
    """
    try:
        return service.drain(generation, close_handle=close_handle)
    except WorkflowError as exc:
        raise _handle_error(exc) from exc


# ----------------------------------------------------------------- cleanup


class DeleteRequest(BaseModel):
    generation: int = Field(..., ge=1, description="Archive generation to delete")
    operation_id: int | None = Field(
        default=None,
        description="Retry a previously created operation; must belong to the generation",
    )


@app.post(
    "/cleanup/delete",
    tags=["cleanup"],
    summary="Delete one archive generation (retryable)",
)
def delete_archive(req: DeleteRequest) -> dict:
    """Delete an archive only if it is fully ingested and its handle is closed.

    A request is bound to the generation and its recorded inode/size.  Retrying
    a failed or interrupted deletion with the same generation reuses the same
    operation and can only act on that original file.
    """
    if req.operation_id is not None:
        op = service._operation_row(req.generation)  # noqa: SLF001
        if op is None or int(op["id"]) != int(req.operation_id):
            raise HTTPException(
                status_code=409,
                detail=(
                    f"operation {req.operation_id} does not belong to "
                    f"generation {req.generation}"
                ),
            )
    try:
        return service.request_delete(req.generation)
    except WorkflowError as exc:
        raise _handle_error(exc) from exc


@app.post(
    "/cleanup/retry",
    tags=["cleanup"],
    summary="Retry an interrupted/failed deletion",
)
def retry_delete(generation: int, operation_id: int | None = None) -> dict:
    """Resume the delete operation for a generation.

    The operation is bound to ``(generation, path, device, inode)``; retrying
    can never retarget it at another file.  If a previous attempt was
    interrupted after the unlink, the retry succeeds idempotently.  If the
    archive was drained again in the meantime, the expected final size is
    refreshed from the newly committed ingestion state.
    """
    op = service._operation_row(generation)  # noqa: SLF001
    if op is None:
        raise HTTPException(
            status_code=404,
            detail=f"no delete operation exists for generation {generation}",
        )
    if operation_id is not None and int(op["id"]) != int(operation_id):
        raise HTTPException(
            status_code=409,
            detail=f"operation {operation_id} does not belong to generation {generation}",
        )
    try:
        result = service.request_delete(generation)
    except WorkflowError as exc:
        raise _handle_error(exc) from exc
    result["operation"] = service._operation_row(generation)  # noqa: SLF001
    return result


# -------------------------------------------------------------- inspection


@app.get("/archives", tags=["inspection"], summary="List archive generations")
def archives(limit: int = Query(default=50, ge=1, le=500)) -> list[dict]:
    return service.archives(limit=limit)


@app.get("/archives/{generation}", tags=["inspection"], summary="Show one archive")
def archive(generation: int) -> dict:
    try:
        return service.archive(generation)
    except WorkflowError as exc:
        raise _handle_error(exc) from exc


@app.get("/entries", tags=["inspection"], summary="Read ingested records")
def entries(
    limit: int = Query(default=50, ge=1, le=500),
    after_id: int = Query(default=0, ge=0),
    generation: int | None = Query(default=None),
    source_id: str | None = Query(default=None),
) -> dict:
    rows = service.entries(
        limit=limit, after_id=after_id, generation=generation, source_id=source_id
    )
    return {
        "count": len(rows),
        "next_after_id": rows[-1]["id"] if rows else after_id,
        "items": rows,
    }


@app.get("/health", tags=["inspection"])
def health() -> dict:
    return {
        "status": "ok",
        "source_id": settings.source_id,
        "active_path": str(settings.active_path),
        "archive_dir": str(settings.archive_dir),
        "db_path": str(settings.db_path),
        "interrupted_operations_recovered": getattr(
            app.state, "interrupted_operations", 0
        ),
    }
