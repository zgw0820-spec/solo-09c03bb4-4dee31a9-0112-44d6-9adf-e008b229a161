"""Retryable HTTP surface for the writer, exposed through the collector API.

Keeping these endpoints on the collector means the whole demo (append, rotate,
close, append-as-new) can be driven from one Swagger page.  They are thin
proxies: the writer still owns the descriptor.
"""

from __future__ import annotations

from collections.abc import Callable

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from .service import RotationService


class AppendRequest(BaseModel):
    message: str | None = Field(
        default=None,
        description="Line to append. Omit to generate a timestamped line.",
        max_length=4000,
    )


class AppendBatchRequest(BaseModel):
    count: int = Field(default=1, ge=1, le=1000)
    prefix: str = Field(default="request", max_length=200)


def build_router(service_resolver: Callable[[], RotationService]) -> APIRouter:
    router = APIRouter(prefix="/writer", tags=["writer-demo"])

    def service() -> RotationService:
        current = service_resolver()
        if current is None:  # pragma: no cover - defensive
            raise HTTPException(status_code=503, detail="writer proxy is not configured")
        return current

    @router.get("/status", summary="Writer state and open descriptors")
    def writer_status() -> dict:
        try:
            return service().writer.status()
        except HTTPException:
            raise
        except Exception as exc:  # noqa: BLE001
            raise HTTPException(status_code=503, detail=f"writer unavailable: {exc}") from exc

    @router.post("/write", summary="Append a line through the current handle")
    def write_one(req: AppendRequest) -> dict:
        """Append through whatever descriptor the writer currently owns.

        After ``/rotation/rotate`` that descriptor still points at the renamed
        archive, so this is the late-append demonstration.
        """
        try:
            return service().writer.write(req.message or "request")
        except HTTPException:
            raise
        except Exception as exc:  # noqa: BLE001
            raise HTTPException(status_code=503, detail=f"writer unavailable: {exc}") from exc

    @router.post("/write-batch", summary="Append several lines")
    def write_batch(req: AppendBatchRequest) -> dict:
        try:
            return service().writer.write_batch(req.count, req.prefix)
        except HTTPException:
            raise
        except Exception as exc:  # noqa: BLE001
            raise HTTPException(status_code=503, detail=f"writer unavailable: {exc}") from exc

    return router
