"""HTTP client for the local log writer process.

The collector never guesses whether a rotated file can still grow.  It asks the
writer, which is the only component that owns file handles.  This is what makes
"all bytes from the old handle have been ingested, and only then delete" a real
decision instead of a timeout heuristic.
"""

from __future__ import annotations

import httpx


class WriterClient:
    def __init__(self, base_url: str, timeout: float = 5.0) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout

    def _client(self) -> httpx.Client:
        return httpx.Client(base_url=self.base_url, timeout=self.timeout)

    def status(self) -> dict:
        with self._client() as client:
            resp = client.get("/writer/status")
            resp.raise_for_status()
            return resp.json()

    def write(self, message: str) -> dict:
        with self._client() as client:
            resp = client.post("/writer/write", json={"message": message})
            resp.raise_for_status()
            return resp.json()

    def write_batch(self, count: int, prefix: str = "request") -> dict:
        with self._client() as client:
            resp = client.post(
                "/writer/write-batch", json={"count": count, "prefix": prefix}
            )
            resp.raise_for_status()
            return resp.json()

    def rotate(self) -> dict:
        with self._client() as client:
            resp = client.post("/writer/rotate")
            resp.raise_for_status()
            return resp.json()

    def close_handle(self) -> dict:
        with self._client() as client:
            resp = client.post("/writer/close-old-handle", json={})
            resp.raise_for_status()
            return resp.json()
