"""Demonstrate the full rotate -> late-append -> drain -> delete flow.

Usage (stack already running with docker compose):
    python -m scripts.demo_flow http://localhost:8000 http://localhost:8001
"""

from __future__ import annotations

import sys

import httpx


def show(title: str, payload) -> None:
    print(f"\n=== {title} ===")
    print(payload)


def main() -> int:
    collector_url = sys.argv[1] if len(sys.argv) > 1 else "http://localhost:8000"
    writer_url = sys.argv[2] if len(sys.argv) > 2 else "http://localhost:8001"
    with httpx.Client(base_url=collector_url, timeout=20) as api, httpx.Client(
        base_url=writer_url, timeout=20
    ) as writer:
        show("collector health", api.get("/health").json())
        show("writer health", writer.get("/health").json())

        show("1. append 3 lines through the active handle",
             writer.post("/writer/write-batch", json={"count": 3, "prefix": "before"}).json())
        show("2. collect them", api.post("/collect").json())

        rotation = api.post("/rotation/rotate").json()
        show("3. rotate (rename only; old handle stays open)", rotation)
        generation = rotation["generation"]

        show("4. append *after* rename through the old handle",
             writer.post("/writer/write", json={"message": "late-request-1"}).json())
        show("5. rotate again before cleanup is refused",
             api.post("/rotation/rotate").status_code)

        show("6. drain: close the old handle, ingest the late line",
             api.post(f"/archives/{generation}/drain").json())
        show("7. archive state", api.get(f"/archives/{generation}").json())
        show("8. ingested records for this generation",
             api.get("/entries", params={"generation": generation, "limit": 100}).json())

        show("9. delete the archive", api.post("/cleanup/delete", json={"generation": generation}).json())
        show("10. archive after deletion", api.get(f"/archives/{generation}").json())

        show("11. next round: append + collect + rotate",
             writer.post("/writer/write", json={"message": "round-2-1"}).json())
        show("    collect", api.post("/collect").json())
        rotation2 = api.post("/rotation/rotate").json()
        show("    rotate", rotation2)
        show("12. drain + delete round 2",
             api.post(f"/archives/{rotation2['generation']}/drain").json())
        show("    delete", api.post("/cleanup/delete", json={"generation": rotation2["generation"]}).json())
        show("13. all ingested records", api.get("/entries", params={"limit": 100}).json())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
