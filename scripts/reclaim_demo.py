"""End-to-end demonstration of post-collection space reclamation.

Drives the same HTTP calls as the Swagger UI against a running stack and shows
the *real* disk footprint (st_blocks via stat) of the active log before and
after ``POST /collect?reclaim=true``.

Usage:
    python -m scripts.reclaim_demo http://localhost:8000 http://localhost:8001
"""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path

import httpx


def show(title: str, payload) -> None:
    print(f"\n=== {title} ===")
    if isinstance(payload, (dict, list)):
        print(json.dumps(payload, indent=2, default=str))
    else:
        print(payload)


def footprint(path: Path) -> dict:
    st = os.stat(path)
    return {
        "path": str(path),
        "apparent_size_bytes": st.st_size,
        "allocated_blocks": st.st_blocks,
        "allocated_bytes": st.st_blocks * 512,
        "inode": st.st_ino,
        "device": st.st_dev,
    }


def main() -> int:
    collector_url = sys.argv[1] if len(sys.argv) > 1 else "http://localhost:8000"
    writer_url = sys.argv[2] if len(sys.argv) > 2 else "http://localhost:8001"

    with httpx.Client(base_url=collector_url, timeout=20) as api, httpx.Client(
        base_url=writer_url, timeout=20
    ) as writer:
        health = api.get("/health").json()
        show("collector health", health)
        active = Path(health["active_path"])

        # 1. Enough lines to span many filesystem blocks.
        show(
            "1. append 500 lines",
            writer.post(
                "/writer/write-batch", json={"count": 500, "prefix": "bulk"}
            ).json()["count"],
        )
        before = footprint(active)
        show("2. disk footprint BEFORE reclaim", before)

        # 2. Plain collection does not reclaim.
        plain = api.post("/collect").json()
        show("3. plain /collect (reclaim off)", {
            "lines_inserted": plain["lines_inserted"],
            "offset_after": plain["offset_after"],
        })
        show("4. footprint after plain collect", footprint(active))

        # 3. No new logs: reclaim=true still releases the collected blocks.
        reclaimed = api.post("/collect", params={"reclaim": "true"}).json()
        show("5. /collect?reclaim=true with NO new logs", reclaimed["reclaim"])
        after = footprint(active)
        show("6. disk footprint AFTER reclaim", after)

        saved = before["allocated_bytes"] - after["allocated_bytes"]
        show(
            "7. comparison",
            {
                "apparent_size_unchanged": after["apparent_size_bytes"]
                == before["apparent_size_bytes"],
                "allocated_bytes_before": before["allocated_bytes"],
                "allocated_bytes_after": after["allocated_bytes"],
                "bytes_released": saved,
                "percent_released": round(
                    100 * saved / before["allocated_bytes"], 1
                ),
                "inode_unchanged": after["inode"] == before["inode"],
            },
        )

        # 4. The original writer handle keeps appending; offsets stay intact.
        written = writer.post(
            "/writer/write", json={"message": "after-punch-line"}
        ).json()
        show("8. writer append through the same open handle", {
            "written_to": written["written_to"],
            "inode": written["inode"],
            "size": written["size"],
        })
        second = api.post("/collect", params={"reclaim": "true"}).json()
        show("9. second reclaiming collect", {
            "lines_inserted": second["lines_inserted"],
            "reclaim": second["reclaim"],
        })
        entries = api.get("/entries", params={"limit": 500}).json()
        show("10. ingested records", {
            "count": entries["count"],
            "first": entries["items"][0]["content"],
            "last": entries["items"][-1]["content"],
            "unique_offsets": len(
                {i["byte_offset"] for i in entries["items"]}
            )
            == len(entries["items"]),
        })
        show("11. persisted reclaim state (GET /reclaim)", api.get("/reclaim").json())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
