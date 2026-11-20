# Access Log Collector (FastAPI + SQLite)

A small collector/cleanup service for an append-only access log that is rotated
by **rename**. The hard part is not reading the file — it is the stale
descriptor: after `rename()`, the writer keeps appending through its old file
handle, so the "rotated" file keeps growing while the collector thinks it is
finished.

This project makes that scenario real, then proves the cleanup path is safe:

* the local writer really keeps the old `fd` open after rotation and can still
  append through it;
* the collector reads late appends into the same archive generation, exactly
  once per byte position;
* an archive may only be deleted after its handle is confirmed closed **and**
  every byte is in SQLite;
* a new rotation is refused until the previous archive is cleaned up;
* cleanup is a persisted, generation-bound operation that can be retried after
  a crash but can never be pointed at another file;
* `POST /collect?reclaim=true` additionally gives the log **volume** its disk
  back for bytes already committed to SQLite, by hole punching
  (`FALLOC_FL_PUNCH_HOLE`) — file length and byte offsets are unchanged and the
  writer keeps appending through its original handle.

## Layout

```
app/                 collector + cleanup API (FastAPI)
  collector.py       restart-safe incremental reader
  identity.py        file identity with inode-reuse protection
  reclaim.py         hole-punch space reclamation behind the committed cursor
  service.py         rotate / drain / delete / reclaim workflow
  main.py            HTTP API
  writer_api.py      thin writer proxy so everything is demoable in one Swagger
writer_service/      local writer that owns the real file descriptor
scripts/             demo_flow.py (happy path), reclaim_demo.py (disk reclaim),
                     restart_drill.sh (crash test)
tests/               pytest suite (file-level and HTTP-level)
```

## Run

```bash
docker compose up --build
```

* Collector + Swagger UI: <http://localhost:8000/docs>
* Writer + Swagger UI: <http://localhost:8001/docs>

The writer and collector share the `logs` volume; SQLite lives on the
`collector-data` volume, so `docker compose down` (without `-v`) preserves the
cursors and you can restart mid-flow.

## Demo the whole flow in Swagger (collector page, port 8000)

All writer calls are also proxied under `/writer/*`, so one page is enough.

1. `POST /writer/write-batch` `{"count": 3, "prefix": "before"}`
2. `POST /collect` → 3 lines inserted.
3. `POST /rotation/rotate` → returns `generation` N. The old handle is still open.
4. `POST /writer/write` `{"message": "late-request"}` → writes into the
   **archive**, not the new active file (the response `written_to` shows it).
5. `POST /rotation/rotate` again → `409`, because generation N is not cleaned up.
6. `POST /archives/N/drain` → closes the old handle as a separate step and
   ingests the late line. `complete: true`, `final_size` includes it.
7. `POST /cleanup/delete` `{"generation": N}` → deletes the archive file.
8. `GET /entries?generation=N` → every line, including the late append.

The same flow from the shell:

```bash
python -m scripts.demo_flow http://localhost:8000 http://localhost:8001
```

## Reclaiming disk after collection (hole punching)

Ingesting a line copies it into SQLite, but the original bytes still occupy
blocks on the log volume. `POST /collect` takes an optional `reclaim` switch:

```
POST /collect?reclaim=true
```

After the normal ingestion transaction commits, the collector releases, on the
**same original file**, the filesystem blocks that lie *completely before the
committed cursor* using `fallocate(FALLOC_FL_PUNCH_HOLE | FALLOC_FL_KEEP_SIZE)`:

* **Length and offsets are unchanged.** Only mapped blocks are returned to the
  volume; reads of a punched range return zeroes. The apparent file size stays
  identical, so every stored `byte_offset` keeps meaning the same byte and the
  writer's original `O_APPEND` handle keeps appending at the end.
* **Only safe blocks are touched.** The block the cursor points into (which may
  hold an un-ingested, half-written line) and everything after it stay
  allocated. The punch boundary is block-aligned to the filesystem's fragment
  size and can never pass the committed offset.
* **Aimed at one inode.** The reclaimer opens the file itself and pins it to
  the cursor's recorded `(device, inode)`. During a rotation the original inode
  is followed onto its archive path; a path reused for another file is never
  punched.
* **Interleaving and restarts are safe.** The released boundary
  (`punched_bytes`) is persisted in SQLite, only moves forward, and is updated
  conditionally. Concurrent collections ingest exactly once and never
  double-release; after a restart the boundary is simply resumed.
* **No new logs? Still useful.** Calling `POST /collect?reclaim=true` again on
  an active file reclaims blocks a previous attempt could not (for example once
  a partial tail becomes a complete line). This also works after the collector
  restarts.
* **Failures are reported, not hidden.** If punching fails (unsupported
  filesystem, read-only file, I/O error) the response stays `200`, ingestion is
  untouched and every record remains queryable; the error appears in the
  response's `reclaim` object and is persisted on the cursor (`GET /reclaim`),
  and the next call retries automatically. On a filesystem without hole-punch
  support the capability is remembered and the syscall is not retried.

Watch the real footprint in Swagger (or shell): note `st_blocks`/allocated
bytes before and after while the apparent size stays constant.

```bash
python -m scripts.reclaim_demo http://localhost:8000 http://localhost:8001
```

`GET /reclaim` reports the persisted boundary, last error, current size and
allocated blocks for every cursor.

## Crash / restart drill

```bash
bash scripts/restart_drill.sh
```

It writes lines, rotates, appends through the old handle, `SIGKILL`s the
collector, restarts it, appends once more, then drains and deletes. Expected
output ends with `DRILL OK` and exactly the five written lines — no loss, no
duplicates.

## Guarantees and how they are enforced

| Requirement | Mechanism |
|---|---|
| Renamed file keeps receiving writes | Writer keeps the descriptor and only switches on `POST /writer/close-old-handle`; this is a separate action from rotation. |
| Late appends land in the old generation | Archives are keyed by `(device, inode, epoch)`, which survives `rename()`. |
| Exactly-once ingestion | `UNIQUE(file_key, byte_offset)` + `INSERT OR IGNORE` in the same transaction that advances the cursor. |
| Restart resumes where it stopped | Cursors live in SQLite. On boot, archives left on disk are re-adopted and any `in_progress` delete is marked retryable. |
| Never split a partial line | Non-final reads stop before a newline-less tail; only a closed handle makes the tail final. |
| New round only after previous cleanup | `POST /rotation/rotate` refuses while any archive is not `deleted`. |
| Delete only after close + full ingest | `request_delete` requires `handle_closed = 1` and `status = ingested`; `drain` only sets those when the writer confirms no handle to that inode and the committed offset equals the file size. |
| Cleanup retry is safe | Delete is an `operations` row bound to one generation with pinned `path/device/inode/expected_final_size`; it is re-verified right before `unlink`, and retries are idempotent if the file is already gone. |
| Stale request cannot hit a later file | The request only references a generation; the inode/size check rejects a path that was replaced or grew; deleted generations short-circuit. |
| Inode reuse cannot corrupt cursors | Identity registry allocates a new epoch when a retired `(device, inode)` reappears. |
| Reclaim never changes visible data | `FALLOC_FL_KEEP_SIZE` keeps length/offsets; only blocks fully before the committed cursor are punched, so the cursor block and any partial tail stay allocated. |
| Reclaim cannot mis-aim after rotation | The punch is pinned to the cursor's `(device, inode)` and the file is re-opened/verified first; the archived original is reached by identity, never by guessing a path. |
| Reclaim is restart- and concurrency-safe | `punched_bytes` is persisted, advances only forward and is updated conditionally; concurrent collects and post-restart calls cannot double-release or go past the cursor. |
| Reclaim failure is survivable | The error is returned in the response and persisted on the cursor; ingestion commits independently, records stay queryable, and the next `reclaim=true` call retries. |

## API

| Method | Path | Purpose |
|---|---|---|
| POST | `/collect?reclaim=true` | Read appended lines; optionally hole-punch collected blocks |
| POST | `/rotation/rotate` | Rename active → archive (old handle stays open) |
| POST | `/archives/{generation}/drain?close_handle=true` | Close old handle + ingest all remaining bytes |
| POST | `/cleanup/delete` | Delete an archive generation (creates a retryable operation) |
| POST | `/cleanup/retry?generation=&operation_id=` | Resume an interrupted/failed deletion |
| GET | `/archives`, `/archives/{generation}` | Archive + operation status |
| GET | `/reclaim` | Persisted hole-punch boundary, error and disk usage per cursor |
| GET | `/entries?limit=&after_id=&generation=` | Ingested records |
| GET | `/health` | Paths, DB, recovered operations |
| GET/POST | `/writer/*` | Status, write, write-batch (proxy to the writer) |

## Tests

```bash
pip install -r requirements-dev.txt
pytest -q
```

40 tests cover incremental/partial-tail reads, late appends after rename,
restart adoption, inode reuse, delete preconditions, path retargeting, growth
after ingestion, interrupted-delete retry, the HTTP surface, and space
reclamation: real block release with unchanged length/offsets, partial-tail
protection, no-op re-collection, identity pinning across rotation and inode
replacement, parallel collections, read-only/unsupported/failure-and-retry
paths, collector restart and the old-database schema migration.

## Known limitations

* One source and one active file per deployment (`LOG_SOURCE_ID`); multiple
  sources would need per-source writer routing.
* `collect` is pull-based (no background tailer). Docker Compose does not
  schedule it; call `POST /collect` (with `reclaim=true` when wanted) or add a
  cron/worker.
* Hole punching requires a Linux filesystem supporting `FALLOC_FL_PUNCH_HOLE`
  (ext4, XFS, Btrfs, overlayfs backed by one of these, tmpfs, …). On an
  unsupported object the first attempt records the `EOPNOTSUPP`/`EINVAL` error
  and the capability is switched off for that cursor rather than retried.
  Reclaim needs a writable descriptor; a read-only file reports a reclaim
  error but never blocks ingestion.
* Reclaim is driven through `/collect`; an archive that is rotated away and
  never collected again is still removed wholesale by `POST /cleanup/delete`.
* The writer is an in-memory descriptor process: `docker compose restart writer`
  loses its handle state. The collector handles that (the archive is adopted and
  drained), but the writer will resume appending to its configured active path.
* Verification is by size + identity, not checksums; no fsync of the archive
  directory after `unlink` (irrelevant on a single node, relevant if durability
  across power loss of the unlink itself matters).
* No authentication, retention policy, or compression.
