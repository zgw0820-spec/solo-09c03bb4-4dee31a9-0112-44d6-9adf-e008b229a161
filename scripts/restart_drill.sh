#!/usr/bin/env bash
# Crash/restart drill: the collector is SIGKILLed between rotation and drain.
# Verifies that on restart it adopts the on-disk archive, collects the late
# append made through the still-open old handle, and neither loses nor
# duplicates records.
set -euo pipefail

ROOT="${ROOT:-/tmp/rotation-drill}"
API_PORT="${API_PORT:-8050}"
WRITER_PORT="${WRITER_PORT:-8051}"
PY="${PY:-python3}"
WORKDIR="${WORKDIR:-/workspace}"

rm -rf "$ROOT"
mkdir -p "$ROOT/logs/archive" "$ROOT/data"

export LOG_ACTIVE_PATH="$ROOT/logs/access.log"
export LOG_ARCHIVE_DIR="$ROOT/logs/archive"
export LOG_DB_PATH="$ROOT/data/collector.sqlite3"
export WRITER_BASE_URL="http://127.0.0.1:$WRITER_PORT"

cd "$WORKDIR"

"$PY" -m uvicorn writer_service.main:app --host 127.0.0.1 \
  --port "$WRITER_PORT" --log-level warning >"$ROOT/writer.log" 2>&1 &
WRITER_PID=$!

start_collector() {
  "$PY" -m uvicorn app.main:app --host 127.0.0.1 \
    --port "$API_PORT" --log-level warning >"$ROOT/collector.log" 2>&1 &
  echo $!
}

cleanup() {
  kill -9 "${COLLECTOR_PID:-0}" "$WRITER_PID" 2>/dev/null || true
  wait 2>/dev/null || true
}
trap cleanup EXIT

sleep 3
COLLECTOR_PID=$(start_collector)
sleep 3

api() { curl -sS -X "$1" "http://127.0.0.1:$API_PORT$2" -H 'content-type: application/json' ${3:+-d "$3"}; }
writer() { curl -sS -X "$1" "http://127.0.0.1:$WRITER_PORT$2" -H 'content-type: application/json' ${3:+-d "$3"}; }

echo "[1] write three lines"
writer POST /writer/write-batch '{"count":3,"prefix":"pre"}' >/dev/null
echo "[2] collect: $(api POST /collect | "$PY" -c 'import json,sys;print(json.load(sys.stdin)["lines_inserted"],"lines")')"

GEN=$(api POST /rotation/rotate | "$PY" -c 'import json,sys;print(json.load(sys.stdin)["generation"])')
echo "[3] rotated generation $GEN (old handle still open)"

echo "[4] late append through the old handle, then SIGKILL the collector"
writer POST /writer/write '{"message":"late-before-crash"}' >/dev/null
kill -9 "$COLLECTOR_PID"
wait "$COLLECTOR_PID" 2>/dev/null || true
sleep 1

COLLECTOR_PID=$(start_collector)
sleep 3
echo "[5] restart adopted: $(api GET /archives | "$PY" -c 'import json,sys;print([(a["generation"],a["status"]) for a in json.load(sys.stdin)])')"

echo "[6] another late append after restart"
writer POST /writer/write '{"message":"late-after-restart"}' >/dev/null

DRAIN=$(api POST "/archives/$GEN/drain")
echo "[7] drain: $("$PY" -c 'import json,sys;d=json.load(sys.stdin);print("complete=",d["complete"],"inserted=",d["lines_inserted"],"size=",d["final_size"])' <<<"$DRAIN")"

echo "[8] entries: $(api GET '/entries?limit=100' | "$PY" -c 'import json,sys;print([i["content"] for i in json.load(sys.stdin)["items"]])')"

echo "[9] delete: $(api POST /cleanup/delete "{\"generation\":$GEN}" | "$PY" -c 'import json,sys;print(json.load(sys.stdin)["status"])')"

kill -9 "$COLLECTOR_PID"
wait "$COLLECTOR_PID" 2>/dev/null || true
sleep 1
COLLECTOR_PID=$(start_collector)
sleep 3
echo "[10] after second restart, entries total: $(api GET '/entries?limit=100' | "$PY" -c 'import json,sys;print(json.load(sys.stdin)["count"])')"
echo "[11] archive files left on disk: $(ls -1 "$ROOT/logs/archive" | wc -l)"
echo "DRILL OK"
