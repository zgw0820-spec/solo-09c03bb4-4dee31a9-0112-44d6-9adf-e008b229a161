#!/usr/bin/env bash
# Reclaim drill: drives POST /collect?reclaim=true through the three scenarios
# the feature must survive and prints the real disk footprint (st_blocks) at
# each step. Works against the docker compose stack or the local two-process
# layout used by restart_drill.sh.
#
#   API_PORT=8000 WRITER_PORT=8001 bash scripts/reclaim_drill.sh
set -euo pipefail

API_BASE="${API_BASE:-http://127.0.0.1:${API_PORT:-8000}}"
WRITER_BASE="${WRITER_BASE:-http://127.0.0.1:${WRITER_PORT:-8001}}"
PY="${PY:-python3}"

api() { curl -fsS -X "$1" "${API_BASE}$2" ${3:+-H 'content-type: application/json' -d "$3"}; }
writer() { curl -fsS -X "$1" "${WRITER_BASE}$2" -H 'content-type: application/json' ${3:+-d "$3"}; }

ACTIVE="$(api GET /health | "$PY" -c 'import json,sys;print(json.load(sys.stdin)["active_path"])')"
footprint() { "$PY" -c "import os;s=os.stat('$ACTIVE');print(f'size={s.st_size} allocated={s.st_blocks*512} inode={s.st_ino}')"; }

echo "[1] parallel reclaiming collects"
for _ in $(seq 1 6); do
  writer POST /writer/write-batch '{"count":500,"prefix":"parallel-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}' >/dev/null
done
echo "    before: $(footprint)"
for _ in $(seq 1 8); do
  api POST "/collect?reclaim=true" >/dev/null &
done
wait
echo "    after:  $(footprint)"

echo "[2] rotation: reclaim follows the original inode onto the archive path"
GEN=$(api POST /rotation/rotate | "$PY" -c 'import json,sys;print(json.load(sys.stdin)["generation"])')
ARCH=$(ls -t "$(dirname "$ACTIVE")/archive"/*.log | head -1)
arch_print() { "$PY" -c "import os;s=os.stat('$ARCH');print(f'archive size={s.st_size} allocated={s.st_blocks*512}')"; }
echo "    $(arch_print)"
api POST "/collect?reclaim=true" | "$PY" -c 'import json,sys;r=json.load(sys.stdin)["reclaim"];print("    reclaim path:",r["path"],"performed:",r["performed"])'
api POST "/archives/$GEN/drain" >/dev/null
api POST /cleanup/delete "{\"generation\":$GEN}" >/dev/null

echo "[3] failure is reported and a retry succeeds"
for _ in $(seq 1 4); do
  writer POST /writer/write-batch '{"count":500,"prefix":"retry-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}' >/dev/null
done
chmod 444 "$ACTIVE"
CODE=$(curl -sS -o /tmp/reclaim-drill-fail.json -w '%{http_code}' -X POST "${API_BASE}/collect?reclaim=true")
chmod 644 "$ACTIVE"
"$PY" -c "import json;d=json.load(open('/tmp/reclaim-drill-fail.json'));print('    failing call HTTP $CODE; ingested:',d['lines_inserted'],'reclaim error:',d['reclaim']['error'][:60])"
echo "    retry:  $(api POST '/collect?reclaim=true' | "$PY" -c 'import json,sys;r=json.load(sys.stdin)["reclaim"];print("performed:",r["performed"],"released:",r["bytes_reclaimed"],"error:",r["error"])')"
echo "    final:  $(footprint)"
echo "RECLAIM DRILL OK"
