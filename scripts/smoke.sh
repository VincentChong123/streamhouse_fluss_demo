#!/usr/bin/env bash
# End-to-end acceptance test against the REAL services. The host needs only Docker + Compose v2 +
# bash; every check runs in the `e2e` container (same image as the app, `verify` command).
#
#   bash scripts/smoke.sh                     fresh run: deletes ONLY this demo's containers/volumes first
#   SMOKE_FRESH=false bash scripts/smoke.sh   reuse the running stack and its data
#
# Output: build/smoke/ (phase-*.txt, smoke.log, resources.txt, compose-ps.txt, logs-*.txt)
set -uo pipefail
cd "$(dirname "$0")/.."

OUT=build/smoke
mkdir -p "$OUT"
rm -f "$OUT"/phase-*.txt
: > "$OUT/smoke.log"
RC=0

log() { echo "[smoke $(date -u +%H:%M:%S)] $*" | tee -a "$OUT/smoke.log"; }

phase() {
  local name=$1; shift
  log "verify phase: $name"
  docker compose --profile test run --rm --no-deps e2e verify --phase "$name" "$@" 2>&1 \
    | grep -E '^(PASS|FAIL|==|VERIFIED_RUN_ID|PENDING_RUN_ID)' | tee "$OUT/phase-$name.txt"
  local code=${PIPESTATUS[0]}
  if [ "$code" -ne 0 ]; then RC=1; log "phase $name FAILED (exit $code)"; fi
}

require() { command -v "$1" >/dev/null 2>&1 || { echo "missing dependency: $1" >&2; exit 2; }; }
require docker
docker compose version >/dev/null 2>&1 || { echo "missing dependency: docker compose v2" >&2; exit 2; }

if [ "${SMOKE_FRESH:-true}" = "true" ]; then
  log "fresh start: docker compose down --volumes (this demo's resources only)"
  docker compose down --volumes --remove-orphans >>"$OUT/smoke.log" 2>&1
fi

START=$(date +%s)
log "docker compose up --build -d"
docker compose up --build -d >>"$OUT/smoke.log" 2>&1 || { log "compose up failed"; exit 1; }

# 1. The four stages, gating, refresh safety, run isolation.
phase stages
log "four stages verified $(( $(date +%s) - START )) s after compose up"

# 2. Manual events, SQL (two queries, INSERT -> ingestion, Fluss), Python, cancellation, offset ranges.
phase playground

docker stats --no-stream --format 'table {{.Name}}\t{{.CPUPerc}}\t{{.MemUsage}}' \
  $(docker compose ps -q) > "$OUT/resources.txt" 2>&1 || true

# 3. TaskManager failure and recovery.
log "docker compose stop flink-taskmanager"
docker compose stop flink-taskmanager >>"$OUT/smoke.log" 2>&1
phase tm-down
log "docker compose start flink-taskmanager"
docker compose start flink-taskmanager >>"$OUT/smoke.log" 2>&1
phase tm-recovered

# 4. Fluss TabletServer failure and recovery (the same run's stage 3 is retried).
log "docker compose stop fluss-tablet"
docker compose stop fluss-tablet >>"$OUT/smoke.log" 2>&1
phase fluss-down
log "docker compose start fluss-tablet"
docker compose start fluss-tablet >>"$OUT/smoke.log" 2>&1
phase fluss-recovered

# 5. Kafka failure and recovery (the same run's stage 1 is retried).
log "docker compose stop kafka"
docker compose stop kafka >>"$OUT/smoke.log" 2>&1
phase kafka-down
log "docker compose start kafka"
docker compose start kafka >>"$OUT/smoke.log" 2>&1
phase kafka-recovered

docker compose ps > "$OUT/compose-ps.txt" 2>&1 || true
docker compose logs --no-color > "$OUT/logs-final.txt" 2>&1 || true

PASSES=$(cat "$OUT"/phase-*.txt | grep -c '^PASS' || true)
FAILS=$(cat "$OUT"/phase-*.txt | grep -c '^FAIL' || true)
log "TOTAL: $PASSES passed, $FAILS failed in $(( $(date +%s) - START )) s"
[ "$FAILS" -eq 0 ] || RC=1
exit $RC
