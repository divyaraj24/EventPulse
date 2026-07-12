#!/bin/bash
# Runs one full experimental condition end to end: rebuild -> load
# generator (+ optional chaos) in parallel -> wait for the worker to drain
# its backlog -> extract the delivery log -> teardown -> chart. Output
# files land in scripts/results/, prefixed with the label; rerunning a
# label overwrites its files.
#
# Usage:
#   ./run_experiment.sh <label> [--rate N] [--duration N] [--no-chaos] [-- chaos.py args...]
#
# Examples:
#   ./run_experiment.sh none_maxconcurrency -- --max-concurrency 3
#   ./run_experiment.sh none_rejectrate -- --reject-rate 0.9
#   ./run_experiment.sh concurrent_volume --rate 150 --duration 60 --no-chaos

set -e

if docker compose version >/dev/null 2>&1; then
  DOCKER_COMPOSE=(docker compose)
elif command -v docker-compose >/dev/null 2>&1; then
  DOCKER_COMPOSE=(docker-compose)
else
  echo "ERROR: Docker Compose is not available in PATH as 'docker compose' or 'docker-compose'."
  exit 1
fi

if [ -z "$1" ] || [[ "$1" == --* ]]; then
  echo "Usage: ./run_experiment.sh <label> [--rate N] [--duration N] [--no-chaos] [-- chaos.py args...]"
  exit 1
fi

LABEL="$1"
shift

RATE=10
DURATION=50
NO_CHAOS=false
POLICY="none"
CHAOS_ARGS=()

while [[ $# -gt 0 ]]; do
  case "$1" in
    --rate) RATE="$2"; shift 2 ;;
    --duration) DURATION="$2"; shift 2 ;;
    --no-chaos) NO_CHAOS=true; shift ;;
    --policy) POLICY="$2"; shift 2 ;;
    --) shift; CHAOS_ARGS=("$@"); break ;;
    *) echo "Unknown argument: $1"; exit 1 ;;
  esac
done

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
RESULTS_DIR="$SCRIPT_DIR/results"
mkdir -p "$RESULTS_DIR"

# Safety net in case draining hangs for any reason not already covered by
# worker.py's own ack guarantee (process_message's try/except/finally).
DRAIN_TIMEOUT_SECONDS="${DRAIN_TIMEOUT_SECONDS:-180}"

# Same idea for the send phase -- duration + 2 minutes of slack before
# concluding load_generator.py/chaos.py is actually stuck.
SEND_TIMEOUT_SECONDS="${SEND_TIMEOUT_SECONDS:-$((DURATION + 120))}"

INGEST_CSV="$RESULTS_DIR/${LABEL}_ingest.csv"
DELIVERY_CSV="$RESULTS_DIR/${LABEL}_delivery.csv"
TIMELINE_JSON="$RESULTS_DIR/${LABEL}_timeline.json"
CHART_PNG="$RESULTS_DIR/${LABEL}_chart.png"
PG_CONNECTIONS_CSV="$RESULTS_DIR/${LABEL}_pg_connections.csv"

# Polls whether PID is still alive once a second; force-kills it past
# $2 seconds so a hung load_generator/chaos process can't stall the script.
wait_with_timeout() {
  local pid="$1"
  local timeout_seconds="$2"
  local name="$3"
  local elapsed=0
  while kill -0 "$pid" 2>/dev/null; do
    if [ "$elapsed" -ge "$timeout_seconds" ]; then
      echo "  WARNING: $name (pid $pid) still running after ${timeout_seconds}s -- force-killing"
      kill -9 "$pid" 2>/dev/null || true
      return 1
    fi
    sleep 1
    elapsed=$((elapsed + 1))
  done
  return 0
}

echo "=== [$LABEL] rebuilding containers (policy=$POLICY) ==="
cd "$PROJECT_ROOT"
"${DOCKER_COMPOSE[@]}" down -v
RETRY_POLICY="$POLICY" "${DOCKER_COMPOSE[@]}" up --build -d --wait
sleep 3

# Samples Postgres's active connection count once a second for the whole
# ingest+drain window; killed explicitly once drain-wait finishes.
echo "timestamp,active_connections" > "$PG_CONNECTIONS_CSV"
(
  while true; do
    COUNT=$(docker exec postgres-dev psql -U postgres -d eventpulse -t -c \
      "SELECT count(*) FROM pg_stat_activity;" 2>/dev/null | tr -d ' ')
    TS=$(date -u +"%Y-%m-%dT%H:%M:%S")
    echo "$TS,$COUNT" >> "$PG_CONNECTIONS_CSV"
    sleep 1
  done
) &
PG_MONITOR_PID=$!

cd "$SCRIPT_DIR"

if [ "$NO_CHAOS" = true ]; then
  echo "=== [$LABEL] pure volume test, no chaos -- rate=$RATE duration=${DURATION}s ==="
  python3 load_generator.py \
    --rate "$RATE" \
    --duration "$DURATION" \
    --api-url "http://localhost:8000" \
    --endpoint-url "http://receiver_mock:9000/webhook" \
    --output "$INGEST_CSV" &
  LOADGEN_PID=$!
  wait_with_timeout "$LOADGEN_PID" "$SEND_TIMEOUT_SECONDS" "load_generator.py" || \
    echo "  continuing with whatever data was captured before the kill"
else
  echo "=== [$LABEL] load generator + chaos -- rate=$RATE duration=${DURATION}s ==="
  python3 load_generator.py \
    --rate "$RATE" \
    --duration "$DURATION" \
    --api-url "http://localhost:8000" \
    --endpoint-url "http://receiver_mock:9000/webhook" \
    --output "$INGEST_CSV" &
  LOADGEN_PID=$!

  python3 chaos.py --steady 10 --fault 20 --recovery 20 \
    --timeline-output "$TIMELINE_JSON" \
    "${CHAOS_ARGS[@]}" &
  CHAOS_PID=$!

  wait_with_timeout "$LOADGEN_PID" "$SEND_TIMEOUT_SECONDS" "load_generator.py" || \
    echo "  continuing with whatever data was captured before the kill"
  wait_with_timeout "$CHAOS_PID" 90 "chaos.py" || \
    echo "  continuing -- chaos.py's own schedule is short, this shouldn't normally trigger"
fi

echo "=== [$LABEL] waiting for worker to drain remaining backlog (timeout ${DRAIN_TIMEOUT_SECONDS}s) ==="
cd "$PROJECT_ROOT"
START_TIME=$(date +%s)
while true; do
  REMAINING=$(python3 - <<'PY'
import redis
r = redis.Redis(host='localhost', port=6379, decode_responses=True)
groups = r.xinfo_groups('deliveries')
g = groups[0]
lag = g.get('lag') or 0
pending = g.get('pending') or 0
print(lag + pending)
PY
)
  echo "  remaining (undelivered + unacked): $REMAINING"
  if [ "$REMAINING" -le 0 ]; then
    break
  fi
  NOW=$(date +%s)
  ELAPSED=$((NOW - START_TIME))
  if [ "$ELAPSED" -ge "$DRAIN_TIMEOUT_SECONDS" ]; then
    echo "  WARNING: backlog did not drain within ${DRAIN_TIMEOUT_SECONDS}s; continuing with partial results"
    break
  fi
  sleep 1
done

echo "=== [$LABEL] extracting worker delivery log ==="
kill "$PG_MONITOR_PID" 2>/dev/null || true
wait "$PG_MONITOR_PID" 2>/dev/null || true
"${DOCKER_COMPOSE[@]}" stop worker
docker cp eventpulse-worker:/app/delivery_log.csv "$DELIVERY_CSV"

echo "=== [$LABEL] tearing down ==="
"${DOCKER_COMPOSE[@]}" down -v

echo "=== [$LABEL] generating chart ==="
cd "$SCRIPT_DIR"
ANALYZE_ARGS=(--file "$LABEL:$DELIVERY_CSV" --output "$CHART_PNG")
if [ -f "$TIMELINE_JSON" ]; then
  ANALYZE_ARGS+=(--timeline "$LABEL:$TIMELINE_JSON")
fi
python3 analyze.py "${ANALYZE_ARGS[@]}"

echo "=== [$LABEL] done -- results in scripts/results/${LABEL}_* ==="
PEAK_CONNECTIONS=$(tail -n +2 "$PG_CONNECTIONS_CSV" | awk -F, '{print $2}' | sort -n | tail -1)
echo "  peak Postgres active connections during this run: ${PEAK_CONNECTIONS:-unknown} (server max_connections is typically 100)"
ls -la "$RESULTS_DIR" | grep "$LABEL"