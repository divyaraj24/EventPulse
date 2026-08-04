#!/bin/bash
# Runs one full experimental condition end to end: rebuild -> load
# generator (+ optional chaos) in parallel -> wait for the worker to drain
# its backlog -> extract the delivery log -> teardown -> chart. Output
# files land in scripts/results/<label>/; rerunning a
# label overwrites its files.
#
# Three fault modes:
#   default (chaos.py)  -- admin endpoint on receiver_mock switches on
#                           synthetic errors/latency/capacity on a schedule
#   --surge-rate N       -- receiver_mock's capacity is set ONCE and left
#                           fixed; the fault is a real rate surge from
#                           load_generator.py exceeding that fixed capacity
#   --no-chaos            -- pure volume test, no fault at all
#
# --repeats N runs the whole condition N times, suffixing the label with
# _r1.._rN (labels are left unsuffixed for N=1, so existing single-run
# usage/output paths are unaffected).
#
# Usage:
#   ./run_experiment.sh <label> [--rate N] [--duration N] [--no-chaos] [--policy P] [--repeats N] [-- chaos.py args...]
#   ./run_experiment.sh <label> --surge-rate N [--rate N] [--steady N] [--surge-duration N] [--recovery N] [--capacity N] [--poisson] [--policy P] [--repeats N]
#
# Examples:
#   ./run_experiment.sh none_maxconcurrency -- --max-concurrency 3
#   ./run_experiment.sh concurrent_volume --rate 150 --duration 60 --no-chaos
#   ./run_experiment.sh naive_surge --policy naive --rate 10 --surge-rate 30 --capacity 5 --poisson --repeats 3

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
  echo "Usage: ./run_experiment.sh <label> [--rate N] [--duration N] [--no-chaos] [--policy P] [--repeats N] [--surge-rate N ...] [-- chaos.py args...]"
  exit 1
fi

LABEL="$1"
shift

RATE=10
DURATION=50
NO_CHAOS=false
POLICY="none"
REPEATS=1
SURGE_RATE=""
CAPACITY=5
SERVICE_LATENCY_MS=50
POISSON=false
STEADY=15
SURGE_DURATION=40
RECOVERY=90
CHAOS_ARGS=()

while [[ $# -gt 0 ]]; do
  case "$1" in
    --rate) RATE="$2"; shift 2 ;;
    --duration) DURATION="$2"; shift 2 ;;
    --no-chaos) NO_CHAOS=true; shift ;;
    --policy) POLICY="$2"; shift 2 ;;
    --repeats) REPEATS="$2"; shift 2 ;;
    --surge-rate) SURGE_RATE="$2"; shift 2 ;;
    --capacity) CAPACITY="$2"; shift 2 ;;
    --service-latency-ms) SERVICE_LATENCY_MS="$2"; shift 2 ;;
    --poisson) POISSON=true; shift ;;
    --steady) STEADY="$2"; shift 2 ;;
    --surge-duration) SURGE_DURATION="$2"; shift 2 ;;
    --recovery) RECOVERY="$2"; shift 2 ;;
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

if [ -n "$SURGE_RATE" ]; then
  TOTAL_DURATION=$(( ${STEADY%.*} + ${SURGE_DURATION%.*} + ${RECOVERY%.*} ))
else
  TOTAL_DURATION="$DURATION"
fi
# Same idea for the send phase -- total duration + 2 minutes of slack
# before concluding load_generator.py/chaos.py is actually stuck.
SEND_TIMEOUT_SECONDS="${SEND_TIMEOUT_SECONDS:-$((TOTAL_DURATION + 120))}"

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

run_once() {
  local run_label="$1"

  local run_dir="$RESULTS_DIR/$run_label"
  mkdir -p "$run_dir"
  local ingest_csv="$run_dir/ingest.csv"
  local delivery_csv="$run_dir/delivery.csv"
  local timeline_json="$run_dir/timeline.json"
  local chart_png="$run_dir/chart.png"
  local pg_connections_csv="$run_dir/pg_connections.csv"

  echo "=== [$run_label] rebuilding containers (policy=$POLICY) ==="
  cd "$PROJECT_ROOT"
  "${DOCKER_COMPOSE[@]}" down -v
  RETRY_POLICY="$POLICY" "${DOCKER_COMPOSE[@]}" up --build -d --wait
  sleep 3

  # Samples Postgres's active connection count once a second for the whole
  # ingest+drain window; killed explicitly once drain-wait finishes.
  echo "timestamp,active_connections" > "$pg_connections_csv"
  (
    while true; do
      COUNT=$(docker exec postgres-dev psql -U postgres -d eventpulse -t -c \
        "SELECT count(*) FROM pg_stat_activity;" 2>/dev/null | tr -d ' ')
      TS=$(date -u +"%Y-%m-%dT%H:%M:%S")
      echo "$TS,$COUNT" >> "$pg_connections_csv"
      sleep 1
    done
  ) &
  local pg_monitor_pid=$!

  cd "$SCRIPT_DIR"

  if [ "$NO_CHAOS" = true ]; then
    echo "=== [$run_label] pure volume test, no fault -- rate=$RATE duration=${DURATION}s ==="
    python3 load_generator.py \
      --rate "$RATE" --duration "$DURATION" \
      --api-url "http://localhost:8000" \
      --endpoint-url "http://receiver_mock:9000/webhook" \
      --output "$ingest_csv" &
    local loadgen_pid=$!
    wait_with_timeout "$loadgen_pid" "$SEND_TIMEOUT_SECONDS" "load_generator.py" || \
      echo "  continuing with whatever data was captured before the kill"

  elif [ -n "$SURGE_RATE" ]; then
    echo "=== [$run_label] surge fault -- base rate=$RATE, surge rate=$SURGE_RATE, capacity=$CAPACITY @ ${SERVICE_LATENCY_MS}ms, steady/surge/recovery=${STEADY}/${SURGE_DURATION}/${RECOVERY}s ==="
    # Fixed capacity AND fixed service latency for the whole run -- the
    # fault is the real rate surge exceeding the resulting sustainable
    # throughput (capacity / service_time), not an admin call flipping
    # synthetic errors on/off. latency_ms=0 here would make max_concurrency
    # meaningless: with ~instant processing, slots free up too fast for
    # any realistic rate to saturate them.
    curl -s -X POST "http://localhost:9000/admin/chaos" \
      -H "Content-Type: application/json" \
      -d "{\"max_concurrency\": $CAPACITY, \"reject_rate\": 0, \"latency_ms\": $SERVICE_LATENCY_MS}" > /dev/null

    python3 -c "
mu = $CAPACITY / ($SERVICE_LATENCY_MS / 1000)
print(f'  sustainable throughput (mu) = capacity/service_time = {$CAPACITY}/{$SERVICE_LATENCY_MS}ms = {mu:.1f} req/s')
print(f'  steady rho = rate/mu = {$RATE}/{mu:.1f} = {$RATE/mu:.2f}' + ('  (WARNING: >=1, steady phase is already overloaded)' if $RATE/mu >= 1 else ''))
print(f'  surge rho  = surge-rate/mu = {$SURGE_RATE}/{mu:.1f} = {$SURGE_RATE/mu:.2f}' + ('  (WARNING: <1, surge will not actually overload the receiver)' if $SURGE_RATE/mu < 1 else ''))
"

    local poisson_flag=()
    [ "$POISSON" = true ] && poisson_flag=(--poisson)

    python3 load_generator.py \
      --rate "$RATE" --surge-rate "$SURGE_RATE" \
      --steady "$STEADY" --surge-duration "$SURGE_DURATION" --recovery "$RECOVERY" \
      "${poisson_flag[@]}" \
      --api-url "http://localhost:8000" \
      --endpoint-url "http://receiver_mock:9000/webhook" \
      --output "$ingest_csv" --timeline-output "$timeline_json" &
    local loadgen_pid=$!
    wait_with_timeout "$loadgen_pid" "$SEND_TIMEOUT_SECONDS" "load_generator.py" || \
      echo "  continuing with whatever data was captured before the kill"

  else
    echo "=== [$run_label] load generator + chaos -- rate=$RATE duration=${DURATION}s ==="
    python3 load_generator.py \
      --rate "$RATE" --duration "$DURATION" \
      --api-url "http://localhost:8000" \
      --endpoint-url "http://receiver_mock:9000/webhook" \
      --output "$ingest_csv" &
    local loadgen_pid=$!

    python3 chaos.py --steady 10 --fault 20 --recovery 20 \
      --timeline-output "$timeline_json" \
      "${CHAOS_ARGS[@]}" &
    local chaos_pid=$!

    wait_with_timeout "$loadgen_pid" "$SEND_TIMEOUT_SECONDS" "load_generator.py" || \
      echo "  continuing with whatever data was captured before the kill"
    wait_with_timeout "$chaos_pid" 90 "chaos.py" || \
      echo "  continuing -- chaos.py's own schedule is short, this shouldn't normally trigger"
  fi

  echo "=== [$run_label] waiting for worker to drain remaining backlog (timeout ${DRAIN_TIMEOUT_SECONDS}s) ==="
  cd "$PROJECT_ROOT"
  local start_time
  start_time=$(date +%s)
  while true; do
    local remaining
    remaining=$(python3 - <<'PY'
import redis
r = redis.Redis(host='localhost', port=6379, decode_responses=True)
groups = r.xinfo_groups('deliveries')
g = groups[0]
lag = g.get('lag') or 0
pending = g.get('pending') or 0
print(lag + pending)
PY
)
    echo "  remaining (undelivered + unacked): $remaining"
    if [ "$remaining" -le 0 ]; then
      break
    fi
    local now elapsed
    now=$(date +%s)
    elapsed=$((now - start_time))
    if [ "$elapsed" -ge "$DRAIN_TIMEOUT_SECONDS" ]; then
      echo "  WARNING: backlog did not drain within ${DRAIN_TIMEOUT_SECONDS}s; continuing with partial results"
      break
    fi
    sleep 1
  done

  echo "=== [$run_label] extracting worker delivery log ==="
  kill "$pg_monitor_pid" 2>/dev/null || true
  wait "$pg_monitor_pid" 2>/dev/null || true
  "${DOCKER_COMPOSE[@]}" stop worker
  docker cp eventpulse-worker:/app/delivery_log.csv "$delivery_csv"

  echo "=== [$run_label] tearing down ==="
  "${DOCKER_COMPOSE[@]}" down -v

  echo "=== [$run_label] generating chart ==="
  cd "$SCRIPT_DIR"
  local analyze_args=(--file "$run_label:$delivery_csv" --output "$chart_png")
  if [ -f "$timeline_json" ]; then
    analyze_args+=(--timeline "$run_label:$timeline_json")
  fi
  python3 analyze.py "${analyze_args[@]}"

  echo "=== [$run_label] done -- results in scripts/results/${run_label}/ ==="
  local peak_connections
  peak_connections=$(tail -n +2 "$pg_connections_csv" | awk -F, '{print $2}' | sort -n | tail -1)
  echo "  peak Postgres active connections during this run: ${peak_connections:-unknown} (server max_connections is typically 100)"
  ls -la "$run_dir"
}

if [ "$REPEATS" -le 1 ]; then
  run_once "$LABEL"
else
  for i in $(seq 1 "$REPEATS"); do
    echo "############ REPEAT $i/$REPEATS ############"
    run_once "${LABEL}_r${i}"
  done
fi
