"""
CLI client for the harness API -- the Python replacement for the old
scripts/run_experiment.sh bash orchestrator. This talks to an
already-running harness service over HTTP; it doesn't reimplement any
orchestration logic itself (that all lives in harness/service/main.py).
Start the harness stack first if it isn't already up:

    docker compose -f harness/docker-compose.yml up -d --wait

Usage:
    python harness/main.py naive_hardfault --policy naive --rate 15 --duration 180 \\
        --chaos --steady 15 --fault 90 --recovery 60 --max-concurrency 1 \\
        --latency-ms 300 --recovered-max-concurrency 2

    python harness/main.py smoke --rate 10 --duration 15

    python harness/main.py hardfault_sweep --policy adaptive --rate 15 --duration 120 \\
        --chaos --repeats 3
"""
import argparse
import sys
import time

import httpx

DEFAULT_HARNESS_URL = "http://localhost:8080"


def compute_rho(rate: float, max_concurrency: int, latency_ms: int, worker_concurrency: int):
    """rho = offered rate / sustainable service rate. mu is capped by
    whichever concurrency is smaller -- the worker's own semaphore only
    ever produces delay/backlog (httpx's timeout covers just the network
    call, not time spent waiting for a slot), while the receiver's own
    ceiling produces real 503s the instant demand exceeds it. Empirically
    derived this session, not from the RetryGuard paper directly (its
    model has no worker-side capacity tier at all)."""
    effective_concurrency = min(max_concurrency, worker_concurrency)
    if latency_ms <= 0 or effective_concurrency <= 0:
        return None, None, None
    mu = effective_concurrency / (latency_ms / 1000)
    binding_side = "worker" if worker_concurrency < max_concurrency else "receiver"
    return rate / mu, mu, binding_side


def print_rho(phase_label: str, rate: float, max_concurrency: int, latency_ms: int, worker_concurrency: int):
    if latency_ms <= 0:
        print(f"[cli] {phase_label}: latency=0ms -- concurrency ceiling never binds, no real backpressure")
        return
    rho, mu, binding_side = compute_rho(rate, max_concurrency, latency_ms, worker_concurrency)
    verdict = "OVERLOAD" if rho >= 1 else "under capacity"
    print(f"[cli] {phase_label}: rho={rho:.2f} (capacity {mu:.1f} ev/s vs {rate} ev/s offered) -- "
          f"{verdict}, binding side: {binding_side}")


def run_once(client: httpx.Client, args, label: str, output_dir: str) -> bool:
    """Runs one experiment end to end. Returns True on success (done),
    False on failure/cancellation -- never raises for those outcomes, only
    for connection/HTTP errors, so a --repeats sweep can keep going."""
    payload = {
        "label": label,
        "rate": args.rate,
        "duration": args.duration,
        "policy": args.policy,
        "endpoint_id": args.endpoint_id,
        "worker_concurrency": args.worker_concurrency,
        "poisson": args.poisson,
        "chaos": {
            "enabled": args.chaos,
            "steady": args.steady,
            "fault": args.fault,
            "recovery": args.recovery,
            "max_concurrency": args.max_concurrency,
            "reject_rate": args.reject_rate,
            "latency_ms": args.latency_ms,
            "recovered_max_concurrency": args.recovered_max_concurrency,
            "recovered_latency_ms": args.recovered_latency_ms,
        },
    }

    try:
        resp = client.post(f"{args.harness_url}/test/start", json=payload)
    except httpx.ConnectError:
        print(f"ERROR: couldn't reach the harness at {args.harness_url} -- is it running?\n"
              f"  docker compose -f harness/docker-compose.yml up -d --wait", file=sys.stderr)
        sys.exit(1)

    if resp.status_code == 409:
        print(f"ERROR: {resp.json()['detail']}", file=sys.stderr)
        sys.exit(1)
    resp.raise_for_status()
    run_id = resp.json()["run_id"]
    print(f"[cli] started run {run_id} (label={label})")

    status = {}
    while True:
        status_resp = client.get(f"{args.harness_url}/test/status/{run_id}")
        status_resp.raise_for_status()
        status = status_resp.json()
        print(f"[cli] status={status['status']} progress={status['progress']} "
              f"chaos_phase={status.get('chaos_phase')}")
        if status["status"] in ("done", "failed", "cancelled"):
            break
        time.sleep(args.poll_interval)

    if status["status"] in ("failed", "cancelled"):
        print(f"[cli] {status['status'].upper()}: {status.get('error')}", file=sys.stderr)
        return False

    chart_resp = client.get(f"{args.harness_url}/test/result/{run_id}/chart.png")
    chart_resp.raise_for_status()
    output_path = f"{output_dir}/{label}_chart.png"
    with open(output_path, "wb") as f:
        f.write(chart_resp.content)
    print(f"[cli] done -- chart saved to {output_path}")
    return True


def main():
    parser = argparse.ArgumentParser(description="Run an EventPulse experiment via the harness API")
    parser.add_argument("label")
    parser.add_argument("--harness-url", default=DEFAULT_HARNESS_URL)
    parser.add_argument("--rate", type=float, default=10)
    parser.add_argument("--duration", type=float, default=50)
    parser.add_argument("--policy", default="none", choices=["none", "naive", "adaptive"])
    parser.add_argument("--endpoint-id", default="test1")
    parser.add_argument("--worker-concurrency", type=int, default=1000,
                         help="worker's max concurrent delivery attempts -- non-binding by default "
                              "(matches receiver_mock's own healthy-state default) so the receiver's "
                              "max-concurrency is the sole capacity constraint; lower it deliberately "
                              "to study worker-side overshoot as its own variable")
    parser.add_argument("--poisson", action="store_true",
                         help="exponential inter-arrival times instead of fixed-interval pacing -- "
                              "matches RetryGuard's own M/M/1/m theoretical model (Sec V), at the cost "
                              "of adding arrival-timing variance as an extra uncontrolled factor; "
                              "default is fixed pacing, matching every existing result set")
    parser.add_argument("--chaos", action="store_true", help="enable fault injection (default: pure volume, no fault)")
    parser.add_argument("--steady", type=float, default=15.0, help="seconds of healthy baseline before the fault")
    parser.add_argument("--fault", type=float, default=40.0, help="seconds the fault stays active")
    parser.add_argument("--recovery", type=float, default=60.0, help="seconds to observe after the fault clears")
    parser.add_argument("--max-concurrency", type=int, default=3, help="capacity ceiling during the fault")
    parser.add_argument("--reject-rate", type=float, default=0.0, help="random rejection probability during the fault")
    parser.add_argument("--latency-ms", type=int, default=300, help="added latency during the fault")
    parser.add_argument("--recovered-max-concurrency", type=int, default=5)
    parser.add_argument("--recovered-latency-ms", type=int, default=100)
    parser.add_argument("--output-dir", default=".", help="where to save the fetched chart.png")
    parser.add_argument("--poll-interval", type=float, default=2.0)
    parser.add_argument("--repeats", type=int, default=1,
                         help="rerun the whole condition N times, labeled <label>_r1.._rN "
                              "(matches RetryGuard's own 3-repeats-per-condition methodology)")
    args = parser.parse_args()

    if args.chaos:
        print_rho("Fault", args.rate, args.max_concurrency, args.latency_ms, args.worker_concurrency)
        print_rho("Recovery", args.rate, args.recovered_max_concurrency, args.recovered_latency_ms, args.worker_concurrency)

    with httpx.Client(timeout=10.0) as client:
        if args.repeats <= 1:
            ok = run_once(client, args, args.label, args.output_dir)
            sys.exit(0 if ok else 1)

        results = []
        for i in range(1, args.repeats + 1):
            label = f"{args.label}_r{i}"
            print(f"[cli] --- repeat {i}/{args.repeats} ---")
            results.append(run_once(client, args, label, args.output_dir))

        succeeded = sum(results)
        print(f"[cli] repeats complete: {succeeded}/{args.repeats} succeeded")
        sys.exit(0 if all(results) else 1)


if __name__ == "__main__":
    main()
