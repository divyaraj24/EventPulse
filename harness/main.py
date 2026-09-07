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
"""
import argparse
import sys
import time

import httpx

DEFAULT_HARNESS_URL = "http://localhost:8080"


def main():
    parser = argparse.ArgumentParser(description="Run an EventPulse experiment via the harness API")
    parser.add_argument("label")
    parser.add_argument("--harness-url", default=DEFAULT_HARNESS_URL)
    parser.add_argument("--rate", type=float, default=10)
    parser.add_argument("--duration", type=float, default=50)
    parser.add_argument("--policy", default="none", choices=["none", "naive", "adaptive"])
    parser.add_argument("--endpoint-id", default="test1")
    parser.add_argument("--worker-concurrency", type=int, default=20,
                         help="worker's max concurrent delivery attempts -- match this to "
                              "--max-concurrency/--recovered-max-concurrency to remove worker-side "
                              "overshoot as a variable and isolate the receiver's own capacity ceiling")
    parser.add_argument("--chaos", action="store_true", help="enable fault injection (default: pure volume, no fault)")
    parser.add_argument("--steady", type=float, default=15.0, help="seconds of healthy baseline before the fault")
    parser.add_argument("--fault", type=float, default=40.0, help="seconds the fault stays active")
    parser.add_argument("--recovery", type=float, default=60.0, help="seconds to observe after the fault clears")
    parser.add_argument("--max-concurrency", type=int, default=3, help="capacity ceiling during the fault")
    parser.add_argument("--reject-rate", type=float, default=0.0, help="random rejection probability during the fault")
    parser.add_argument("--latency-ms", type=int, default=0, help="added latency during the fault")
    parser.add_argument("--recovered-max-concurrency", type=int, default=5)
    parser.add_argument("--recovered-latency-ms", type=int, default=200)
    parser.add_argument("--output-dir", default=".", help="where to save the fetched chart.png")
    parser.add_argument("--poll-interval", type=float, default=2.0)
    args = parser.parse_args()

    payload = {
        "label": args.label,
        "rate": args.rate,
        "duration": args.duration,
        "policy": args.policy,
        "endpoint_id": args.endpoint_id,
        "worker_concurrency": args.worker_concurrency,
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

    with httpx.Client(timeout=10.0) as client:
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
        print(f"[cli] started run {run_id}")

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
            sys.exit(1)

        chart_resp = client.get(f"{args.harness_url}/test/result/{run_id}/chart.png")
        chart_resp.raise_for_status()
        output_path = f"{args.output_dir}/{args.label}_chart.png"
        with open(output_path, "wb") as f:
            f.write(chart_resp.content)
        print(f"[cli] done -- chart saved to {output_path}")


if __name__ == "__main__":
    main()
