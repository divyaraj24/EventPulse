"""
Drives the mock receiver through steady -> fault -> recovery, and writes
chaos_timeline.json with the trigger/clear timestamps so analyze.py can
shade the fault window and measure recovery time precisely.

Run in parallel with load_generator.py (its --duration should cover
--steady + --fault + --recovery so it's still sending when the fault clears).

Usage:
    python chaos.py --steady 10 --fault 20 --recovery 20 --max-concurrency 3
"""
import argparse
import json
import time
from datetime import datetime, timezone

import httpx


def call_admin(client: httpx.Client, receiver_url: str, path: str, payload: dict | None = None):
    if payload is None:
        response = client.post(f"{receiver_url}{path}")
    else:
        response = client.post(f"{receiver_url}{path}", json=payload)
    response.raise_for_status()
    return response.json()


def main():
    parser = argparse.ArgumentParser(description="Drive the mock receiver through a steady/fault/recovery cycle")
    parser.add_argument("--receiver-url", default="http://localhost:9000")
    parser.add_argument("--steady", type=float, default=10.0, help="seconds of healthy baseline before the fault")
    parser.add_argument("--fault", type=float, default=20.0, help="seconds the fault stays active")
    parser.add_argument("--recovery", type=float, default=20.0, help="seconds to wait after clearing, for logging only")
    parser.add_argument("--max-concurrency", type=int, default=3, help="capacity ceiling during the fault -- the real bottleneck")
    parser.add_argument("--reject-rate", type=float, default=0.0, help="random rejection probability during the fault")
    parser.add_argument("--latency-ms", type=int, default=0, help="added latency during the fault")
    parser.add_argument(
        "--recovered-max-concurrency", type=int, default=5,
        help="capacity ceiling once the fault 'clears' -- kept bounded so backlog drains "
             "gradually instead of bursting through in one bucket",
    )
    parser.add_argument(
        "--recovered-latency-ms", type=int, default=200,
        help="added latency once the fault 'clears' -- models a still-degraded endpoint",
    )
    parser.add_argument("--timeline-output", default="chaos_timeline.json")
    args = parser.parse_args()

    timeline = {"params": vars(args)}

    with httpx.Client(timeout=5.0) as client:
        call_admin(client, args.receiver_url, "/admin/reset")
        timeline["steady_start"] = datetime.now(timezone.utc).isoformat()
        print(f"[chaos] steady state for {args.steady}s")
        time.sleep(args.steady)

        timeline["fault_start"] = datetime.now(timezone.utc).isoformat()
        call_admin(client, args.receiver_url, "/admin/chaos", {
            "reject_rate": args.reject_rate,
            "latency_ms": args.latency_ms,
            "max_concurrency": args.max_concurrency,
        })
        print(f"[chaos] FAULT TRIGGERED -- max_concurrency={args.max_concurrency}, "
              f"reject_rate={args.reject_rate}, latency_ms={args.latency_ms} -- holding for {args.fault}s")
        time.sleep(args.fault)

        timeline["fault_end"] = datetime.now(timezone.utc).isoformat()
        # Not a full /admin/reset -- snapping to unconstrained capacity would
        # let the whole backlog drain in one burst, hiding the difference
        # between retry policies.
        call_admin(client, args.receiver_url, "/admin/chaos", {
            "reject_rate": 0.0,
            "latency_ms": args.recovered_latency_ms,
            "max_concurrency": args.recovered_max_concurrency,
        })
        print(f"[chaos] fault cleared -- recovered capacity: max_concurrency={args.recovered_max_concurrency}, "
              f"latency_ms={args.recovered_latency_ms} -- observing recovery for {args.recovery}s")
        time.sleep(args.recovery)

        timeline["observation_end"] = datetime.now(timezone.utc).isoformat()
        # Deliberately not resetting here either -- whatever calls this
        # script tears the stack down afterward anyway.

    with open(args.timeline_output, "w") as f:
        json.dump(timeline, f, indent=2)
    print(f"[chaos] wrote timeline to {args.timeline_output}")


if __name__ == "__main__":
    main()
