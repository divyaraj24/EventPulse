"""
Sends events to the ingest API as an open-loop generator -- schedules sends
on their own timeline rather than waiting for each response, so a slow
receiver doesn't self-limit the offered rate and hide overload behavior.

Two arrival modes:
  - periodic (default): fixed inter-arrival interval, scheduled against an
    absolute target time so pacing doesn't drift over a long run.
  - --poisson: exponential inter-arrival times (memoryless), closer to real
    request traffic than a perfectly periodic schedule.

Rate can either be constant (--rate/--duration) or a three-phase surge
schedule (--surge-rate): steady at --rate, surge at --surge-rate, recovery
back at --rate. The surge is the fault trigger -- a real rate that exceeds
the receiver's real fixed capacity, causing genuine queueing/rejection,
rather than an admin endpoint switching on synthetic errors. Writes a
timeline JSON in the same shape chaos.py does (steady_start/fault_start/
fault_end/observation_end) so analyze.py needs no changes to read it.

Usage:
    python load_generator.py --rate 20 --duration 60 --output results.csv
    python load_generator.py --rate 10 --surge-rate 30 --steady 15 \\
        --surge-duration 40 --recovery 90 --poisson \\
        --output results.csv --timeline-output timeline.json

Run from the host machine (not inside Docker) -- it talks to the API's
host-exposed port (localhost:8000).
"""
import argparse
import asyncio
import csv
import json
import random
import time
from datetime import datetime, timezone
from typing import Any

import httpx


async def send_event(
    client: httpx.AsyncClient,
    api_url: str,
    endpoint_id: str,
    endpoint_url: str,
    writer: Any,  # csv._writer -- an internal type with no clean public name
    seq: int,
):
    payload = {
        "event_type": "payment.success",
        "endpoint_id": endpoint_id,
        "endpoint_url": endpoint_url,
        "payload": {"seq": seq},
    }

    send_started = time.monotonic()
    timestamp = datetime.now(timezone.utc).isoformat()

    try:
        response = await client.post(f"{api_url}/events", json=payload, timeout=5.0)
        latency_ms = (time.monotonic() - send_started) * 1000
        result = "ok" if response.status_code == 202 else "rejected"
        writer.writerow([timestamp, seq, response.status_code, f"{latency_ms:.2f}", result])
    except httpx.RequestError as e:
        latency_ms = (time.monotonic() - send_started) * 1000
        writer.writerow([timestamp, seq, "ERR", f"{latency_ms:.2f}", str(e)])


def build_schedule(args) -> list[tuple[float, float, float]]:
    """Returns [(phase_start_offset, rate), ...] -- a constant-rate phase,
    or three phases (steady/surge/recovery) if --surge-rate is set."""
    if args.surge_rate is None:
        return [(0.0, args.rate, args.duration)]
    return [
        (0.0, args.rate, args.steady),
        (args.steady, args.surge_rate, args.surge_duration),
        (args.steady + args.surge_duration, args.rate, args.recovery),
    ]


def generate_send_times(schedule: list[tuple[float, float, float]], poisson: bool) -> list[float]:
    """Flattens the phase schedule into a single sorted list of send offsets
    (seconds from t=0), each phase sampled at its own rate."""
    times = []
    for phase_start, rate, phase_duration in schedule:
        if rate <= 0 or phase_duration <= 0:
            continue
        t = phase_start
        phase_end = phase_start + phase_duration
        if poisson:
            while True:
                t += random.expovariate(rate)
                if t >= phase_end:
                    break
                times.append(t)
        else:
            interval = 1.0 / rate
            n = int(phase_duration * rate)
            times.extend(phase_start + i * interval for i in range(n))
    times.sort()
    return times


async def run(args):
    schedule = build_schedule(args)
    send_times = generate_send_times(schedule, args.poisson)

    with open(args.output, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["timestamp", "seq", "status_code", "latency_ms", "result"])

        # httpx's default connection cap (100) becomes an invisible
        # client-side bottleneck at higher offered rates.
        limits = httpx.Limits(max_connections=300, max_keepalive_connections=100)
        async with httpx.AsyncClient(limits=limits) as client:
            start = time.monotonic()
            timeline = {"params": vars(args)}
            timeline["steady_start"] = datetime.now(timezone.utc).isoformat()

            tasks = []
            surge_start_offset = args.steady if args.surge_rate is not None else None
            surge_end_offset = (args.steady + args.surge_duration) if args.surge_rate is not None else None
            marked_surge_start = marked_surge_end = False

            for seq, target_offset in enumerate(send_times):
                now_offset = time.monotonic() - start
                if target_offset > now_offset:
                    await asyncio.sleep(target_offset - now_offset)

                if surge_start_offset is not None and not marked_surge_start and target_offset >= surge_start_offset:
                    timeline["fault_start"] = datetime.now(timezone.utc).isoformat()
                    marked_surge_start = True
                if surge_end_offset is not None and not marked_surge_end and target_offset >= surge_end_offset:
                    timeline["fault_end"] = datetime.now(timezone.utc).isoformat()
                    marked_surge_end = True

                tasks.append(asyncio.create_task(
                    send_event(client, args.api_url, args.endpoint_id, args.endpoint_url, writer, seq)
                ))

            await asyncio.gather(*tasks)
            timeline["observation_end"] = datetime.now(timezone.utc).isoformat()

    total = len(send_times)
    print(f"[load_generator] sent {total} events -> {args.output}")

    if args.timeline_output and args.surge_rate is not None:
        with open(args.timeline_output, "w") as f:
            json.dump(timeline, f, indent=2)
        print(f"[load_generator] wrote timeline to {args.timeline_output}")


def main():
    parser = argparse.ArgumentParser(description="EventPulse load generator")
    parser.add_argument("--rate", type=float, required=True, help="events per second (base/steady rate)")
    parser.add_argument("--duration", type=float, default=None, help="how long to send at --rate, in seconds (ignored if --surge-rate is set)")
    parser.add_argument("--poisson", action="store_true", help="exponential inter-arrival times instead of fixed periodic spacing")
    parser.add_argument("--surge-rate", type=float, default=None, help="rate during the surge phase -- set this to trigger a real capacity mismatch against the receiver's fixed concurrency limit")
    parser.add_argument("--steady", type=float, default=15.0, help="seconds at --rate before the surge (surge mode only)")
    parser.add_argument("--surge-duration", type=float, default=40.0, help="seconds at --surge-rate (surge mode only)")
    parser.add_argument("--recovery", type=float, default=90.0, help="seconds back at --rate after the surge (surge mode only)")
    parser.add_argument("--timeline-output", default=None, help="path to write the surge timeline JSON (surge mode only)")
    parser.add_argument("--api-url", default="http://localhost:8000")
    parser.add_argument("--endpoint-id", default="test1")
    parser.add_argument("--endpoint-url", default="http://receiver_mock:9000/webhook")
    parser.add_argument("--output", default="results.csv")
    args = parser.parse_args()

    if args.surge_rate is None and args.duration is None:
        parser.error("--duration is required unless --surge-rate is set")

    asyncio.run(run(args))


if __name__ == "__main__":
    main()
