"""
Sends events to the ingest API at a steady, controlled rate, so "offered
event rate" is a reproducible independent variable rather than just
hammering the API as fast as possible.

Usage:
    python load_generator.py --rate 20 --duration 60 --output results_naive.csv

Run from the host machine (not inside Docker) -- it talks to the API's
host-exposed port (localhost:8000).
"""
import argparse
import asyncio
import csv
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


async def run(rate: float, duration: float, api_url: str, endpoint_id: str, endpoint_url: str, output_path: str):
    interval = 1.0 / rate
    total_events = int(rate * duration)

    with open(output_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["timestamp", "seq", "status_code", "latency_ms", "result"])

        # httpx's default connection cap (100) becomes an invisible
        # client-side bottleneck at higher offered rates.
        limits = httpx.Limits(max_connections=300, max_keepalive_connections=100)
        async with httpx.AsyncClient(limits=limits) as client:
            start = time.monotonic()
            tasks = []

            for seq in range(total_events):
                # Schedule against an absolute target time rather than
                # sleeping `interval` each loop, so pacing doesn't drift late.
                target_time = start + seq * interval
                now = time.monotonic()
                if target_time > now:
                    await asyncio.sleep(target_time - now)

                tasks.append(asyncio.create_task(
                    send_event(client, api_url, endpoint_id, endpoint_url, writer, seq)
                ))

            await asyncio.gather(*tasks)

    print(f"[load_generator] sent {total_events} events over {duration}s (target rate {rate}/s) -> {output_path}")


def main():
    parser = argparse.ArgumentParser(description="EventPulse load generator")
    parser.add_argument("--rate", type=float, required=True, help="events per second")
    parser.add_argument("--duration", type=float, required=True, help="how long to send, in seconds")
    parser.add_argument("--api-url", default="http://localhost:8000")
    parser.add_argument("--endpoint-id", default="test1")
    parser.add_argument("--endpoint-url", default="http://receiver_mock:9000/webhook")
    parser.add_argument("--output", default="results.csv")
    args = parser.parse_args()

    asyncio.run(run(args.rate, args.duration, args.api_url, args.endpoint_id, args.endpoint_url, args.output))


if __name__ == "__main__":
    main()
