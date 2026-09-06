"""
Canonical, harness-callable version of load generation -- an async
function the harness service awaits directly (via asyncio.gather
alongside chaos.py, once that's wired in), not a subprocess. See
scripts/load_generator.py for the standalone CLI version kept for manual
use; the two may drift slightly, which is an accepted tradeoff (see
PROJECT_HISTORY.md) rather than a shared-package refactor.
"""
import asyncio
import csv
import time
from datetime import datetime, timezone
from typing import Any, Callable, Optional

import httpx


async def send_event(
    client: httpx.AsyncClient,
    api_url: str,
    endpoint_id: str,
    endpoint_url: str,
    writer: Any,
    f: Any,
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
    # Flush every row -- a killed run (or a crashed harness process) would
    # otherwise lose all buffered-but-unwritten rows, which is exactly what
    # happened to the abandoned surge experiment's adaptive run.
    f.flush()


async def run(
    rate: float,
    duration: float,
    api_url: str,
    endpoint_id: str,
    endpoint_url: str,
    output_path: str,
    on_progress: Optional[Callable[[int, int], None]] = None,
):
    interval = 1.0 / rate
    total_events = int(rate * duration)

    with open(output_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["timestamp", "seq", "status_code", "latency_ms", "result"])
        f.flush()

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
                    send_event(client, api_url, endpoint_id, endpoint_url, writer, f, seq)
                ))
                if on_progress and (seq + 1) % 10 == 0:
                    on_progress(seq + 1, total_events)

            await asyncio.gather(*tasks)
            if on_progress:
                on_progress(total_events, total_events)

    print(f"[harness.load_generator] sent {total_events} events over {duration}s (target rate {rate}/s) -> {output_path}")
