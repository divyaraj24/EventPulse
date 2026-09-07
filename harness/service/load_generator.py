"""
Canonical, harness-callable version of load generation -- an async
function the harness service awaits directly (via asyncio.gather
alongside chaos.py), not a subprocess. The only caller is
harness/service/main.py; the old standalone CLI script this replaced
is gone (see harness/main.py for the current CLI entry point).
"""
import asyncio
import csv
import random
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
    poisson: bool = False,
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
        # Bound concurrent in-flight sends via a semaphore -- the same
        # pattern worker.py uses for delivery concurrency -- rather than
        # periodically blocking the whole scheduling loop on a full batch.
        # A batch-drain stalls real wall-clock time whenever one batch is
        # slow, and since pacing is anchored to absolute target times
        # (start + seq*interval), the loop then fires a catch-up burst
        # right after the drain to make up the lost ground. A semaphore
        # lets sends flow continuously -- each task blocks only on its own
        # acquire, never the scheduling loop -- so pacing stays smooth even
        # when the receiver is genuinely slow, not just drift-free in
        # aggregate.
        concurrency_limit = max(1, int(rate))
        semaphore = asyncio.Semaphore(concurrency_limit)

        async with httpx.AsyncClient(limits=limits) as client:
            async def send_bounded(seq: int):
                async with semaphore:
                    await send_event(client, api_url, endpoint_id, endpoint_url, writer, f, seq)

            start = time.monotonic()
            tasks: list[asyncio.Task] = []
            next_poisson_time = start

            for seq in range(total_events):
                if poisson:
                    # Exponential inter-arrival time (mean 1/rate) --
                    # matches the Poisson arrival process RetryGuard's own
                    # M/M/1/m analysis (Sec V) assumes, unlike fixed-interval
                    # pacing's more deterministic arrival process. Opt-in
                    # only: existing results were all produced with fixed
                    # pacing, and a controlled comparison across retry
                    # policies benefits from not adding incidental
                    # arrival-timing variance as an extra uncontrolled factor.
                    next_poisson_time += random.expovariate(rate)
                    target_time = next_poisson_time
                else:
                    # Schedule against an absolute target time rather than
                    # sleeping `interval` each loop, so pacing doesn't drift late.
                    target_time = start + seq * interval
                now = time.monotonic()
                if target_time > now:
                    await asyncio.sleep(target_time - now)

                tasks.append(asyncio.create_task(send_bounded(seq)))
                if on_progress and (seq + 1) % 10 == 0:
                    on_progress(seq + 1, total_events)
                    # Drop references to already-finished tasks so the list
                    # itself doesn't grow unboundedly over a long run --
                    # completed tasks have already written+flushed their row.
                    tasks = [t for t in tasks if not t.done()]

            await asyncio.gather(*tasks)
            if on_progress:
                on_progress(total_events, total_events)

    print(f"[harness.load_generator] sent {total_events} events over {duration}s (target rate {rate}/s) -> {output_path}")
