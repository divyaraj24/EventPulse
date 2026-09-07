"""
Canonical, harness-callable version of fault injection -- async so it
runs concurrently with load_generator.run() in the harness's own event
loop via asyncio.gather(), not as a separate subprocess. The only
caller is harness/service/main.py; the old standalone CLI script this
replaced is gone (see harness/main.py for the current CLI entry point).
"""
import asyncio
import json
from datetime import datetime, timezone
from typing import Callable, Optional

import httpx


async def call_admin(client: httpx.AsyncClient, receiver_url: str, path: str, payload: Optional[dict] = None):
    if payload is None:
        response = await client.post(f"{receiver_url}{path}")
    else:
        response = await client.post(f"{receiver_url}{path}", json=payload)
    response.raise_for_status()
    return response.json()


async def run(
    receiver_url: str,
    steady: float,
    fault: float,
    recovery: float,
    max_concurrency: int,
    reject_rate: float,
    latency_ms: int,
    recovered_max_concurrency: int,
    recovered_latency_ms: int,
    timeline_output: Optional[str] = None,
    on_phase_change: Optional[Callable[[str], None]] = None,
) -> dict:
    timeline = {
        "params": {
            "receiver_url": receiver_url, "steady": steady, "fault": fault, "recovery": recovery,
            "max_concurrency": max_concurrency, "reject_rate": reject_rate, "latency_ms": latency_ms,
            "recovered_max_concurrency": recovered_max_concurrency,
            "recovered_latency_ms": recovered_latency_ms,
        }
    }

    async with httpx.AsyncClient(timeout=5.0) as client:
        await call_admin(client, receiver_url, "/admin/reset")
        timeline["steady_start"] = datetime.now(timezone.utc).isoformat()
        if on_phase_change:
            on_phase_change("steady")
        print(f"[harness.chaos] steady state for {steady}s")
        await asyncio.sleep(steady)

        timeline["fault_start"] = datetime.now(timezone.utc).isoformat()
        await call_admin(client, receiver_url, "/admin/chaos", {
            "reject_rate": reject_rate,
            "latency_ms": latency_ms,
            "max_concurrency": max_concurrency,
        })
        if on_phase_change:
            on_phase_change("fault")
        print(f"[harness.chaos] FAULT TRIGGERED -- max_concurrency={max_concurrency}, "
              f"reject_rate={reject_rate}, latency_ms={latency_ms} -- holding for {fault}s")
        await asyncio.sleep(fault)

        timeline["fault_end"] = datetime.now(timezone.utc).isoformat()
        # Not a full /admin/reset -- snapping to unconstrained capacity would
        # let the whole backlog drain in one burst, hiding the difference
        # between retry policies.
        await call_admin(client, receiver_url, "/admin/chaos", {
            "reject_rate": 0.0,
            "latency_ms": recovered_latency_ms,
            "max_concurrency": recovered_max_concurrency,
        })
        if on_phase_change:
            on_phase_change("recovery")
        print(f"[harness.chaos] fault cleared -- recovered capacity: max_concurrency={recovered_max_concurrency}, "
              f"latency_ms={recovered_latency_ms} -- observing recovery for {recovery}s")
        await asyncio.sleep(recovery)

        timeline["observation_end"] = datetime.now(timezone.utc).isoformat()
        # Deliberately not resetting here either -- whatever calls this
        # tears the stack down afterward anyway.

    if timeline_output:
        with open(timeline_output, "w") as f:
            json.dump(timeline, f, indent=2)
        print(f"[harness.chaos] wrote timeline to {timeline_output}")

    return timeline
