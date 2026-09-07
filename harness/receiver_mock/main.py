import asyncio
import hashlib
import hmac
import os
import random

from fastapi import FastAPI, Request, Response
from pydantic import BaseModel

SIGNING_SECRET = os.getenv("SIGNING_SECRET", "dev-secret-change-me").encode()

app = FastAPI(title="EventPulse Mock Receiver")

# The receiver's normal operating point -- not a literal zero-latency,
# unbounded-concurrency idealization. Real webhook receivers always have
# some baseline processing/network time, and every request pays it, not
# just ones during a fault; the fault only deepens it. Modeling that
# matters for two reasons: (1) AdaptivePolicy's latency signal learns its
# trip threshold as a multiple of the observed baseline, and a baseline of
# ~0ms makes that multiple meaningless (any tiny bump reads as a huge
# ratio); (2) a genuinely finite background capacity is what lets "the
# fault clears, revert to background" work as recovery at all -- reverting
# to a literally unconstrained state would let any backlog burst through
# in a single instant regardless of retry policy, which is exactly why a
# separate "recovered" tier used to exist.
#
# The headroom over this project's typical offered rate (~15 ev/s) has to
# be deliberately moderate, not maximal: an earlier version of this used
# 20ms/50-concurrent (mu=2500 ev/s, ~0.6% utilization) and it neutralized
# the whole experiment -- a real hardfault run that previously never
# recovered (33% rejection rate, 7305 retries) instead recovered cleanly
# in 2.2s, because reverting to that much headroom left no bottleneck
# anywhere for a retry storm to actually get stuck against. 100ms/10-
# concurrent (mu=100 ev/s, ~15% utilization) is still comfortably healthy
# -- 6-7x headroom over typical offered load -- without being so generous
# that a severe fault's backlog clears before retry pressure matters.
BACKGROUND_LATENCY_MS = int(os.getenv("BACKGROUND_LATENCY_MS", "100"))
BACKGROUND_MAX_CONCURRENCY = int(os.getenv("BACKGROUND_MAX_CONCURRENCY", "10"))


class ChaosState:
    def __init__(self):
        self.reject_rate: float = 0.0
        self.latency_ms: int = BACKGROUND_LATENCY_MS
        self.max_concurrency: int = BACKGROUND_MAX_CONCURRENCY
        self.in_flight: int = 0


chaos = ChaosState()
state_lock = asyncio.Lock()

# Visibility only, for sanity-checking during dev -- goodput/latency charts
# read from the worker's own log, not this.
stats = {"received": 0, "accepted": 0, "rejected_capacity": 0, "rejected_rate": 0}


class ChaosConfig(BaseModel):
    reject_rate: float = 0.0
    latency_ms: int = BACKGROUND_LATENCY_MS
    max_concurrency: int = BACKGROUND_MAX_CONCURRENCY


@app.post("/admin/chaos")
async def set_chaos(config: ChaosConfig):
    chaos.reject_rate = config.reject_rate
    chaos.latency_ms = config.latency_ms
    chaos.max_concurrency = config.max_concurrency
    print(
        f"[receiver_mock] chaos updated: reject_rate={chaos.reject_rate}, "
        f"latency_ms={chaos.latency_ms}, max_concurrency={chaos.max_concurrency}"
    )
    return {"status": "ok", "chaos": config.model_dump()}


@app.get("/admin/chaos")
async def get_chaos():
    return {
        "reject_rate": chaos.reject_rate,
        "latency_ms": chaos.latency_ms,
        "max_concurrency": chaos.max_concurrency,
        "in_flight": chaos.in_flight,
        "stats": stats,
    }


@app.post("/admin/reset")
async def reset_chaos():
    chaos.reject_rate = 0.0
    chaos.latency_ms = BACKGROUND_LATENCY_MS
    chaos.max_concurrency = BACKGROUND_MAX_CONCURRENCY
    stats.update({"received": 0, "accepted": 0, "rejected_capacity": 0, "rejected_rate": 0})
    print(f"[receiver_mock] reset to background defaults "
          f"(latency_ms={BACKGROUND_LATENCY_MS}, max_concurrency={BACKGROUND_MAX_CONCURRENCY})")
    return {"status": "reset"}


def verify_signature(body: bytes, signature: str) -> bool:
    expected = hmac.new(SIGNING_SECRET, body, hashlib.sha256).hexdigest()
    return hmac.compare_digest(expected, signature)


@app.post("/webhook")
async def receive_webhook(request: Request):
    stats["received"] += 1
    body = await request.body()
    signature = request.headers.get("X-EventPulse-Signature", "")

    if not verify_signature(body, signature):
        return Response(status_code=401, content="invalid signature")

    # Capacity ceiling: requests beyond max_concurrency are rejected
    # immediately, the way a real overloaded server sheds load.
    async with state_lock:
        if chaos.in_flight >= chaos.max_concurrency:
            stats["rejected_capacity"] += 1
            return Response(status_code=503, content="overloaded")
        chaos.in_flight += 1

    try:
        if random.random() < chaos.reject_rate:
            stats["rejected_rate"] += 1
            return Response(status_code=500, content="simulated failure")

        if chaos.latency_ms > 0:
            await asyncio.sleep(chaos.latency_ms / 1000)

        stats["accepted"] += 1
        return Response(status_code=200, content="ok")

    finally:
        async with state_lock:
            chaos.in_flight -= 1


@app.get("/health")
def health():
    return {"status": "ok"}
