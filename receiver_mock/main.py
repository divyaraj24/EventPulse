import asyncio
import hashlib
import hmac
import os
import random

from fastapi import FastAPI, Request, Response
from pydantic import BaseModel

SIGNING_SECRET = os.getenv("SIGNING_SECRET", "dev-secret-change-me").encode()

app = FastAPI(title="EventPulse Mock Receiver")


class ChaosState:
    def __init__(self):
        self.reject_rate: float = 0.0
        self.latency_ms: int = 0
        self.max_concurrency: int = 1000
        self.in_flight: int = 0


chaos = ChaosState()
state_lock = asyncio.Lock()

# Visibility only, for sanity-checking during dev -- goodput/latency charts
# read from the worker's own log, not this.
stats = {"received": 0, "accepted": 0, "rejected_capacity": 0, "rejected_rate": 0}


class ChaosConfig(BaseModel):
    reject_rate: float = 0.0
    latency_ms: int = 0
    max_concurrency: int = 1000


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
    chaos.latency_ms = 0
    chaos.max_concurrency = 1000
    stats.update({"received": 0, "accepted": 0, "rejected_capacity": 0, "rejected_rate": 0})
    print("[receiver_mock] reset to healthy defaults")
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
