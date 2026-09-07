from datetime import datetime
from typing import Optional

from pydantic import BaseModel


class ChaosConfig(BaseModel):
    enabled: bool = False
    steady: float = 15.0
    fault: float = 40.0
    recovery: float = 60.0
    max_concurrency: int = 3
    reject_rate: float = 0.0
    latency_ms: int = 0
    recovered_max_concurrency: int = 5
    recovered_latency_ms: int = 200


class TestStartRequest(BaseModel):
    label: str
    rate: float
    duration: float
    policy: str = "none"
    endpoint_id: str = "test1"
    # Non-binding by default (matches receiver_mock's own healthy-state
    # default of 1000) so the receiver's own max_concurrency is the sole
    # definition of service capacity, matching RetryGuard's two-tier model
    # (Service A does the retrying, Service B has capacity mu -- Service A
    # has no throughput cap of its own). Set this lower only to deliberately
    # study worker-side overshoot as its own variable.
    worker_concurrency: int = 1000
    chaos: ChaosConfig = ChaosConfig()


class TestStartResponse(BaseModel):
    run_id: str
    status: str


class TestStatusResponse(BaseModel):
    run_id: str
    status: str
    label: str
    progress: dict
    chaos_phase: Optional[str] = None
    error: Optional[str] = None


class RunHistoryEntry(BaseModel):
    run_id: str
    label: str
    policy: str
    status: str
    started_at: datetime


class MessageResponse(BaseModel):
    detail: str
