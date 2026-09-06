from typing import Optional

from pydantic import BaseModel


class TestStartRequest(BaseModel):
    label: str
    rate: float
    duration: float
    policy: str = "none"
    endpoint_id: str = "test1"


class TestStartResponse(BaseModel):
    run_id: str
    status: str


class TestStatusResponse(BaseModel):
    run_id: str
    status: str
    label: str
    progress: dict
    error: Optional[str] = None
