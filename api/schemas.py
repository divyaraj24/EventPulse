import uuid
from typing import Any, Dict

from pydantic import BaseModel


class EventCreate(BaseModel):
    event_type: str
    endpoint_id: str
    endpoint_url: str
    payload: Dict[str, Any]


class EventOut(BaseModel):
    id: uuid.UUID
    event_type: str
    endpoint_id: str

    class Config:
        from_attributes = True