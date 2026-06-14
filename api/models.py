import uuid
from datetime import datetime, timezone

from sqlalchemy import Column, String, DateTime, JSON, Boolean
from sqlalchemy.dialects.postgresql import UUID

from database import Base


def now_utc():
    return datetime.now(timezone.utc)


class Event(Base):
    __tablename__ = "events"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    event_type = Column(String, nullable=False)
    endpoint_id = Column(String, nullable=False, index=True)
    endpoint_url = Column(String, nullable=False)
    payload = Column(JSON, nullable=False)
    created_at = Column(DateTime(timezone=True), default=now_utc)


class OutboxEntry(Base):
    __tablename__ = "outbox"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    event_id = Column(UUID(as_uuid=True), nullable=False, index=True)
    published = Column(Boolean, default=False, index=True)
    created_at = Column(DateTime(timezone=True), default=now_utc)
    published_at = Column(DateTime(timezone=True), nullable=True)