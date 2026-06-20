import json
import os
import time
from datetime import datetime, timezone

import redis

from database import SessionLocal
from models import Event, OutboxEntry

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
STREAM_NAME = os.getenv("STREAM_NAME", "deliveries")
POLL_INTERVAL_SECONDS = float(os.getenv("POLL_INTERVAL_SECONDS", "0.2"))
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "50"))

redis_client = redis.from_url(REDIS_URL, decode_responses=True)


def poll_and_relay():
    db = SessionLocal()
    try:
        pending = (
            db.query(OutboxEntry)
            .filter(OutboxEntry.published == False)
            .order_by(OutboxEntry.created_at.asc())
            .limit(BATCH_SIZE)
            .all()
        )

        if not pending:
            return

        for entry in pending:
            event = db.query(Event).filter(Event.id == entry.event_id).first()
            if event is None:
                print(f"[relay] WARNING: outbox {entry.id} has no matching event, skipping")
                continue

            redis_client.xadd(
                STREAM_NAME,
                {
                    "event_id": str(event.id),
                    "event_type": str(event.event_type),
                    "endpoint_id": str(event.endpoint_id),
                    "endpoint_url": str(event.endpoint_url),
                    "payload": json.dumps(event.payload),
                },
            )

            entry.published = True
            entry.published_at = datetime.now(timezone.utc)
            print(f"[relay] published event {event.id} -> stream '{STREAM_NAME}'")

        db.commit()

    finally:
        db.close()


def main():
    print(f"[relay] starting -- polling every {POLL_INTERVAL_SECONDS}s, batch size {BATCH_SIZE}")
    while True:
        try:
            poll_and_relay()
        except Exception as e:
            print(f"[relay] ERROR during poll cycle: {e}")
        time.sleep(POLL_INTERVAL_SECONDS)


if __name__ == "__main__":
    main()