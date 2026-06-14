from fastapi import FastAPI, Depends
from sqlalchemy.orm import Session

from database import Base, engine, get_db
import models
import schemas

# swap to alembic migrations once the schema stops changing
Base.metadata.create_all(bind=engine)

app = FastAPI(title="EventPulse Ingest API")


@app.post("/events", response_model=schemas.EventOut, status_code=202)
def create_event(event: schemas.EventCreate, db: Session = Depends(get_db)):
    db_event = models.Event(
        event_type=event.event_type,
        endpoint_id=event.endpoint_id,
        endpoint_url=event.endpoint_url,
        payload=event.payload,
    )
    db.add(db_event)
    db.flush()  

    outbox_entry = models.OutboxEntry(event_id=db_event.id)
    db.add(outbox_entry)

    db.commit()
    db.refresh(db_event)

    return db_event


@app.get("/health")
def health():
    return {"status": "ok"}