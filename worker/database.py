"""
worker/database.py -- belongs to the WORKER service only.

Pool size is tied to WORKER_CONCURRENCY (default 20, same env var
worker.py reads) rather than a fixed number: send_to_dlq() can be called
from up to WORKER_CONCURRENCY deliveries failing at once (exactly what
happens during a chaos episode under NoRetryPolicy), so the pool needs to
cover that worst case, not just typical usage.

NOT shared with api/database.py or relay/database.py -- see those files'
headers for why their pool sizes differ from this one.
"""
import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, declarative_base

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://postgres:postgres@localhost:5432/eventpulse",
)
WORKER_CONCURRENCY = int(os.getenv("WORKER_CONCURRENCY", "20"))  # same env var worker.py reads

engine = create_engine(
    DATABASE_URL,
    pool_pre_ping=True,
    pool_size=WORKER_CONCURRENCY,  # matches the worker's own concurrency ceiling --
    max_overflow=10,               # if 20 deliveries can fail simultaneously during
                                    # chaos, all 20 might hit send_to_dlq() at once
)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()