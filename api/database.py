import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, declarative_base

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://postgres:postgres@localhost:5432/eventpulse",
)

# Default pool (5+10) became a bottleneck at 150+ events/sec; not shared
# with relay/worker since each service has its own usage pattern.
engine = create_engine(
    DATABASE_URL,
    pool_pre_ping=True,
    pool_size=30,
    max_overflow=20,
)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
