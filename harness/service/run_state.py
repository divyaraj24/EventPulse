import asyncio
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Optional


class RunStatus(str, Enum):
    STARTING = "starting"
    RUNNING = "running"
    DRAINING = "draining"
    EXTRACTING = "extracting"
    DONE = "done"
    FAILED = "failed"
    CANCELLED = "cancelled"


@dataclass
class TestRun:
    run_id: str
    status: RunStatus
    label: str
    rate: float
    duration: float
    policy: str
    endpoint_id: str
    started_at: datetime
    progress: dict = field(default_factory=lambda: {"events_sent": 0, "events_total": 0})
    chaos_phase: Optional[str] = None
    error: Optional[str] = None
    result_dir: Optional[Path] = None
    # Not part of the run's public state (status endpoints don't serialize
    # this) -- lets /test/cancel find the in-flight task to cancel.
    task: Optional[asyncio.Task] = None


class RunRegistry:
    """Single-flight run tracking: one active run at a time, rejected (not
    queued) if another is already in progress. A small history buffer lets
    /test/status/{run_id} and /test/result/{run_id} answer for recent runs
    after the current one finishes."""

    def __init__(self, history_limit: int = 10):
        self._current: Optional[TestRun] = None
        self._history: dict[str, TestRun] = {}
        self._history_limit = history_limit
        self._busy = False

    @property
    def current(self) -> Optional[TestRun]:
        return self._current

    def try_start(self, run: TestRun) -> bool:
        # No `await` between the check and the set, so this is atomic under
        # asyncio's single-threaded cooperative scheduling -- no lock needed.
        if self._busy:
            return False
        self._busy = True
        self._current = run
        self._remember(run)
        return True

    def finish(self) -> None:
        self._busy = False

    def _remember(self, run: TestRun) -> None:
        self._history[run.run_id] = run
        if len(self._history) > self._history_limit:
            del self._history[next(iter(self._history))]

    def get(self, run_id: str) -> Optional[TestRun]:
        if self._current and self._current.run_id == run_id:
            return self._current
        return self._history.get(run_id)

    def list_recent(self) -> list[TestRun]:
        # dict insertion order is chronological (oldest first); reverse for
        # newest-first, which is what a history view wants.
        return list(reversed(self._history.values()))
