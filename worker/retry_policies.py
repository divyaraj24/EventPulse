import os
import random
import time
from collections import defaultdict
from typing import Protocol, Tuple

NAIVE_MAX_ATTEMPTS = int(os.getenv("NAIVE_MAX_ATTEMPTS", "5"))
NAIVE_BASE_DELAY = float(os.getenv("NAIVE_BASE_DELAY", "0.5"))
NAIVE_MAX_DELAY = float(os.getenv("NAIVE_MAX_DELAY", "10.0"))
NAIVE_JITTER = float(os.getenv("NAIVE_JITTER", "0.5"))

# Adaptive gate timing follows RetryGuard (Algorithm 1, Section IV); retry
# backoff once the gate is open reuses naive's shape since the paper doesn't
# specify it.
ADAPTIVE_MAX_ATTEMPTS = int(os.getenv("ADAPTIVE_MAX_ATTEMPTS", "5"))
ADAPTIVE_BASE_DELAY = float(os.getenv("ADAPTIVE_BASE_DELAY", "0.5"))
ADAPTIVE_MAX_DELAY = float(os.getenv("ADAPTIVE_MAX_DELAY", "10.0"))
ADAPTIVE_JITTER = float(os.getenv("ADAPTIVE_JITTER", "0.5"))
ADAPTIVE_REJECTION_THRESHOLD = float(os.getenv("ADAPTIVE_REJECTION_THRESHOLD", "0.2"))
ADAPTIVE_INTERVAL_PERIODS = int(os.getenv("ADAPTIVE_INTERVAL_PERIODS", "3"))
ADAPTIVE_MEASUREMENT_WINDOW_SECONDS = float(os.getenv("ADAPTIVE_MEASUREMENT_WINDOW_SECONDS", "10.0"))


class RetryPolicy(Protocol):
    def should_retry(self, endpoint_id: str, attempt: int) -> Tuple[bool, float]:
        ...

    def record_attempt(self, endpoint_id: str, success: bool) -> None:
        ...


class NoRetryPolicy:
    """Baseline: one failed attempt goes straight to the dead letter queue."""

    def should_retry(self, endpoint_id: str, attempt: int) -> Tuple[bool, float]:
        return False, 0.0

    def record_attempt(self, endpoint_id: str, success: bool) -> None:
        pass


class NaiveBackoffPolicy:
    """Bounded exponential backoff with jitter, stateless per message."""

    def should_retry(self, endpoint_id: str, attempt: int) -> Tuple[bool, float]:
        if attempt >= NAIVE_MAX_ATTEMPTS:
            return False, 0.0
        delay = min(
            NAIVE_BASE_DELAY * (2 ** attempt) + random.uniform(0, NAIVE_JITTER),
            NAIVE_MAX_DELAY,
        )
        return True, delay

    def record_attempt(self, endpoint_id: str, success: bool) -> None:
        pass


class _EndpointState:
    __slots__ = ("retries_on", "low_streak", "high_streak", "window_start", "window_total", "window_failures")

    def __init__(self):
        self.retries_on = True
        self.low_streak = 0
        self.high_streak = 0
        self.window_start = time.monotonic()
        self.window_total = 0
        self.window_failures = 0


class AdaptivePolicy:
    """
    RetryGuard's on/off retry gate: per endpoint, retries flip off after
    `interval_periods` consecutive measurement windows land above the
    failure-rate threshold, and back on after the same streak below it.
    """

    def __init__(self):
        self._states: dict[str, _EndpointState] = defaultdict(_EndpointState)

    def _roll_window_if_due(self, state: _EndpointState, now: float) -> None:
        if now - state.window_start < ADAPTIVE_MEASUREMENT_WINDOW_SECONDS:
            return

        # Only score a window that actually saw traffic.
        if state.window_total > 0:
            failure_rate = state.window_failures / state.window_total
            if failure_rate < ADAPTIVE_REJECTION_THRESHOLD:
                state.low_streak += 1
                state.high_streak = 0
            elif failure_rate > ADAPTIVE_REJECTION_THRESHOLD:
                state.high_streak += 1
                state.low_streak = 0
            else:
                state.low_streak = 0
                state.high_streak = 0

            if state.low_streak >= ADAPTIVE_INTERVAL_PERIODS:
                state.retries_on = True
            elif state.high_streak >= ADAPTIVE_INTERVAL_PERIODS:
                state.retries_on = False

        state.window_start = now
        state.window_total = 0
        state.window_failures = 0

    def record_attempt(self, endpoint_id: str, success: bool) -> None:
        state = self._states[endpoint_id]
        self._roll_window_if_due(state, time.monotonic())
        state.window_total += 1
        if not success:
            state.window_failures += 1

    def should_retry(self, endpoint_id: str, attempt: int) -> Tuple[bool, float]:
        state = self._states[endpoint_id]
        self._roll_window_if_due(state, time.monotonic())

        if not state.retries_on:
            return False, 0.0

        if attempt >= ADAPTIVE_MAX_ATTEMPTS:
            return False, 0.0

        delay = min(
            ADAPTIVE_BASE_DELAY * (2 ** attempt) + random.uniform(0, ADAPTIVE_JITTER),
            ADAPTIVE_MAX_DELAY,
        )
        return True, delay


def get_policy(name: str) -> RetryPolicy:
    policies = {
        "none": NoRetryPolicy(),
        "naive": NaiveBackoffPolicy(),
        "adaptive": AdaptivePolicy(),
    }
    if name not in policies:
        raise ValueError(f"Unknown RETRY_POLICY '{name}', expected one of {list(policies)}")
    return policies[name]
