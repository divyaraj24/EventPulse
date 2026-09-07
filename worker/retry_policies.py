import os
import random
import time
from collections import defaultdict
from typing import Optional, Protocol, Tuple

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

# RetryGuard (Sec IV-D) names response delay as a valid surrogate signal
# alongside rejection rate, but neither specifies an algorithm nor
# evaluates it -- Algorithm 1 and both real deployments (AWS, Istio) use
# rejection rate only. AdaptivePolicy below extends the paper's gate to
# watch both signals together rather than choosing one: a receiver can
# degrade (slow responses) well before, or entirely without, producing
# outright rejections -- this project's own "concurrency non-binding"
# experiments produced exactly that case (pure latency, zero rejections),
# where a rejection-only gate has no signal to react to at all. Multiplier
# is over the endpoint's own learned healthy-state baseline, not an
# absolute ms value, since "normal" latency is endpoint-specific.
ADAPTIVE_LATENCY_MULTIPLIER = float(os.getenv("ADAPTIVE_LATENCY_MULTIPLIER", "3.0"))


class RetryPolicy(Protocol):
    def should_retry(self, endpoint_id: str, attempt: int) -> Tuple[bool, float]:
        ...

    def record_attempt(self, endpoint_id: str, success: bool, latency_ms: float = 0.0) -> None:
        ...


class NoRetryPolicy:
    """Baseline: one failed attempt goes straight to the dead letter queue."""

    def should_retry(self, endpoint_id: str, attempt: int) -> Tuple[bool, float]:
        return False, 0.0

    def record_attempt(self, endpoint_id: str, success: bool, latency_ms: float = 0.0) -> None:
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

    def record_attempt(self, endpoint_id: str, success: bool, latency_ms: float = 0.0) -> None:
        pass


class _EndpointState:
    __slots__ = (
        "retries_on", "low_streak", "high_streak", "window_start",
        "window_total", "window_failures", "window_latency_total",
        "baseline_latency_ms",
    )
    baseline_latency_ms: Optional[float]

    def __init__(self):
        self.retries_on = True
        self.low_streak = 0
        self.high_streak = 0
        self.window_start = time.monotonic()
        self.window_total = 0
        self.window_failures = 0
        self.window_latency_total = 0.0
        self.baseline_latency_ms = None


class AdaptivePolicy:
    """
    RetryGuard's on/off retry gate (Algorithm 1, Sec IV), extended to watch
    two signals instead of one. Per endpoint, a measurement window is
    scored "elevated" if EITHER its failure rate exceeds
    ADAPTIVE_REJECTION_THRESHOLD OR its average latency exceeds
    ADAPTIVE_LATENCY_MULTIPLIER times the endpoint's learned healthy-state
    latency baseline. Retries flip off after `interval_periods` consecutive
    elevated windows, and back on after the same streak of non-elevated
    windows.

    This is a deliberate extension beyond the paper: Algorithm 1 and both
    of RetryGuard's real deployments (AWS, Istio) use rejection rate alone.
    Sec IV-D names latency as a valid alternative signal but specifies no
    algorithm for it and runs no experiment with it -- combining both here
    closes a real gap rejection-only gating has, since a receiver can
    degrade (get slow) well before, or entirely without, producing outright
    rejections.

    The latency baseline is a slow EWMA (alpha=0.1), refreshed only during
    non-elevated windows while retries are on, so it can't drift upward to
    "normalize" a sustained fault while one is active. Rejection-rate
    scoring is available from the very first window (matching the
    original, single-signal gate exactly); the latency signal only starts
    contributing once a baseline exists, i.e. after the first non-elevated
    window establishes one -- there is nothing to compare against before
    that, so a purely rejection-driven scenario behaves identically to the
    paper's own single-signal gate.
    """

    def __init__(self):
        self._states: dict[str, _EndpointState] = defaultdict(_EndpointState)
        # Optional hook for observability only (e.g. the live event feed in
        # worker.py) -- fired with (endpoint_id, retries_on) whenever the
        # gate actually flips. Not required for the gate to function.
        self.on_gate_change = None

    def _roll_window_if_due(self, state: _EndpointState, now: float, endpoint_id: str = "") -> None:
        if now - state.window_start < ADAPTIVE_MEASUREMENT_WINDOW_SECONDS:
            return

        # Only score a window that actually saw traffic.
        if state.window_total > 0:
            failure_rate = state.window_failures / state.window_total
            avg_latency = state.window_latency_total / state.window_total

            rejection_elevated = failure_rate > ADAPTIVE_REJECTION_THRESHOLD
            latency_elevated = (
                state.baseline_latency_ms is not None
                and avg_latency > state.baseline_latency_ms * ADAPTIVE_LATENCY_MULTIPLIER
            )
            elevated = rejection_elevated or latency_elevated

            if elevated:
                state.high_streak += 1
                state.low_streak = 0
            else:
                state.low_streak += 1
                state.high_streak = 0
                if state.retries_on:
                    state.baseline_latency_ms = (
                        avg_latency if state.baseline_latency_ms is None
                        else 0.9 * state.baseline_latency_ms + 0.1 * avg_latency
                    )

            was_on = state.retries_on
            if state.low_streak >= ADAPTIVE_INTERVAL_PERIODS:
                state.retries_on = True
            elif state.high_streak >= ADAPTIVE_INTERVAL_PERIODS:
                state.retries_on = False
            if state.retries_on != was_on and self.on_gate_change:
                self.on_gate_change(endpoint_id, state.retries_on)

        state.window_start = now
        state.window_total = 0
        state.window_failures = 0
        state.window_latency_total = 0.0

    def record_attempt(self, endpoint_id: str, success: bool, latency_ms: float = 0.0) -> None:
        state = self._states[endpoint_id]
        self._roll_window_if_due(state, time.monotonic(), endpoint_id)
        state.window_total += 1
        state.window_latency_total += latency_ms
        if not success:
            state.window_failures += 1

    def should_retry(self, endpoint_id: str, attempt: int) -> Tuple[bool, float]:
        state = self._states[endpoint_id]
        self._roll_window_if_due(state, time.monotonic(), endpoint_id)

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
