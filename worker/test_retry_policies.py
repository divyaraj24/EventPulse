"""
Unit tests for retry_policies.py. Pure logic, no Docker/DB/Redis needed --
run with `pytest` from the worker/ directory.
"""
import retry_policies as rp


def test_no_retry_never_retries():
    policy = rp.NoRetryPolicy()
    should_retry, delay = policy.should_retry("endpoint", attempt=1)
    assert should_retry is False
    assert delay == 0.0


def test_naive_retries_until_max_attempts():
    policy = rp.NaiveBackoffPolicy()
    for attempt in range(1, rp.NAIVE_MAX_ATTEMPTS):
        should_retry, delay = policy.should_retry("endpoint", attempt)
        assert should_retry is True
        assert delay > 0

    should_retry, _ = policy.should_retry("endpoint", rp.NAIVE_MAX_ATTEMPTS)
    assert should_retry is False


def test_naive_delay_is_capped():
    policy = rp.NaiveBackoffPolicy()
    for attempt in range(1, rp.NAIVE_MAX_ATTEMPTS):
        _, delay = policy.should_retry("endpoint", attempt)
        assert delay <= rp.NAIVE_MAX_DELAY


def test_naive_is_stateless_across_endpoints():
    policy = rp.NaiveBackoffPolicy()
    should_retry_a, _ = policy.should_retry("endpoint-a", attempt=1)
    should_retry_b, _ = policy.should_retry("endpoint-b", attempt=1)
    assert should_retry_a is should_retry_b is True


class FakeClock:
    """Lets tests advance time.monotonic() without real sleeping."""

    def __init__(self, start=0.0):
        self.now = start

    def advance(self, seconds):
        self.now += seconds

    def __call__(self):
        return self.now


def test_adaptive_starts_with_retries_enabled():
    policy = rp.AdaptivePolicy()
    should_retry, _ = policy.should_retry("endpoint", attempt=1)
    assert should_retry is True


def test_adaptive_gate_closes_after_sustained_failures(monkeypatch):
    clock = FakeClock()
    monkeypatch.setattr(rp.time, "monotonic", clock)
    policy = rp.AdaptivePolicy()

    # Record enough failing attempts in each of 3 consecutive windows to
    # push the failure rate above ADAPTIVE_REJECTION_THRESHOLD, advancing
    # the clock past ADAPTIVE_MEASUREMENT_WINDOW_SECONDS between windows.
    for _ in range(rp.ADAPTIVE_INTERVAL_PERIODS):
        for _ in range(5):
            policy.record_attempt("endpoint", success=False)
        clock.advance(rp.ADAPTIVE_MEASUREMENT_WINDOW_SECONDS + 0.1)

    should_retry, _ = policy.should_retry("endpoint", attempt=1)
    assert should_retry is False


def test_adaptive_gate_reopens_after_sustained_recovery(monkeypatch):
    clock = FakeClock()
    monkeypatch.setattr(rp.time, "monotonic", clock)
    policy = rp.AdaptivePolicy()

    # Close the gate first.
    for _ in range(rp.ADAPTIVE_INTERVAL_PERIODS):
        for _ in range(5):
            policy.record_attempt("endpoint", success=False)
        clock.advance(rp.ADAPTIVE_MEASUREMENT_WINDOW_SECONDS + 0.1)
    assert policy.should_retry("endpoint", attempt=1)[0] is False

    # Then recover for the same number of consecutive windows.
    for _ in range(rp.ADAPTIVE_INTERVAL_PERIODS):
        for _ in range(5):
            policy.record_attempt("endpoint", success=True)
        clock.advance(rp.ADAPTIVE_MEASUREMENT_WINDOW_SECONDS + 0.1)

    should_retry, _ = policy.should_retry("endpoint", attempt=1)
    assert should_retry is True


def test_adaptive_idle_window_does_not_accrue_streak(monkeypatch):
    clock = FakeClock()
    monkeypatch.setattr(rp.time, "monotonic", clock)
    policy = rp.AdaptivePolicy()

    # Advance through several empty windows (no traffic at all) -- an idle
    # endpoint shouldn't accrue a streak in either direction.
    for _ in range(rp.ADAPTIVE_INTERVAL_PERIODS + 2):
        policy.should_retry("endpoint", attempt=1)
        clock.advance(rp.ADAPTIVE_MEASUREMENT_WINDOW_SECONDS + 0.1)

    should_retry, _ = policy.should_retry("endpoint", attempt=1)
    assert should_retry is True


def test_adaptive_tracks_endpoints_independently(monkeypatch):
    clock = FakeClock()
    monkeypatch.setattr(rp.time, "monotonic", clock)
    policy = rp.AdaptivePolicy()

    for _ in range(rp.ADAPTIVE_INTERVAL_PERIODS):
        for _ in range(5):
            policy.record_attempt("bad-endpoint", success=False)
        clock.advance(rp.ADAPTIVE_MEASUREMENT_WINDOW_SECONDS + 0.1)

    assert policy.should_retry("bad-endpoint", attempt=1)[0] is False
    assert policy.should_retry("healthy-endpoint", attempt=1)[0] is True


def test_adaptive_respects_max_attempts():
    policy = rp.AdaptivePolicy()
    should_retry, _ = policy.should_retry("endpoint", attempt=rp.ADAPTIVE_MAX_ATTEMPTS)
    assert should_retry is False


def test_get_policy_returns_correct_types():
    assert isinstance(rp.get_policy("none"), rp.NoRetryPolicy)
    assert isinstance(rp.get_policy("naive"), rp.NaiveBackoffPolicy)
    assert isinstance(rp.get_policy("adaptive"), rp.AdaptivePolicy)


def test_get_policy_rejects_unknown_name():
    try:
        rp.get_policy("nonexistent")
        assert False, "expected ValueError"
    except ValueError:
        pass
