# EventPulse

**Reliable webhook delivery system with failure analytics** — a durable, at-least-once delivery pipeline, deliberately subjected to controlled chaos to measure exactly when automatic retry stops helping and starts sustaining the outage it was meant to resolve.

Webhooks depend on third-party receivers that time out or fail intermittently. The usual remedy — automatic retry — is itself destabilizing: retry traffic amplifies load on an already-degraded receiver and can hold the delivery tier in a self-sustaining overloaded state long after the original fault has cleared, a pattern documented in the distributed-systems literature as a **metastable failure** (Bronson et al., HotOS '21). EventPulse builds a real delivery pipeline, then benchmarks a naive fixed-backoff retry policy against a metastability-aware adaptive policy under identical, reproducible fault injection.

## Architecture

![System architecture](docs/assets/architecture.png)

Five containerized services on an internal Docker network, plus two host-side scripts that drive experiments:

- **Ingest API** — `POST /events` writes the event and an outbox row in a single Postgres transaction (transactional outbox pattern), returns `202 Accepted`. This single-transaction write is what guarantees an accepted event is never silently lost, independent of anything downstream.
- **Relay** — polls the outbox for unpublished rows and publishes them to a Redis Stream, marking each row published on success.
- **Worker pool** — consumes the stream via a consumer group, signs each payload (HMAC-SHA256), delivers over HTTP under a bounded concurrency limit, and applies whichever retry policy is active.
- **Mock receiver** — a controllable stand-in for a third-party endpoint, with admin endpoints to set a concurrency ceiling, random rejection rate, and injected latency at runtime.
- **Postgres + Redis** — durable storage (events/outbox/dead-letters) and the delivery work queue.
- **Load generator + chaos harness** (host-side) — generate paced offered load and drive the receiver through a steady/fault/recovery timeline.

```
Ingest API → Transactional Outbox → Relay → Redis Stream → Worker Pool (Retry Policy) → Signed HTTP Delivery → Receiver
```

Every service except the worker is retry-policy-agnostic — switching between `none` / `naive` / `adaptive` changes only the worker's configuration, so the fault applies identically across all three conditions.

<details>
<summary>Additional diagrams (DFD, use case, class, sequence)</summary>

| | |
|---|---|
| ![DFD Level 0](docs/assets/dfd_level0.png) | ![DFD Level 1](docs/assets/dfd_level1.png) |
| ![Use case diagram](docs/assets/usecase.png) | ![Class diagram](docs/assets/class_diagram.png) |
| ![Sequence diagram](docs/assets/sequence_diagram.png) | |

</details>

## Retry policies

All three implement the same two-method interface (`worker/retry_policies.py`), so the worker calls them identically regardless of which is active:

| Policy | Behavior |
|---|---|
| `none` | One failed attempt → straight to dead-letter. The experimental baseline: isolates whether *retrying itself* is what amplifies load, independent of any backoff strategy. |
| `naive` | Bounded exponential backoff with jitter, capped at 5 attempts. Stateless — delay depends only on how many times *this* message has been attempted, with no memory of how the endpoint is behaving overall. |
| `adaptive` | A per-endpoint failure-rate gate based on [RetryGuard](docs/references/09_tavori_retryguard_arXiv2511.23278.pdf) (Tavori et al., 2025): retries are disabled once the endpoint's failure rate stays above 20% for 3 consecutive 10s measurement windows, and re-enabled after the same streak below it. When the gate is open, it reuses naive's backoff timing — the gate is the only thing that differs between the two conditions. |

## Results

**Headline finding, from the project report** (`docs/EventPulse_Review2_Draft.docx`) — identical offered load (15 events/s, 120s), identical fault (40s at concurrency-ceiling 1 + 300ms latency), only the retry policy differs:

| Condition | Recovery time after fault clears | Unresolved events (of 1800) |
|---|---|---|
| `none` | instant (never builds a backlog) | 0 — but discards 26% outright |
| `naive` | **does not recover** within the observed window | 684 (38%) still circulating when the run ends |
| `adaptive` | **6.0s** | 0 |

Extending the naive run to 300s of continuous load doesn't reveal a slow recovery — goodput holds flat at ~3 events/s (against a 15 events/s baseline) with 67% of events unresolved at cutoff. It's a *chronic degraded equilibrium*, not a slow drain. Notably, per-request latency stays low (0–50ms) throughout the collapse — the receiver is being overwhelmed by retry *volume*, not slow processing, so latency alone would never surface this failure mode.

**Reproducible in this repository** — a harsher variant (`scripts/results/*_hardfault_*`, committed in-repo), 90s fault with deliberately throttled recovery capacity, one run per policy:

![Combined comparison chart](scripts/results/charts/combined_hardfault.png)

| Condition | Delivered | Dead-lettered | Retries fired | Still unresolved (of 2700) |
|---|---|---|---|---|
| `none` | 1224 (45%) | 1476 (55%) | 0 | 0 |
| `naive` | 815 (30%) | 423 (16%) | 3231 | **1420** |
| `adaptive` | 1134 (42%) | 1566 (58%) | 366 | 0 |

Naive is the only policy that fails to even finish processing the offered load — its retry storm piles messages up faster than the throttled receiver can drain them. Both `none` and `adaptive` fully resolve every event; adaptive does so while still retrying productively where it safely can.

### Limitations (from the project report)

- Reported results are single runs per condition — run-to-run variance isn't yet quantified.
- One receiver endpoint only; adaptive's per-endpoint state hasn't been exercised across endpoints of differing health.
- Fault shapes tested so far are capacity-ceiling + latency; high rejection-rate and partial-outage shapes aren't yet covered.
- "Does not recover" is bounded by the longest window tested (~360s post-fault) — a statement about that window's flat trend, not proof of indefinite non-recovery.

## Running it

```bash
docker compose up --build -d --wait   # bring the full pipeline up
docker compose down -v                # tear down
```

Run a full experiment (rebuild → load + fault injection in parallel → wait for drain → extract logs → chart):

```bash
cd scripts
./run_experiment.sh naive_test --policy naive -- --max-concurrency 1 --reject-rate 0.3
```

See `scripts/run_experiment.sh`'s header comment for the full flag set, including the newer surge-driven fault mode (`--surge-rate`, `--poisson`, `--repeats`) that triggers overload via a genuine offered-rate surge against the receiver's fixed real capacity, rather than an admin endpoint switching on synthetic errors.

## Tech stack

Python 3.12 · FastAPI · SQLAlchemy 2.0 · PostgreSQL 18 · Redis 7 (Streams) · httpx (async) · Docker Compose

## References

Full literature review and reference list in `docs/EventPulse_Review2_Draft.docx`; source PDFs for the three most directly used papers are in `docs/references/`. The adaptive policy is a direct, simplified port of RetryGuard's productive-retry controller (Tavori, Bremler-Barr, Levy & Lavi, 2025).
