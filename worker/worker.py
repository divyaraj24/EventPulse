import csv
import hashlib
import hmac
import json
import os
import signal
import sys
from datetime import datetime, timezone

import httpx
import redis.asyncio as redis
from redis.exceptions import ResponseError
import asyncio

from database import Base, SessionLocal, engine
from models import DeadLetter
from retry_policies import get_policy

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
STREAM_NAME = os.getenv("STREAM_NAME", "deliveries")
GROUP_NAME = os.getenv("GROUP_NAME", "workers")
CONSUMER_NAME = os.getenv("CONSUMER_NAME", f"worker-{os.getpid()}")
SIGNING_SECRET = os.getenv("SIGNING_SECRET", "dev-secret-change-me").encode()
BLOCK_MS = int(os.getenv("BLOCK_MS", "2000"))
READ_COUNT = int(os.getenv("READ_COUNT", "50"))
HTTP_TIMEOUT_SECONDS = float(os.getenv("HTTP_TIMEOUT_SECONDS", "5.0"))
DELIVERY_LOG_PATH = os.getenv("DELIVERY_LOG_PATH", "/app/delivery_log.csv")

# Max in-flight deliveries -- what makes the receiver's concurrency ceiling
# and naive retry's pile-up behavior actually mean something.
WORKER_CONCURRENCY = int(os.getenv("WORKER_CONCURRENCY", "20"))

# Live event feed for the frontend's status panel -- a capped Redis list,
# not a real log pipeline. Cosmetic only: never let a publish failure
# affect delivery itself.
EVENTS_KEY = os.getenv("EVENTS_KEY", "harness:events")
EVENTS_MAX = int(os.getenv("EVENTS_MAX", "200"))


async def publish_event(redis_client: redis.Redis, text: str) -> None:
    try:
        await redis_client.lpush(EVENTS_KEY, text)
        await redis_client.ltrim(EVENTS_KEY, 0, EVENTS_MAX - 1)
    except Exception:
        pass

RETRY_POLICY_NAME = os.getenv("RETRY_POLICY", "none")
retry_policy = get_policy(RETRY_POLICY_NAME)

Base.metadata.create_all(bind=engine)

# Buffered in memory, flushed once at shutdown -- see handle_shutdown().
_log_rows = []


def log_delivery(fields: dict, attempt: int, outcome: str, latency_ms: float, error: str = ""):
    _log_rows.append([
        datetime.now(timezone.utc).isoformat(),
        fields["event_id"],
        fields["endpoint_id"],
        attempt,
        outcome,
        f"{latency_ms:.2f}",
        error,
    ])


def flush_delivery_log():
    with open(DELIVERY_LOG_PATH, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["timestamp", "event_id", "endpoint_id", "attempt", "outcome", "latency_ms", "error"])
        writer.writerows(_log_rows)
    print(f"[worker] wrote {len(_log_rows)} delivery log rows to {DELIVERY_LOG_PATH}")


def handle_shutdown(signum, frame):
    print(f"[worker] received shutdown signal ({signum}), flushing delivery log")
    flush_delivery_log()
    sys.exit(0)


signal.signal(signal.SIGTERM, handle_shutdown)
signal.signal(signal.SIGINT, handle_shutdown)


def sign_payload(payload_bytes: bytes) -> str:
    return hmac.new(SIGNING_SECRET, payload_bytes, hashlib.sha256).hexdigest()


def send_to_dlq(fields: dict, attempts: int, last_error: str):
    db = SessionLocal()
    try:
        db.add(DeadLetter(
            event_id=fields["event_id"],
            endpoint_id=fields["endpoint_id"],
            endpoint_url=fields["endpoint_url"],
            payload=json.loads(fields["payload"]),
            attempts=attempts,
            last_error=last_error,
        ))
        db.commit()
        print(f"[worker] DLQ: event {fields['event_id']} after {attempts} attempt(s) -- {last_error}")
    finally:
        db.close()


async def attempt_delivery(client: httpx.AsyncClient, fields: dict) -> tuple[bool, str, float]:
    endpoint_url = fields["endpoint_url"]
    payload_bytes = fields["payload"].encode()
    signature = sign_payload(payload_bytes)

    started = asyncio.get_event_loop().time()
    try:
        response = await client.post(
            endpoint_url,
            content=payload_bytes,
            headers={
                "Content-Type": "application/json",
                "X-EventPulse-Signature": signature,
                "X-EventPulse-Event-Id": fields["event_id"],
            },
        )
        latency_ms = (asyncio.get_event_loop().time() - started) * 1000
        if 200 <= response.status_code < 300:
            return True, "", latency_ms
        return False, f"HTTP {response.status_code}", latency_ms
    except httpx.RequestError as e:
        latency_ms = (asyncio.get_event_loop().time() - started) * 1000
        return False, f"request error: {e}", latency_ms


async def process_message(
    client: httpx.AsyncClient,
    redis_client: redis.Redis,
    semaphore: asyncio.Semaphore,
    msg_id: str,
    fields: dict,
):
    async with semaphore:
        attempt = 1
        try:
            while True:
                success, error, latency_ms = await attempt_delivery(client, fields)
                retry_policy.record_attempt(fields["endpoint_id"], success, latency_ms)

                short_id = fields["event_id"][:8]

                if success:
                    print(f"[worker] delivered event {fields['event_id']} (attempt {attempt})")
                    log_delivery(fields, attempt, "delivered", latency_ms)
                    await publish_event(redis_client, f"delivered {short_id} (attempt {attempt}, {latency_ms:.0f}ms)")
                    return

                should_retry, delay = retry_policy.should_retry(fields["endpoint_id"], attempt)

                if not should_retry:
                    send_to_dlq(fields, attempts=attempt, last_error=error)
                    log_delivery(fields, attempt, "dlq", latency_ms, error=error)
                    await publish_event(redis_client, f"dead-lettered {short_id} after {attempt} attempt(s)")
                    return

                log_delivery(fields, attempt, "retry", latency_ms, error=error)
                await publish_event(redis_client, f"retrying {short_id} (attempt {attempt} failed: {error})")

                # Release the slot before backing off and reacquire before the
                # next attempt -- holding it through the sleep would pin a
                # WORKER_CONCURRENCY slot idle exactly when a fault-period
                # backlog most needs draining.
                semaphore.release()
                await asyncio.sleep(delay)
                await semaphore.acquire()

                attempt += 1

        except Exception as e:
            print(f"[worker] UNEXPECTED ERROR on event {fields.get('event_id')}: {e}")
            try:
                send_to_dlq(fields, attempts=attempt, last_error=f"unexpected: {e}")
            except Exception as dlq_error:
                print(f"[worker] ERROR: DLQ write itself failed: {dlq_error}")
            log_delivery(fields, attempt, "error", 0.0, error=str(e))

        finally:
            # Always ack, even on the exception path -- otherwise the message
            # sits in Redis's pending list forever and any drain-wait loop
            # hangs indefinitely.
            await redis_client.xack(STREAM_NAME, GROUP_NAME, msg_id)


async def ensure_consumer_group(redis_client: redis.Redis):
    try:
        await redis_client.xgroup_create(STREAM_NAME, GROUP_NAME, id="0", mkstream=True)
        print(f"[worker] created consumer group '{GROUP_NAME}' on stream '{STREAM_NAME}'")
    except ResponseError as e:
        if "BUSYGROUP" in str(e):
            pass
        else:
            raise


async def main():
    redis_client = redis.from_url(REDIS_URL, decode_responses=True)
    semaphore = asyncio.Semaphore(WORKER_CONCURRENCY)

    if hasattr(retry_policy, "on_gate_change"):
        def _on_gate_change(endpoint_id: str, retries_on: bool) -> None:
            state = "opened" if retries_on else "closed"
            asyncio.create_task(publish_event(redis_client, f"[adaptive] gate {state} for {endpoint_id}"))
        retry_policy.on_gate_change = _on_gate_change  # type: ignore[attr-defined]

    await ensure_consumer_group(redis_client)
    print(
        f"[worker] starting -- policy='{RETRY_POLICY_NAME}', "
        f"concurrency={WORKER_CONCURRENCY}, consumer='{CONSUMER_NAME}'"
    )

    # Without explicit limits, httpx defaults to max_connections=100,
    # max_keepalive_connections=20 -- comfortably above the old fixed
    # WORKER_CONCURRENCY=20 default, but silently becomes an invisible
    # second throttle now that worker_concurrency can be set much higher
    # (default 1000, deliberately non-binding). Excess requests would queue
    # inside httpx's own pool instead of the semaphore, invisible to our
    # concurrency accounting, with constant connection churn on top since
    # keepalive slots run out well before max_connections does. Same class
    # of bug load_generator.py already hit and fixed for its own client.
    limits = httpx.Limits(max_connections=WORKER_CONCURRENCY, max_keepalive_connections=min(WORKER_CONCURRENCY, 100))
    async with httpx.AsyncClient(timeout=HTTP_TIMEOUT_SECONDS, limits=limits) as http_client:
        while True:
            try:
                response = await redis_client.xreadgroup(
                    GROUP_NAME, CONSUMER_NAME,
                    {STREAM_NAME: ">"},
                    count=READ_COUNT,
                    block=BLOCK_MS,
                )
                if not response:
                    continue

                # process_message's own semaphore bounds actual in-flight
                # count, not this gather().
                tasks = []
                for _stream_name, messages in response:
                    for msg_id, fields in messages:
                        tasks.append(asyncio.create_task(
                            process_message(http_client, redis_client, semaphore, msg_id, fields)
                        ))

                if tasks:
                    await asyncio.gather(*tasks)

            except Exception as e:
                print(f"[worker] ERROR in main loop: {e}")
                await asyncio.sleep(1)


if __name__ == "__main__":
    asyncio.run(main())
