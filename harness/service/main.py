import asyncio
import os
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import httpx
import redis.asyncio as aioredis
from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

import chaos as chaos_module
import docker_control
import load_generator
from models import (
    ChaosConfig,
    MessageResponse,
    RunHistoryEntry,
    TestStartRequest,
    TestStartResponse,
    TestStatusResponse,
)
from run_state import RunRegistry, RunStatus, TestRun

RESULTS_DIR = Path(os.getenv("HARNESS_RESULTS_DIR", "/app/results"))
RESULTS_DIR.mkdir(parents=True, exist_ok=True)

# NOT Path(__file__)-relative -- this file runs from /app/main.py inside
# the container (COPY'd there at build time by the Dockerfile), not from
# the bind-mounted repo path. analyze.py isn't COPY'd into the image at
# all; it's only reachable via the bind mount, so PROJECT_DIR (which
# docker_control.py already uses for the same reason) is the only
# reliable way to find it.
PROJECT_DIR = Path(os.getenv("PROJECT_DIR", "/workspace"))
ANALYZE_PY = PROJECT_DIR / "harness" / "analyze.py"

# Lives at the repo root (product-facing UI, not harness-internal), and --
# same as analyze.py -- is only reachable via the bind mount, not baked
# into the image. Editing it also doesn't require an image rebuild.
FRONTEND_DIR = PROJECT_DIR / "frontend"
REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379/0")
API_URL = os.getenv("API_URL", "http://api:8000")
RECEIVER_URL = os.getenv("RECEIVER_URL", "http://receiver_mock:9000")
DRAIN_STALL_SECONDS = float(os.getenv("DRAIN_STALL_SECONDS", "30"))

# Matches worker.py's own EVENTS_KEY/EVENTS_MAX -- both processes share
# the same Redis instance already (this service already talks to it for
# wait_for_drain), so this is just another key on it, not new infra.
EVENTS_KEY = os.getenv("EVENTS_KEY", "harness:events")

app = FastAPI(title="EventPulse Harness Service")
registry = RunRegistry()


async def wait_for_drain(stall_seconds: float) -> None:
    """Waits for the delivery backlog (consumer-group lag + pending) to
    reach zero. No absolute time cap -- a fixed timeout has to be guessed
    per experiment size and silently truncates runs that are still
    genuinely progressing (a severe retry storm can legitimately take far
    longer than a "normal" run to resolve). Instead, watches whether the
    backlog is actually shrinking and only gives up once it hasn't
    improved at all for stall_seconds -- that's the real signal something
    is stuck (a crashed worker, a lost connection), not just "this is a
    big run."""
    r = aioredis.from_url(REDIS_URL, decode_responses=True)
    best_remaining = None
    best_seen_at = asyncio.get_event_loop().time()
    try:
        while True:
            groups = await r.xinfo_groups("deliveries")
            g = groups[0]
            remaining = (g.get("lag") or 0) + (g.get("pending") or 0)
            if remaining <= 0:
                return
            now = asyncio.get_event_loop().time()
            if best_remaining is None or remaining < best_remaining:
                best_remaining = remaining
                best_seen_at = now
            elif now - best_seen_at >= stall_seconds:
                print(f"[harness] drain stalled at {remaining} remaining "
                      f"(no improvement for {stall_seconds:.0f}s) -- proceeding anyway")
                return
            await asyncio.sleep(1)
    finally:
        await r.aclose()


async def run_analyze(
    label: str, delivery_csv: Path, chart_png: Path, timeline_json: Optional[Path] = None,
) -> None:
    cmd = [
        "python3", str(ANALYZE_PY),
        "--file", f"{label}:{delivery_csv}",
        "--output", str(chart_png),
    ]
    if timeline_json is not None:
        cmd += ["--timeline", f"{label}:{timeline_json}"]
    proc = await asyncio.create_subprocess_exec(
        *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT,
    )
    out, _ = await proc.communicate()
    if proc.returncode != 0:
        raise RuntimeError(f"analyze.py failed:\n{out.decode()}")


async def live_chart_updater(
    run: TestRun, delivery_csv: Path, chart_png: Path,
    timeline_json: Path, chaos_enabled: bool, interval_seconds: float = 4.0,
) -> None:
    """Periodically snapshots the worker's (now continuously-flushed)
    delivery log and regenerates chart.png in place while a run is still
    RUNNING, so /test/result/{run_id}/chart.png -- the same endpoint the
    frontend already polls at the end -- serves a live-updating chart
    instead of nothing until the run finishes. Deliberately reuses
    analyze.py and the existing chart endpoint rather than a client-side
    charting library or a new streaming pipeline: same reasoning as the
    events feed, smaller surface area than it sounds. Cancelled by the
    caller once load generation finishes, not self-terminating, since
    checking run.status here would race the caller's own transition to
    DRAINING immediately after this task is awaited alongside it.
    """
    while True:
        try:
            await docker_control.extract_delivery_log(str(delivery_csv))
            # chaos.py only writes timeline_json once its full steady/fault/
            # recovery sequence completes -- during the live window it
            # doesn't exist yet, so fault-window shading is skipped until
            # the final post-drain analyze.py call (which always has it).
            await run_analyze(
                run.label, delivery_csv, chart_png,
                timeline_json=timeline_json if (chaos_enabled and timeline_json.exists()) else None,
            )
            run.result_dir = delivery_csv.parent
        except Exception as e:
            print(f"[harness] live_chart_updater snapshot failed: {e}")
        await asyncio.sleep(interval_seconds)


async def publish_event(text: str) -> None:
    """Live event feed for the frontend's status panel -- a capped Redis
    list shared with worker.py's own publish_event, not a real log
    pipeline. Cosmetic only: never let a publish failure affect the run."""
    try:
        r = aioredis.from_url(REDIS_URL, decode_responses=True)
        try:
            await r.lpush(EVENTS_KEY, text)
            await r.ltrim(EVENTS_KEY, 0, 199)
        finally:
            await r.aclose()
    except Exception:
        pass


async def execute_run(run: TestRun, chaos_config: ChaosConfig) -> None:
    run_dir = RESULTS_DIR / run.run_id
    run_dir.mkdir(parents=True, exist_ok=True)
    ingest_csv = run_dir / "ingest.csv"
    delivery_csv = run_dir / "delivery.csv"
    timeline_json = run_dir / "timeline.json"
    chart_png = run_dir / "chart.png"

    # Declared before the try so a cancellation during asyncio.gather (which
    # jumps straight past the explicit live_chart_task.cancel() call further
    # down) still has a task here to clean up in the except block below.
    live_chart_task: Optional[asyncio.Task] = None

    try:
        run.status = RunStatus.STARTING
        # receiver_mock is part of the persistent harness stack, not the
        # core stack torn down per run -- its /admin/chaos state otherwise
        # leaks forward from whatever the previous run last set. chaos.py's
        # own /admin/reset only runs when THIS run enables chaos, so a
        # no-chaos run right after a chaos-enabled one would silently
        # inherit a degraded receiver. Found via an empty "stability check"
        # run that came back with a 31.9% rejection rate.
        async with httpx.AsyncClient(timeout=5.0) as client:
            await chaos_module.call_admin(client, RECEIVER_URL, "/admin/reset")
        await docker_control.restart_core(run.policy, run.worker_concurrency)
        # Only safe to touch the core stack's Redis after restart_core
        # returns -- it's ephemeral (torn down/rebuilt every run), unlike
        # receiver_mock's admin reset above. Clearing here, not before,
        # avoids racing a Redis that doesn't exist yet.
        try:
            r = aioredis.from_url(REDIS_URL, decode_responses=True)
            try:
                await r.delete(EVENTS_KEY)
            finally:
                await r.aclose()
        except Exception:
            pass
        await publish_event(f"run {run.run_id} starting (policy={run.policy}, rate={run.rate}/s, duration={run.duration}s)")

        run.status = RunStatus.RUNNING

        def on_progress(sent: int, total: int) -> None:
            run.progress = {"events_sent": sent, "events_total": total}

        def on_phase_change(phase: str) -> None:
            run.chaos_phase = phase
            detail = {
                "steady": "steady baseline",
                "fault": (
                    f"fault injected: concurrency={chaos_config.max_concurrency}, "
                    f"latency={chaos_config.latency_ms}ms, reject_rate={chaos_config.reject_rate}"
                ),
                "recovery": "fault cleared, back to background capacity",
            }.get(phase, phase)
            asyncio.create_task(publish_event(f"[chaos] {detail}"))

        load_gen_task = load_generator.run(
            rate=run.rate,
            duration=run.duration,
            api_url=API_URL,
            endpoint_id=run.endpoint_id,
            endpoint_url=f"{RECEIVER_URL}/webhook",
            output_path=str(ingest_csv),
            on_progress=on_progress,
            poisson=run.poisson,
        )

        # Not inside the gather() below -- it runs an unconditional loop
        # (no natural end of its own), so it's launched and cancelled
        # separately rather than racing the transition to DRAINING that
        # happens right after load generation/chaos actually finish.
        live_chart_task = asyncio.create_task(
            live_chart_updater(run, delivery_csv, chart_png, timeline_json, chaos_config.enabled)
        )

        if chaos_config.enabled:
            chaos_task = chaos_module.run(
                receiver_url=RECEIVER_URL,
                steady=chaos_config.steady,
                fault=chaos_config.fault,
                recovery=chaos_config.recovery,
                max_concurrency=chaos_config.max_concurrency,
                reject_rate=chaos_config.reject_rate,
                latency_ms=chaos_config.latency_ms,
                timeline_output=str(timeline_json),
                on_phase_change=on_phase_change,
            )
            await asyncio.gather(load_gen_task, chaos_task)
        else:
            await load_gen_task

        live_chart_task.cancel()
        try:
            await live_chart_task
        except asyncio.CancelledError:
            pass

        run.status = RunStatus.DRAINING
        await wait_for_drain(DRAIN_STALL_SECONDS)

        run.status = RunStatus.EXTRACTING
        await docker_control.stop_worker()
        await docker_control.extract_delivery_log(str(delivery_csv))
        await docker_control.teardown_core()

        await run_analyze(
            run.label, delivery_csv, chart_png,
            timeline_json=timeline_json if chaos_config.enabled else None,
        )

        run.status = RunStatus.DONE
        run.result_dir = run_dir

    except asyncio.CancelledError:
        # Thrown into whichever `await` was in flight when /test/cancel
        # called task.cancel(). Caught (not re-raised) so the core stack
        # still gets torn down -- otherwise cancellation would skip
        # straight past this function's cleanup and leave containers
        # running, the same "reported done but the real outcome didn't
        # happen" shape as most of the other bugs in this project. A
        # cancellation landing inside the asyncio.gather() above also
        # jumps straight past the explicit live_chart_task.cancel() call,
        # so it's repeated here.
        if live_chart_task and not live_chart_task.done():
            live_chart_task.cancel()
        run.status = RunStatus.CANCELLED
        run.error = "Cancelled by user"
        try:
            await docker_control.teardown_core()
        except Exception:
            pass
    except Exception as e:
        if live_chart_task and not live_chart_task.done():
            live_chart_task.cancel()
        run.status = RunStatus.FAILED
        run.error = str(e)
        try:
            await docker_control.teardown_core()
        except Exception:
            pass
    finally:
        registry.finish()


@app.post("/test/start", response_model=TestStartResponse, status_code=202)
async def start_test(req: TestStartRequest):
    run_id = f"run_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:6]}"
    run = TestRun(
        run_id=run_id,
        status=RunStatus.STARTING,
        label=req.label,
        rate=req.rate,
        duration=req.duration,
        policy=req.policy,
        endpoint_id=req.endpoint_id,
        worker_concurrency=req.worker_concurrency,
        poisson=req.poisson,
        started_at=datetime.now(timezone.utc),
    )
    if not registry.try_start(run):
        current = registry.current
        raise HTTPException(status_code=409, detail={
            "detail": "A test run is already in progress",
            "current_run_id": current.run_id if current else None,
        })
    run.task = asyncio.create_task(execute_run(run, req.chaos))
    return TestStartResponse(run_id=run.run_id, status=run.status.value)


@app.post("/test/cancel", response_model=MessageResponse)
async def cancel_test():
    run = registry.current
    if not run or run.status in (RunStatus.DONE, RunStatus.FAILED, RunStatus.CANCELLED):
        raise HTTPException(status_code=409, detail="No run in progress to cancel")
    if run.task:
        run.task.cancel()
    return MessageResponse(detail=f"Cancelling {run.run_id}")


def _to_status_response(run: TestRun) -> TestStatusResponse:
    return TestStatusResponse(
        run_id=run.run_id, status=run.status.value, label=run.label,
        progress=run.progress, chaos_phase=run.chaos_phase, error=run.error,
    )


@app.get("/test/status", response_model=TestStatusResponse)
async def current_status():
    run = registry.current
    if not run:
        raise HTTPException(status_code=404, detail="No run has been started yet")
    return _to_status_response(run)


@app.get("/test/status/{run_id}", response_model=TestStatusResponse)
async def status_by_id(run_id: str):
    run = registry.get(run_id)
    if not run:
        raise HTTPException(status_code=404, detail="Unknown run_id")
    return _to_status_response(run)


@app.get("/test/history", response_model=list[RunHistoryEntry])
async def history():
    return [
        RunHistoryEntry(
            run_id=r.run_id, label=r.label, policy=r.policy,
            status=r.status.value, started_at=r.started_at,
        )
        for r in registry.list_recent()
    ]


@app.get("/test/events", response_model=list[str])
async def recent_events():
    """Newest-first recent event strings from the shared Redis feed --
    delivery outcomes, chaos phase changes, adaptive gate flips. Polled by
    the frontend's live status panel; not a real log pipeline. The core
    stack (and its Redis) doesn't exist between runs or during STARTING,
    so this returns an empty list rather than erroring in that window --
    cosmetic feature, never worth a 500."""
    try:
        r = aioredis.from_url(REDIS_URL, decode_responses=True)
        try:
            return await r.lrange(EVENTS_KEY, 0, 99)
        finally:
            await r.aclose()
    except Exception:
        return []


@app.get("/test/result/{run_id}/chart.png")
async def result_chart(run_id: str):
    run = registry.get(run_id)
    if not run or not run.result_dir:
        raise HTTPException(status_code=404, detail="Result not available")
    chart_path = run.result_dir / "chart.png"
    if not chart_path.exists():
        raise HTTPException(status_code=404, detail="Chart not generated yet")
    return FileResponse(chart_path)


@app.get("/health")
def health():
    return {"status": "ok"}


app.mount("/ui", StaticFiles(directory=str(FRONTEND_DIR), html=True), name="frontend")
