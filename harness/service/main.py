import asyncio
import os
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import redis.asyncio as aioredis
from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

import chaos as chaos_module
import docker_control
import load_generator
from models import ChaosConfig, TestStartRequest, TestStartResponse, TestStatusResponse
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

# Unlike analyze.py, the frontend lives inside harness/service/ itself, so
# it IS copied into the image at build time (Dockerfile's `COPY . .`) --
# __file__-relative resolution is correct here, not PROJECT_DIR.
FRONTEND_DIR = Path(__file__).resolve().parent / "frontend"
REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379/0")
API_URL = os.getenv("API_URL", "http://api:8000")
DRAIN_TIMEOUT_SECONDS = float(os.getenv("DRAIN_TIMEOUT_SECONDS", "180"))

app = FastAPI(title="EventPulse Harness Service")
registry = RunRegistry()


async def wait_for_drain(timeout_seconds: float) -> None:
    r = aioredis.from_url(REDIS_URL, decode_responses=True)
    start = asyncio.get_event_loop().time()
    try:
        while True:
            groups = await r.xinfo_groups("deliveries")
            g = groups[0]
            remaining = (g.get("lag") or 0) + (g.get("pending") or 0)
            if remaining <= 0:
                return
            if asyncio.get_event_loop().time() - start >= timeout_seconds:
                return  # proceed anyway, same as run_experiment.sh's warn-and-continue
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


async def execute_run(run: TestRun, chaos_config: ChaosConfig) -> None:
    run_dir = RESULTS_DIR / run.run_id
    run_dir.mkdir(parents=True, exist_ok=True)
    ingest_csv = run_dir / "ingest.csv"
    delivery_csv = run_dir / "delivery.csv"
    timeline_json = run_dir / "timeline.json"
    chart_png = run_dir / "chart.png"

    try:
        run.status = RunStatus.STARTING
        await docker_control.restart_core(run.policy)

        run.status = RunStatus.RUNNING

        def on_progress(sent: int, total: int) -> None:
            run.progress = {"events_sent": sent, "events_total": total}

        def on_phase_change(phase: str) -> None:
            run.chaos_phase = phase

        load_gen_task = load_generator.run(
            rate=run.rate,
            duration=run.duration,
            api_url=API_URL,
            endpoint_id=run.endpoint_id,
            endpoint_url="http://receiver_mock:9000/webhook",
            output_path=str(ingest_csv),
            on_progress=on_progress,
        )

        if chaos_config.enabled:
            chaos_task = chaos_module.run(
                receiver_url="http://receiver_mock:9000",
                steady=chaos_config.steady,
                fault=chaos_config.fault,
                recovery=chaos_config.recovery,
                max_concurrency=chaos_config.max_concurrency,
                reject_rate=chaos_config.reject_rate,
                latency_ms=chaos_config.latency_ms,
                recovered_max_concurrency=chaos_config.recovered_max_concurrency,
                recovered_latency_ms=chaos_config.recovered_latency_ms,
                timeline_output=str(timeline_json),
                on_phase_change=on_phase_change,
            )
            await asyncio.gather(load_gen_task, chaos_task)
        else:
            await load_gen_task

        run.status = RunStatus.DRAINING
        await wait_for_drain(DRAIN_TIMEOUT_SECONDS)

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

    except Exception as e:
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
        started_at=datetime.now(timezone.utc),
    )
    if not registry.try_start(run):
        current = registry.current
        raise HTTPException(status_code=409, detail={
            "detail": "A test run is already in progress",
            "current_run_id": current.run_id if current else None,
        })
    asyncio.create_task(execute_run(run, req.chaos))
    return TestStartResponse(run_id=run.run_id, status=run.status.value)


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
