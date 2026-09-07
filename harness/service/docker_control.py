"""
Docker-outside-of-Docker: this container shells out to `docker compose`
against the host's daemon (via the mounted socket) to manage the core
product's lifecycle, reusing the exact --wait/healthcheck semantics
run_experiment.sh already relies on rather than reimplementing them
against the Docker Engine API directly.
"""
import asyncio
import os

CORE_COMPOSE_FILE = os.getenv("CORE_COMPOSE_FILE", "docker-compose.core.yml")
PROJECT_DIR = os.getenv("PROJECT_DIR", "/workspace")


async def _run(cmd: list[str], extra_env: dict | None = None) -> None:
    env = {**os.environ, **(extra_env or {})}
    proc = await asyncio.create_subprocess_exec(
        *cmd, cwd=PROJECT_DIR, env=env,
        stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT,
    )
    out, _ = await proc.communicate()
    if proc.returncode != 0:
        raise RuntimeError(f"{' '.join(cmd)} failed:\n{out.decode()}")


async def restart_core(policy: str, worker_concurrency: int) -> None:
    await _run(["docker", "compose", "-f", CORE_COMPOSE_FILE, "down", "-v"])
    await _run(
        ["docker", "compose", "-f", CORE_COMPOSE_FILE, "up", "--build", "-d", "--wait"],
        extra_env={"RETRY_POLICY": policy, "WORKER_CONCURRENCY": str(worker_concurrency)},
    )


async def teardown_core() -> None:
    await _run(["docker", "compose", "-f", CORE_COMPOSE_FILE, "down", "-v"])


async def stop_worker() -> None:
    await _run(["docker", "compose", "-f", CORE_COMPOSE_FILE, "stop", "worker"])


async def extract_delivery_log(dest_path: str) -> None:
    await _run(["docker", "cp", "eventpulse-worker:/app/delivery_log.csv", dest_path])
