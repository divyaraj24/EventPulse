"""
Thin CLI wrapper around the canonical fault-injection implementation in
harness/service/chaos.py. There is only one real implementation; this
file exists only to parse argv for manual/CLI use via run_experiment.sh.

Run in parallel with load_generator.py (its --duration should cover
--steady + --fault + --recovery so it's still sending when the fault
clears).

Usage:
    python chaos.py --steady 10 --fault 20 --recovery 20 --max-concurrency 3
"""
import argparse
import asyncio
import importlib.util
from pathlib import Path

# Loaded via importlib with an explicit unique module name, not a plain
# sys.path import -- this file and the canonical one share the literal
# filename "chaos.py", which would otherwise collide in sys.modules
# (see docs/private/bugs_and_lessons.md #9, same issue load_generator.py
# hit first).
_canonical_path = Path(__file__).resolve().parent.parent / "harness" / "service" / "chaos.py"
_spec = importlib.util.spec_from_file_location("_harness_chaos", _canonical_path)
_canonical = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_canonical)
run = _canonical.run


def main():
    parser = argparse.ArgumentParser(description="Drive the mock receiver through a steady/fault/recovery cycle")
    parser.add_argument("--receiver-url", default="http://localhost:9000")
    parser.add_argument("--steady", type=float, default=10.0, help="seconds of healthy baseline before the fault")
    parser.add_argument("--fault", type=float, default=20.0, help="seconds the fault stays active")
    parser.add_argument("--recovery", type=float, default=20.0, help="seconds to wait after clearing, for logging only")
    parser.add_argument("--max-concurrency", type=int, default=3, help="capacity ceiling during the fault -- the real bottleneck")
    parser.add_argument("--reject-rate", type=float, default=0.0, help="random rejection probability during the fault")
    parser.add_argument("--latency-ms", type=int, default=0, help="added latency during the fault")
    parser.add_argument(
        "--recovered-max-concurrency", type=int, default=5,
        help="capacity ceiling once the fault 'clears' -- kept bounded so backlog drains "
             "gradually instead of bursting through in one bucket",
    )
    parser.add_argument(
        "--recovered-latency-ms", type=int, default=200,
        help="added latency once the fault 'clears' -- models a still-degraded endpoint",
    )
    parser.add_argument("--timeline-output", default="chaos_timeline.json")
    args = parser.parse_args()

    asyncio.run(run(
        receiver_url=args.receiver_url,
        steady=args.steady,
        fault=args.fault,
        recovery=args.recovery,
        max_concurrency=args.max_concurrency,
        reject_rate=args.reject_rate,
        latency_ms=args.latency_ms,
        recovered_max_concurrency=args.recovered_max_concurrency,
        recovered_latency_ms=args.recovered_latency_ms,
        timeline_output=args.timeline_output,
    ))


if __name__ == "__main__":
    main()
