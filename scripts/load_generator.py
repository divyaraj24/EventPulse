"""
Thin CLI wrapper around the canonical load-generation implementation in
harness/service/load_generator.py. There is only one real implementation
of load generation in this project; this file exists only to parse
argv for manual/CLI use via run_experiment.sh, so the CLI path and the
harness API path can never silently diverge in behavior (they're the
same code).

Usage:
    python load_generator.py --rate 20 --duration 60 --output results_naive.csv

Run from the host machine (not inside Docker) -- it talks to the API's
host-exposed port (localhost:8000).
"""
import argparse
import asyncio
import importlib.util
from pathlib import Path

# Loaded via importlib with an explicit unique module name, not a plain
# sys.path import -- this file and the canonical one share the literal
# filename "load_generator.py", and a plain `import load_generator` here
# would collide with Python's own module cache (a circular self-import)
# since both would register under the same module name.
_canonical_path = Path(__file__).resolve().parent.parent / "harness" / "service" / "load_generator.py"
_spec = importlib.util.spec_from_file_location("_harness_load_generator", _canonical_path)
_canonical = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_canonical)
run = _canonical.run


def main():
    parser = argparse.ArgumentParser(description="EventPulse load generator")
    parser.add_argument("--rate", type=float, required=True, help="events per second")
    parser.add_argument("--duration", type=float, required=True, help="how long to send, in seconds")
    parser.add_argument("--api-url", default="http://localhost:8000")
    parser.add_argument("--endpoint-id", default="test1")
    parser.add_argument("--endpoint-url", default="http://receiver_mock:9000/webhook")
    parser.add_argument("--output", default="results.csv")
    args = parser.parse_args()

    asyncio.run(run(args.rate, args.duration, args.api_url, args.endpoint_id, args.endpoint_url, args.output))


if __name__ == "__main__":
    main()
