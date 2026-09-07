"""
Reads one or more CSVs -- load_generator.py's ingest-side output or
worker.py's delivery_log.csv -- and plots goodput and latency over time.
Auto-detects which format each file is (see is_success()). The worker's
file is the one that matters once chaos is involved, since the API keeps
returning 202 even while delivery collapses behind it.

Usage:
    python analyze.py --file none:delivery_none.csv
    python analyze.py --file none:delivery_none.csv --file naive:delivery_naive.csv
"""
import argparse
import csv
import json
from collections import defaultdict
from datetime import datetime

import matplotlib.pyplot as plt


def load_csv(path: str):
    with open(path, newline="") as f:
        return list(csv.DictReader(f))


def is_success(row: dict) -> bool:
    # load_generator.py rows: "result" (ok/rejected/error). worker.py rows:
    # "outcome" (delivered/dlq). Both share timestamp/latency_ms columns.
    if "outcome" in row:
        return row["outcome"] == "delivered"
    if "result" in row:
        return row["result"] == "ok"
    raise ValueError(f"Unrecognized CSV row shape: {list(row.keys())}")


def compute_goodput_per_bucket(rows, bucket_seconds: float = 1.0):
    """Buckets successful sends into fixed-width windows, normalized to
    events/sec. Returns (times, goodput, start) -- start is t=0 for this
    file, used to align external timestamps onto the same axis."""
    if not rows:
        return [], [], None

    timestamps = [datetime.fromisoformat(r["timestamp"]) for r in rows]
    start = min(timestamps)

    buckets = defaultdict(int)
    for row, ts in zip(rows, timestamps):
        if is_success(row):
            elapsed = (ts - start).total_seconds()
            bucket_index = int(elapsed / bucket_seconds)
            buckets[bucket_index] += 1

    if not buckets:
        return [], [], start

    max_bucket = max(buckets.keys())
    bucket_indices = list(range(max_bucket + 1))
    times = [b * bucket_seconds for b in bucket_indices]
    goodput = [buckets.get(b, 0) / bucket_seconds for b in bucket_indices]
    return times, goodput, start


def load_timeline(path: str) -> dict:
    with open(path) as f:
        return json.load(f)


def elapsed_seconds(timeline_iso: str, start: datetime) -> float:
    return (datetime.fromisoformat(timeline_iso) - start).total_seconds()


def compute_baseline_goodput(times, goodput, steady_start_s: float, fault_start_s: float) -> float:
    """Average goodput during the steady-state window before the fault."""
    window = [g for t, g in zip(times, goodput) if steady_start_s <= t < fault_start_s]
    if not window:
        return 0.0
    return sum(window) / len(window)


def compute_recovery_time(
    times, goodput, fault_end_s: float, baseline: float,
    tolerance: float = 0.9, sustained_seconds: float = 3.0, bucket_seconds: float = 1.0,
):
    """Seconds from fault_end until goodput returns to >= tolerance * baseline
    and stays there for sustained_seconds. None if it never sustains within
    the observed window."""
    if baseline <= 0:
        return None

    threshold = tolerance * baseline
    sustained_buckets = max(1, int(round(sustained_seconds / bucket_seconds)))

    post_fault = [(t, g) for t, g in zip(times, goodput) if t >= fault_end_s]

    for i, (t, g) in enumerate(post_fault):
        if g >= threshold:
            window = post_fault[i:i + sustained_buckets]
            if len(window) == sustained_buckets and all(wg >= threshold for _, wg in window):
                return t - fault_end_s
    return None


def compute_delivery_stats(rows) -> dict | None:
    """Retries-per-request and rejection-rate summary -- only meaningful for
    worker.py's delivery_log.csv rows (has "outcome"/"attempt"), not
    load_generator.py's ingest-side rows. Returns None for the latter."""
    if not rows or "outcome" not in rows[0]:
        return None

    attempts_per_event: dict[str, int] = defaultdict(int)
    retries = 0
    delivered = 0
    dlq = 0
    for row in rows:
        event_id = row["event_id"]
        attempts_per_event[event_id] = max(attempts_per_event[event_id], int(row["attempt"]))
        if row["outcome"] == "retry":
            retries += 1
        elif row["outcome"] == "delivered":
            delivered += 1
        elif row["outcome"] == "dlq":
            dlq += 1

    total_events = len(attempts_per_event)
    resolved = delivered + dlq
    return {
        "total_events": total_events,
        "resolved": resolved,
        "delivered": delivered,
        "dlq": dlq,
        "retries": retries,
        "avg_attempts": sum(attempts_per_event.values()) / total_events if total_events else 0.0,
        "rejection_rate": dlq / resolved if resolved else 0.0,
    }


def compute_latency_series(rows):
    timestamps = [datetime.fromisoformat(r["timestamp"]) for r in rows]
    start = min(timestamps)
    elapsed = [(ts - start).total_seconds() for ts in timestamps]
    latencies = [float(r["latency_ms"]) for r in rows]
    return elapsed, latencies


def main():
    parser = argparse.ArgumentParser(description="Plot goodput and latency from load_generator.py CSV output")
    parser.add_argument(
        "--file", action="append", required=True,
        help="LABEL:PATH, e.g. naive:results_naive.csv -- repeat for multiple conditions",
    )
    parser.add_argument(
        "--timeline", action="append", default=[],
        help="LABEL:PATH to that label's chaos_timeline.json -- enables recovery-time "
             "calculation and fault-window shading for that condition. Optional per label.",
    )
    parser.add_argument("--output", default="results_chart.png")
    parser.add_argument(
        "--bucket-size", type=float, default=1.0,
        help="goodput bucket width in seconds -- smaller shows more detail, e.g. 0.2",
    )
    parser.add_argument(
        "--recovery-tolerance", type=float, default=0.9,
        help="fraction of steady-state baseline goodput considered 'recovered' (default 0.9)",
    )
    parser.add_argument(
        "--recovery-sustain", type=float, default=3.0,
        help="seconds goodput must stay at/above tolerance to count as recovered, not a one-bucket blip (default 3.0)",
    )
    parser.add_argument(
        "--xlim-max", type=float, default=None,
        help="fix the time axis upper bound (seconds) -- set the same value across side-by-side charts",
    )
    parser.add_argument(
        "--ylim-goodput", type=float, default=None,
        help="fix the goodput axis upper bound (events/sec), same reason as --xlim-max",
    )
    args = parser.parse_args()

    conditions = []
    for entry in args.file:
        label, path = entry.split(":", 1)
        conditions.append((label, load_csv(path)))

    timelines = {}
    for entry in args.timeline:
        label, path = entry.split(":", 1)
        timelines[label] = load_timeline(path)

    fig, (ax_goodput, ax_latency) = plt.subplots(2, 1, figsize=(10, 8), sharex=True)

    recovery_results = {}
    delivery_stats = {}
    shaded_fault_window = False

    for label, rows in conditions:
        stats = compute_delivery_stats(rows)
        if stats is not None:
            delivery_stats[label] = stats

        times, goodput, start = compute_goodput_per_bucket(rows, bucket_seconds=args.bucket_size)
        [line] = ax_goodput.plot(times, goodput, label=label, marker="o", markersize=3)

        # Mark where a condition's line ends early relative to a shared
        # x-axis (e.g. no-retry drains its backlog well before naive does).
        if times and args.xlim_max is not None and (args.xlim_max - times[-1]) > 5:
            ax_goodput.axvline(times[-1], color=line.get_color(), linestyle=":", alpha=0.5, linewidth=1)
            ax_goodput.annotate(
                "drained", xy=(times[-1], 0), xytext=(times[-1] + 2, 0),
                fontsize=7, color=line.get_color(), alpha=0.8, va="bottom",
            )

        elapsed, latencies = compute_latency_series(rows)
        ax_latency.scatter(elapsed, latencies, label=label, s=8, alpha=0.5)

        timeline = timelines.get(label)
        if timeline and times:
            steady_start_s = elapsed_seconds(timeline["steady_start"], start)
            fault_start_s = elapsed_seconds(timeline["fault_start"], start)
            fault_end_s = elapsed_seconds(timeline["fault_end"], start)

            baseline = compute_baseline_goodput(times, goodput, steady_start_s, fault_start_s)
            recovery = compute_recovery_time(
                times, goodput, fault_end_s, baseline,
                tolerance=args.recovery_tolerance,
                sustained_seconds=args.recovery_sustain,
                bucket_seconds=args.bucket_size,
            )
            recovery_results[label] = (baseline, recovery)

            if not shaded_fault_window:
                ax_goodput.axvspan(fault_start_s, fault_end_s, color="red", alpha=0.1, label="fault window")
                shaded_fault_window = True

    ax_goodput.set_ylabel("Goodput (successful events/sec)")
    ax_goodput.set_title("Goodput over time")
    ax_goodput.legend()
    ax_goodput.grid(True, alpha=0.3)

    if args.xlim_max is not None:
        ax_goodput.set_xlim(0, args.xlim_max)
        ax_latency.set_xlim(0, args.xlim_max)
    if args.ylim_goodput is not None:
        ax_goodput.set_ylim(0, args.ylim_goodput)

    ax_latency.set_ylabel("Latency (ms)")
    ax_latency.set_xlabel("Elapsed time (s)")
    ax_latency.set_title("Per-request latency over time")
    ax_latency.legend()
    ax_latency.grid(True, alpha=0.3)

    plt.tight_layout()
    plt.savefig(args.output, dpi=150)
    print(f"[analyze] saved chart to {args.output}")

    if recovery_results:
        print(
            f"\n[analyze] Recovery-time summary "
            f"(tolerance={args.recovery_tolerance:.0%} of baseline, sustained {args.recovery_sustain:.1f}s):"
        )
        for label, (baseline, recovery) in recovery_results.items():
            if recovery is None:
                print(f"  {label}: baseline={baseline:.2f} ev/s -- did NOT recover within the observed window")
            else:
                print(f"  {label}: baseline={baseline:.2f} ev/s -- recovery time = {recovery:.1f}s after fault cleared")

    if delivery_stats:
        print("\n[analyze] Retry/rejection summary:")
        for label, s in delivery_stats.items():
            unresolved = s["total_events"] - s["resolved"]
            print(
                f"  {label}: {s['total_events']} events, {s['retries']} retries "
                f"(avg {s['avg_attempts']:.2f} attempts/event), "
                f"rejection rate {s['rejection_rate']:.1%} ({s['dlq']}/{s['resolved']} resolved)"
                + (f", {unresolved} still unresolved" if unresolved else "")
            )


if __name__ == "__main__":
    main()
