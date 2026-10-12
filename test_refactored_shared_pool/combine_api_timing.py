"""
combine_api_timing.py

Merges the per-call timing logs saved by EACH extraction script
(comments, issue_links, documents, issue_details, sprints, etc.)
into a single combined report, so you can see the TRUE total number
of API calls made every minute across the whole pipeline -- not just
within one script's isolated run.

Each extraction script must call, at the end of its run:

    tracker.save_timing_log(
        "data/raw/logs/<name>_timing.json",
        source_name="<name>"
    )

e.g.:
    tracker.save_timing_log("data/raw/logs/comments_timing.json", source_name="comments")
    tracker.save_timing_log("data/raw/logs/issue_links_timing.json", source_name="issue_links")
    tracker.save_timing_log("data/raw/logs/documents_timing.json", source_name="documents")
    tracker.save_timing_log("data/raw/logs/issue_details_timing.json", source_name="issue_details")

Then run this script once, pointed at the log directory:

    python combine_api_timing.py data/raw/logs/

It prints:
  - total combined API calls across ALL scripts
  - combined 429 count and permanent-failure count
  - a per-source breakdown (how many calls each script made)
  - the TRUE combined per-minute breakdown (fixed wall-clock minutes),
    summed across every script that was running during that minute
  - the true peak minute across the whole pipeline
  - overall run start/end (earliest call to latest call, across all scripts)
"""

import sys
import os
import glob
import json
from datetime import datetime, timedelta

try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

_PROJECT_KEY = os.getenv("project_key")
_DEFAULT_LOG_DIR = f"{_PROJECT_KEY}_data/raw/logs" if _PROJECT_KEY else "data/raw/logs"


def load_timing_logs(log_dir):
    """Load every *_timing.json file in log_dir."""
    pattern = os.path.join(log_dir, "*_timing.json")
    files = sorted(glob.glob(pattern))
    if not files:
        print(f"[WARN] No *_timing.json files found in '{log_dir}'. "
              f"Make sure each script calls tracker.save_timing_log(...).")
    logs = []
    for fp in files:
        with open(fp) as f:
            data = json.load(f)
            data["_file"] = fp
            logs.append(data)
    return logs


def combine(logs):
    all_timestamps = []          # (datetime, source) tuples
    all_429_events = []
    all_permanent_failures = []
    per_source_totals = {}

    for log in logs:
        source = log.get("source", os.path.basename(log["_file"]))
        ts_list = [datetime.fromisoformat(t) for t in log.get("call_timestamps", [])]
        all_timestamps.extend((ts, source) for ts in ts_list)

        for ev in log.get("retryable_error_events", []):
            ev = dict(ev)
            ev["source"] = source
            all_429_events.append(ev)

        for pf in log.get("permanent_failures", []):
            pf = dict(pf)
            pf["source"] = source
            all_permanent_failures.append(pf)

        per_source_totals[source] = log.get("total_api_calls", len(ts_list))

    all_timestamps.sort(key=lambda x: x[0])
    return all_timestamps, all_429_events, all_permanent_failures, per_source_totals


def per_minute_combined(all_timestamps, all_429_events):
    """Fixed wall-clock per-minute totals across ALL sources combined."""
    if not all_timestamps:
        return []

    def minute_key(ts):
        return ts.replace(second=0, microsecond=0)

    first_minute = minute_key(all_timestamps[0][0])
    last_minute = minute_key(all_timestamps[-1][0])

    counts = {}
    source_counts = {}  # minute -> {source: count}
    cur = first_minute
    while cur <= last_minute:
        counts[cur] = 0
        source_counts[cur] = {}
        cur += timedelta(minutes=1)

    for ts, source in all_timestamps:
        k = minute_key(ts)
        counts[k] += 1
        source_counts[k][source] = source_counts[k].get(source, 0) + 1

    event_counts = {}
    for ev in all_429_events:
        k = minute_key(datetime.fromisoformat(ev["time"]))
        event_counts[k] = event_counts.get(k, 0) + 1

    buckets = []
    for start in sorted(counts.keys()):
        end = start + timedelta(seconds=59)
        buckets.append({
            "from": start.strftime("%H:%M:%S"),
            "to": end.strftime("%H:%M:%S"),
            "api_calls": counts[start],
            "429_count": event_counts.get(start, 0),
            "by_source": source_counts[start],
        })
    return buckets

def status_breakdown_combined(all_429_events):
    breakdown = {}
    for ev in all_429_events:
        code = ev.get("status_code", "unknown")
        breakdown[code] = breakdown.get(code, 0) + 1
    return breakdown

def print_report(all_timestamps, all_429_events, all_permanent_failures,
                  per_source_totals, buckets):
    total_calls = sum(per_source_totals.values())

    print("\n" + "=" * 70)
    print("  COMBINED API CALL TIMING SUMMARY (ALL SCRIPTS)")
    print("=" * 70)
    print(f"  Total API calls (all sources combined) : {total_calls}")
    print(f"  429 occurrences (combined)              : {len(all_429_events)}")
    print(f"  Permanently failed (combined)           : {len(all_permanent_failures)}")

    breakdown = status_breakdown_combined(all_429_events)
    if breakdown:
        print("\n  Combined status code breakdown:")
        for code in sorted(breakdown, key=lambda c: (isinstance(c, str), c)):
            print(f"    {code}: {breakdown[code]} occurrence(s)")

    if all_timestamps:
        print(f"  Run started (earliest call, any source) : "
              f"{all_timestamps[0][0].strftime('%H:%M:%S')}")
        print(f"  Run ended (latest call, any source)     : "
              f"{all_timestamps[-1][0].strftime('%H:%M:%S')}")

    print("\n  Per-source totals:")
    for source, count in sorted(per_source_totals.items(), key=lambda x: -x[1]):
        print(f"    {source:20s} : {count} calls")

    print("\n  Combined per-minute breakdown (fixed wall-clock minutes, ALL sources):")
    for b in buckets:
        flag = f"  <-- {b['429_count']} x 429" if b["429_count"] else ""
        by_src = ", ".join(f"{s}={c}" for s, c in sorted(b["by_source"].items(), key=lambda x: -x[1]))
        print(f"    {b['from']} - {b['to']} : {b['api_calls']} calls{flag}")
        print(f"        breakdown: {by_src}")

    if buckets:
        peak = max(buckets, key=lambda b: b["api_calls"])
        print(f"\n  TRUE peak minute (combined across all sources): "
              f"{peak['from']} - {peak['to']} -> {peak['api_calls']} calls "
              f"({peak['429_count']} were 429s)")
        print(f"        breakdown: " +
              ", ".join(f"{s}={c}" for s, c in sorted(peak["by_source"].items(), key=lambda x: -x[1])))

    if all_permanent_failures:
        print("\n  Permanently failed endpoints (combined, gave up after 2 attempts):")
        for pf in sorted(all_permanent_failures, key=lambda x: x.get("time", "")):
            print(f"    - [{pf['source']}] call #{pf['call_number']}  "
                  f"endpoint='{pf['endpoint']}'  error={pf['error']}  at {pf['time']}")

    print("=" * 70 + "\n")


def main():
    log_dir = sys.argv[1] if len(sys.argv) > 1 else _DEFAULT_LOG_DIR
    logs = load_timing_logs(log_dir)
    if not logs:
        return

    all_timestamps, all_429_events, all_permanent_failures, per_source_totals = combine(logs)
    buckets = per_minute_combined(all_timestamps, all_429_events)
    print_report(all_timestamps, all_429_events, all_permanent_failures,
                 per_source_totals, buckets)

    # Also dump a machine-readable combined JSON report
    out_path = os.path.join(log_dir, "combined_timing_report.json")
    with open(out_path, "w") as f:
        json.dump({
            "total_api_calls": sum(per_source_totals.values()),
            "per_source_totals": per_source_totals,
            "rate_limit_429_count": len(all_429_events),
            "permanent_failure_count": len(all_permanent_failures),
            "rate_limit_429_events": all_429_events,
            "permanent_failures": all_permanent_failures,
            "per_minute_breakdown": buckets,
            "run_started": all_timestamps[0][0].isoformat() if all_timestamps else None,
            "run_ended": all_timestamps[-1][0].isoformat() if all_timestamps else None,
            "status_code_breakdown": status_breakdown_combined(all_429_events),
        }, f, indent=2)
    print(f"Combined JSON report saved to: {out_path}")


if __name__ == "__main__":
    main()