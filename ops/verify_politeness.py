#!/usr/bin/env python3
"""Prove the 0.2 qps per-domain limit was never broken.

Reads crawled.log.gz, groups requests by registered domain (eTLD+1), and
reports every pair of consecutive requests closer together than the limit.

Exit code 0 means zero violations. This output belongs in the report.
"""

from __future__ import annotations

import argparse
import glob
import sys
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from crawler.config import load_config  # noqa: E402
from crawler.logwriter import read_lines  # noqa: E402
from crawler.slot import slot_key  # noqa: E402

# Clock jitter and log-write ordering can shave a few ms off a legitimate gap.
TOLERANCE_SECONDS = 0.05


def load_events(paths: list[Path], source: str) -> tuple[dict[str, list[float]], int]:
    """Map domain -> sorted dispatch times. Returns the map and a skip count.

    Two sources, and they cover different populations.

    crawled: send_ts, recv_ts, url, status, content_type, n_links, bytes.
    Written from parse(), so it holds the fetches that returned a parseable
    response. It omits every failed request and every robots.txt fetch, and
    both consumed a real turn on the domain's timer. In one measured run
    40,957 of 45,825 robots fetches timed out, so most of what the crawler
    sent was invisible here.

    dispatched: dispatch_ts, slot_id, url, monotonic_ts. Written from the
    dispatch timer itself, so it holds everything. This is the source the
    compliance claim rests on; the other is kept because it is what earlier
    runs recorded.

    Gaps are measured from the monotonic column when it is present, because
    that is the clock the in-process guard compared. The wall clock can be
    stepped by NTP, and the measured margin is 135 ms (a 5.135s minimum gap
    against a 5.000s floor), so a backward step larger than that would
    fabricate a violation. Logs written before that column existed have three
    columns and fall back to the wall clock, which is what earlier runs were
    verified with.
    """
    by_domain: dict[str, list[float]] = defaultdict(list)
    skipped = 0
    monotonic_rows = 0
    wall_rows = 0
    for path in paths:
        for lineno, line in enumerate(read_lines(path), 1):
            parts = line.rstrip("\n").split("\t")
            if source == "dispatched":
                if len(parts) < 2:
                    continue
                # Column 2 is the key the timer itself grouped by. Using it
                # rather than re-deriving from the URL means a disagreement
                # about identity shows up as a violation instead of hiding one.
                key = parts[1]
            else:
                if len(parts) < 3:
                    continue
                key = slot_key(parts[2])
            # "NA" is a response that carried no dispatch stamp. It cannot be
            # checked, so it is counted and reported rather than dropped.
            if parts[0] == "NA":
                skipped += 1
                continue
            ts = None
            if source == "dispatched" and len(parts) >= 4:
                try:
                    ts = float(parts[3])
                    monotonic_rows += 1
                except ValueError:
                    ts = None
            if ts is None:
                try:
                    ts = float(parts[0])
                    wall_rows += 1
                except ValueError:
                    print(f"{path}:{lineno}: bad timestamp, skipped", file=sys.stderr)
                    skipped += 1
                    continue
            by_domain[key].append(ts)
    if monotonic_rows and wall_rows:
        # Mixing the two clocks inside one domain's series would produce
        # meaningless gaps, so say so rather than print a verdict.
        print(
            f"WARNING: {monotonic_rows} rows carry a monotonic stamp and "
            f"{wall_rows} do not; gaps spanning the boundary are not comparable",
            file=sys.stderr,
        )
    return by_domain, skipped


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("logs", nargs="*", help="log files (default: from --source)")
    ap.add_argument("--show", type=int, default=20, help="max violations to print")
    ap.add_argument(
        "--source",
        choices=("crawled", "dispatched"),
        default="dispatched",
        help="which log to check; dispatched covers robots.txt and failures too",
    )
    args = ap.parse_args()

    cfg = load_config()
    # Deliberately not download_delay. That setting carries a safety margin
    # above the requirement, and reading it here would let a larger margin
    # silently relax the test it is meant to pass.
    limit = cfg["politeness"]["required_min_gap"]

    root = Path(__file__).resolve().parent.parent
    if args.logs:
        paths = [Path(p) for p in args.logs]
    else:
        pattern = f"{args.source}*.log.gz"
        paths = [Path(p) for p in sorted(glob.glob(str(root / "data" / pattern)))]

    paths = [p for p in paths if p.exists()]
    if not paths:
        print(f"no {args.source} logs found", file=sys.stderr)
        if args.source == "dispatched":
            print(
                "set politeness.log_dispatches = true in config.toml and re-run",
                file=sys.stderr,
            )
        return 2

    by_domain, skipped = load_events(paths, args.source)

    violations: list[tuple[str, float, float]] = []
    total_requests = 0
    min_gap_overall = float("inf")

    for domain, stamps in by_domain.items():
        total_requests += len(stamps)
        if len(stamps) < 2:
            continue
        stamps.sort()
        for earlier, later in zip(stamps, stamps[1:], strict=False):
            gap = later - earlier
            min_gap_overall = min(min_gap_overall, gap)
            if gap < limit - TOLERANCE_SECONDS:
                violations.append((domain, gap, later))

    print(f"source      : {args.source}")
    print(f"files       : {len(paths)}")
    print(f"requests    : {total_requests}")
    print(f"domains     : {len(by_domain)}")
    print(f"limit       : {limit:.2f}s per registered domain")
    if skipped:
        print(f"UNCHECKABLE : {skipped} row(s) had no usable dispatch time")
    if args.source == "crawled":
        print("Note: this source holds parseable responses only. It omits failed")
        print("requests and every robots.txt fetch. Use --source dispatched.")
    if min_gap_overall < float("inf"):
        print(f"min gap seen: {min_gap_overall:.3f}s")

    if violations:
        violations.sort(key=lambda v: v[1])
        print(f"\nVIOLATIONS  : {len(violations)}")
        for domain, gap, ts in violations[: args.show]:
            print(f"  {domain:40s} gap={gap:.3f}s at {ts:.3f}")
        if len(violations) > args.show:
            print(f"  ... and {len(violations) - args.show} more")
        return 1

    print("\nVIOLATIONS  : 0")
    if skipped:
        # A row with no dispatch time is a row whose gap was never checked.
        # Reporting zero violations over an incomplete check would overstate
        # what was verified, so this fails too.
        print(f"but {skipped} row(s) could not be checked; treating as a failure")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
