#!/usr/bin/env python3
"""Prove the 0.2 qps per-domain limit was never broken.

Reads crawled.log.gz, groups requests by registered domain (eTLD+1), and
reports every pair of consecutive requests closer together than the limit.

Exit code 0 means zero violations. This output belongs in the report.
"""

from __future__ import annotations

import argparse
import glob
import gzip
import sys
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from crawler.config import load_config  # noqa: E402
from crawler.slot import slot_key  # noqa: E402

# Clock jitter and log-write ordering can shave a few ms off a legitimate gap.
TOLERANCE_SECONDS = 0.05


def load_events(paths: list[Path]) -> dict[str, list[float]]:
    """Map domain -> sorted request send times.

    Column layout: send_ts, recv_ts, url, status, content_type, n_links.
    Scrapy spaces request *starts* by DOWNLOAD_DELAY, so send_ts is the column
    that compliance must be measured on.
    """
    by_domain: dict[str, list[float]] = defaultdict(list)
    for path in paths:
        with gzip.open(path, "rt", encoding="utf-8") as fh:
            for lineno, line in enumerate(fh, 1):
                parts = line.rstrip("\n").split("\t")
                if len(parts) < 3:
                    continue
                try:
                    ts = float(parts[0])
                except ValueError:
                    print(f"{path}:{lineno}: bad timestamp, skipped", file=sys.stderr)
                    continue
                by_domain[slot_key(parts[2])].append(ts)
    return by_domain


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("logs", nargs="*", help="crawled log files (default: data/crawled*.log.gz)")
    ap.add_argument("--show", type=int, default=20, help="max violations to print")
    args = ap.parse_args()

    cfg = load_config()
    limit = cfg["politeness"]["download_delay"]

    root = Path(__file__).resolve().parent.parent
    if args.logs:
        paths = [Path(p) for p in args.logs]
    else:
        paths = [Path(p) for p in sorted(glob.glob(str(root / "data" / "crawled*.log.gz")))]

    paths = [p for p in paths if p.exists()]
    if not paths:
        print("no crawled logs found", file=sys.stderr)
        return 2

    by_domain = load_events(paths)

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

    print(f"files       : {len(paths)}")
    print(f"requests    : {total_requests}")
    print(f"domains     : {len(by_domain)}")
    print(f"limit       : {limit:.2f}s per registered domain")
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
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
