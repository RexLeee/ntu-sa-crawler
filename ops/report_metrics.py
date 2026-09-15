#!/usr/bin/env python3
"""Summarize a run and project the 48-hour outcome.

The Tier 1 metrics are total discovered URLs and total successfully-crawled
URLs. This also reports links-per-page and new-domain growth, which are the
inputs that decide the final numbers.
"""

from __future__ import annotations

import argparse
import glob
import subprocess
import sys
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from crawler.logwriter import read_lines  # noqa: E402
from crawler.slot import slot_key  # noqa: E402

HOURS_48 = 48 * 3600


def _paths(pattern: str, given: list[str]) -> list[Path]:
    root = Path(__file__).resolve().parent.parent
    if given:
        return [Path(p) for p in given if Path(p).exists()]
    return [Path(p) for p in sorted(glob.glob(str(root / "data" / pattern)))]


def count_discovered(paths: list[Path]) -> tuple[int, int]:
    """Return (total lines, distinct URLs) without holding them in memory.

    A Python set costs about 42 bytes per URL, so the 127M URLs a 48 hour run
    produces would need 5 GB and exhaust the machine. `sort -u` spills to disk
    instead, which the WSL rootfs has ample room for.

    Falls back to a set when sort is unavailable, since short runs fit easily.
    """
    if not paths:
        return 0, 0

    files = " ".join(f"'{p}'" for p in paths)
    # LC_ALL=C compares bytes, which is both correct for URLs and much faster.
    pipeline = f"zcat -f {files} | LC_ALL=C sort -u | wc -l"
    try:
        out = subprocess.run(
            ["sh", "-c", pipeline], capture_output=True, text=True, check=True
        )
        unique = int(out.stdout.strip())
    except (subprocess.CalledProcessError, FileNotFoundError, ValueError):
        seen: set[str] = set()
        for path in paths:
            for line in read_lines(path):
                url = line.rstrip("\n")
                if url:
                    seen.add(url)
        return sum(1 for p in paths for line in read_lines(p) if line.strip()), len(seen)

    total = 0
    for path in paths:
        for line in read_lines(path):
            if line.strip():
                total += 1
    return total, unique


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--crawled", nargs="*", default=[])
    ap.add_argument("--discovered", nargs="*", default=[])
    args = ap.parse_args()

    crawled_paths = _paths("crawled*.log.gz", args.crawled)
    discovered_paths = _paths("discovered*.log.gz", args.discovered)
    if not crawled_paths:
        print("no crawled logs found", file=sys.stderr)
        return 2

    first_ts = float("inf")
    last_ts = 0.0
    crawled = 0
    ok_crawled = 0
    total_links = 0
    status_counts: dict[str, int] = defaultdict(int)
    domains: set[str] = set()
    latencies: list[float] = []

    for path in crawled_paths:
        for line in read_lines(path):
            p = line.rstrip("\n").split("\t")
            if len(p) < 6:
                continue
            sent, recv, url, status, _ctype, nlinks = p[0], p[1], p[2], p[3], p[4], p[5]
            try:
                sent_f, recv_f = float(sent), float(recv)
            except ValueError:
                continue
            crawled += 1
            first_ts = min(first_ts, sent_f)
            last_ts = max(last_ts, recv_f)
            latencies.append(recv_f - sent_f)
            status_counts[status] += 1
            if status.startswith("2"):
                ok_crawled += 1
            try:
                total_links += int(nlinks)
            except ValueError:
                pass
            domains.add(slot_key(url))

    discovered_lines, unique_count = count_discovered(discovered_paths)

    elapsed = max(last_ts - first_ts, 1e-6)
    pages_per_sec = crawled / elapsed
    links_per_page = total_links / crawled if crawled else 0.0
    new_per_page = unique_count / crawled if crawled else 0.0
    latencies.sort()

    print("=== measured ===")
    print(f"elapsed            : {elapsed:.1f}s")
    print(f"crawled (all)      : {crawled}")
    print(f"crawled (2xx)      : {ok_crawled}  ({100*ok_crawled/max(crawled,1):.1f}%)")
    print(f"discovered (raw)   : {discovered_lines}")
    print(f"discovered (unique): {unique_count}")
    print(f"domains touched    : {len(domains)}")
    print(f"pages/sec          : {pages_per_sec:.2f}")
    print(f"links/page         : {links_per_page:.1f}")
    print(f"unique new/page    : {new_per_page:.1f}")
    if latencies:
        print(f"latency p50/p90    : {latencies[len(latencies)//2]:.2f}s / "
              f"{latencies[int(len(latencies)*0.9)]:.2f}s")

    print("\nstatus codes:")
    for code in sorted(status_counts):
        print(f"  {code}: {status_counts[code]}")

    # The per-domain limit caps throughput at active_domains / delay, so a
    # short sample understates the steady state. Report the naive projection
    # and the domain-bound ceiling side by side.
    print("\n=== naive 48h projection (assumes this rate holds) ===")
    print(f"crawled            : {pages_per_sec * HOURS_48:,.0f}")
    print(f"discovered         : {pages_per_sec * HOURS_48 * new_per_page:,.0f}")
    print("\nNote: a short run is still ramping up. Front size, not this rate,")
    print("decides the steady state: pages/sec <= active_domains / delay.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
