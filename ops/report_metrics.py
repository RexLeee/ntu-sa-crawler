#!/usr/bin/env python3
"""Summarize a run and project the 48-hour outcome.

The Tier 1 metrics are total discovered URLs and total successfully-crawled
URLs. This also reports links-per-page and new-domain growth, which are the
inputs that decide the final numbers.
"""

from __future__ import annotations

import argparse
import glob
import json
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

    # One zcat per file, not one zcat over all of them. Every run this project
    # makes ends in SIGKILL, so the last gzip member has no end-of-stream
    # marker and zcat reports "unexpected end of file". Given several files it
    # stops there and never reads the rest, while still exiting 0 through the
    # pipe: a 4 shard run reported 116,000 discovered URLs instead of 448,000,
    # with no error shown. Reading each file in its own zcat contains the
    # failure to the file that is actually truncated.
    reads = "; ".join(f"zcat -f '{p}' 2>/dev/null || true" for p in paths)
    # LC_ALL=C compares bytes, which is both correct for URLs and much faster.
    pipeline = f"{{ {reads}; }} | LC_ALL=C sort -u | wc -l"
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


def count_unique_ok(paths: list[Path]) -> int:
    """Distinct URLs that returned 2xx.

    Distinct, not a row count. A shard that is restarted resumes its Bloom
    filter from JOBDIR, but a JOBDIR moved aside as broken does not, and that
    shard re-fetches what it already had. The Tier 1 figure has to be a count
    of pages, so the rows are deduplicated here the same way the discovered
    URLs are.
    """
    if not paths:
        return 0
    reads = "; ".join(f"zcat -f '{p}' 2>/dev/null || true" for p in paths)
    # Field 4 is the status, field 3 the URL. Awk filters before sort so the
    # sort only handles the rows that count.
    pipeline = (
        f"{{ {reads}; }} | LC_ALL=C awk -F'\\t' '$4 ~ /^2/ {{print $3}}' "
        f"| LC_ALL=C sort -u | wc -l"
    )
    try:
        out = subprocess.run(
            ["sh", "-c", pipeline], capture_output=True, text=True, check=True
        )
        return int(out.stdout.strip())
    except (subprocess.CalledProcessError, FileNotFoundError, ValueError):
        seen: set[str] = set()
        for path in paths:
            for line in read_lines(path):
                parts = line.rstrip("\n").split("\t")
                if len(parts) >= 4 and parts[3].startswith("2"):
                    seen.add(parts[2])
        return len(seen)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--crawled", nargs="*", default=[])
    ap.add_argument("--discovered", nargs="*", default=[])
    ap.add_argument("--json", default="", help="also write every number here")
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
    total_bytes = 0
    missing_stamp = 0
    status_counts: dict[str, int] = defaultdict(int)
    domains: set[str] = set()
    latencies: list[float] = []

    for path in crawled_paths:
        for line in read_lines(path):
            p = line.rstrip("\n").split("\t")
            if len(p) < 6:
                continue
            sent, recv, url, status, _ctype, nlinks = p[0], p[1], p[2], p[3], p[4], p[5]
            crawled += 1
            status_counts[status] += 1
            if status.startswith("2"):
                ok_crawled += 1
            try:
                total_links += int(nlinks)
            except ValueError:
                pass
            # Column 7 is the body size, added so the report can state bytes
            # downloaded. Older logs have six columns.
            if len(p) >= 7:
                try:
                    total_bytes += int(p[6])
                except ValueError:
                    pass
            domains.add(slot_key(url))
            try:
                recv_f = float(recv)
            except ValueError:
                continue
            last_ts = max(last_ts, recv_f)
            # "NA" means the response carried no dispatch stamp. It used to be
            # silently replaced by the receive time, which always widens the
            # measured gap and so could only ever hide a violation. Count it
            # instead: it should be 0, and the latency of such a row is unknown.
            if sent == "NA":
                missing_stamp += 1
                continue
            try:
                sent_f = float(sent)
            except ValueError:
                continue
            first_ts = min(first_ts, sent_f)
            latencies.append(recv_f - sent_f)

    discovered_lines, unique_count = count_discovered(discovered_paths)
    unique_ok = count_unique_ok(crawled_paths)

    elapsed = max(last_ts - first_ts, 1e-6)
    pages_per_sec = crawled / elapsed
    links_per_page = total_links / crawled if crawled else 0.0
    new_per_page = unique_count / crawled if crawled else 0.0
    latencies.sort()

    print("=== tier 1 ===")
    print(f"discovered (unique): {unique_count}")
    print(f"crawled ok (unique): {unique_ok}")
    print("Neither includes a URL found in a sitemap: the crawler never")
    print("fetches sitemap.xml and never reads robots.txt Sitemap: lines.")

    print("\n=== measured ===")
    print(f"elapsed            : {elapsed:.1f}s")
    print(f"crawled (all rows) : {crawled}")
    print(f"crawled (2xx rows) : {ok_crawled}  ({100*ok_crawled/max(crawled,1):.1f}%)")
    print(f"discovered (raw)   : {discovered_lines}")
    print(f"domains touched    : {len(domains)}")
    print(f"bytes downloaded   : {total_bytes:,} ({total_bytes/1e9:.2f} GB)")
    print(f"pages/sec          : {pages_per_sec:.2f}")
    print(f"links/page         : {links_per_page:.1f}")
    print(f"unique new/page    : {new_per_page:.1f}")
    if missing_stamp:
        print(f"MISSING STAMP      : {missing_stamp} row(s) with no dispatch time")
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

    if args.json:
        payload = {
            "discovered_unique": unique_count,
            "crawled_ok_unique": unique_ok,
            "discovered_raw_lines": discovered_lines,
            "crawled_rows": crawled,
            "crawled_2xx_rows": ok_crawled,
            "domains_touched": len(domains),
            "bytes_downloaded": total_bytes,
            "elapsed_seconds": elapsed,
            "pages_per_sec": pages_per_sec,
            "links_per_page": links_per_page,
            "missing_dispatch_stamp": missing_stamp,
            "status_counts": dict(status_counts),
            "latency_p50": latencies[len(latencies) // 2] if latencies else None,
            "latency_p90": (
                latencies[int(len(latencies) * 0.9)] if latencies else None
            ),
            "sitemap_urls_included": False,
            "crawled_files": [str(p) for p in crawled_paths],
            "discovered_files": [str(p) for p in discovered_paths],
        }
        Path(args.json).write_text(json.dumps(payload, indent=1), encoding="utf-8")
        print(f"\nwrote {args.json}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
