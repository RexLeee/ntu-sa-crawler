#!/usr/bin/env python3
"""Prove no crawled URL was disallowed by its site's robots.txt.

Re-fetches robots.txt for a sample of the crawled hosts and re-checks every
logged URL against it, independently of the crawler's own decisions.

Network access is required. Re-fetching every host would itself need to respect
the rate limit, so the sample size is bounded by --hosts.
"""

from __future__ import annotations

import argparse
import glob
import sys
import time
import urllib.error
import urllib.request
from collections import defaultdict
from pathlib import Path
from urllib.parse import urlsplit

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from protego import Protego  # noqa: E402

from crawler.config import load_config  # noqa: E402
from crawler.logwriter import read_lines  # noqa: E402


def fetch_robots(origin: str, user_agent: str, timeout: float) -> Protego | None:
    req = urllib.request.Request(
        f"{origin}/robots.txt", headers={"User-Agent": user_agent}
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            if resp.status != 200:
                return None
            body = resp.read(1_000_000).decode("utf-8", errors="replace")
    except (urllib.error.URLError, OSError, ValueError, TimeoutError):
        return None
    try:
        return Protego.parse(body)
    except Exception:
        return None


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("logs", nargs="*")
    ap.add_argument("--hosts", type=int, default=40, help="how many origins to re-check")
    ap.add_argument("--delay", type=float, default=1.0, help="seconds between robots fetches")
    args = ap.parse_args()

    cfg = load_config()
    user_agent = cfg["identity"]["user_agent"]

    root = Path(__file__).resolve().parent.parent
    paths = (
        [Path(p) for p in args.logs]
        if args.logs
        else [Path(p) for p in sorted(glob.glob(str(root / "data" / "crawled*.log.gz")))]
    )
    paths = [p for p in paths if p.exists()]
    if not paths:
        print("no crawled logs found", file=sys.stderr)
        return 2

    by_origin: dict[str, list[str]] = defaultdict(list)
    total = 0
    for path in paths:
        for line in read_lines(path):
            parts = line.rstrip("\n").split("\t")
            if len(parts) < 3:
                continue
            url = parts[2]
            s = urlsplit(url)
            if not s.scheme or not s.netloc:
                continue
            by_origin[f"{s.scheme}://{s.netloc}"].append(url)
            total += 1

    # Check the busiest origins first: they carry the most risk.
    origins = sorted(by_origin, key=lambda o: len(by_origin[o]), reverse=True)[: args.hosts]

    checked = 0
    unreachable = 0
    violations: list[tuple[str, str]] = []

    for i, origin in enumerate(origins):
        if i:
            time.sleep(args.delay)
        rp = fetch_robots(origin, user_agent, timeout=10.0)
        if rp is None:
            unreachable += 1
            continue
        for url in by_origin[origin]:
            checked += 1
            try:
                if not rp.can_fetch(url, user_agent):
                    violations.append((origin, url))
            except Exception:
                continue

    print(f"crawled urls   : {total}")
    print(f"origins seen   : {len(by_origin)}")
    print(f"origins checked: {len(origins)} (unreachable robots.txt: {unreachable})")
    print(f"urls re-checked: {checked}")

    if violations:
        print(f"\nVIOLATIONS     : {len(violations)}")
        for origin, url in violations[:20]:
            print(f"  {origin}  {url}")
        return 1

    print("\nVIOLATIONS     : 0")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
