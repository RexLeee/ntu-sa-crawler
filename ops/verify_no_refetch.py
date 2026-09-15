#!/usr/bin/env python3
"""Prove no URL was fetched twice.

Every fetch costs a request against a domain's 0.2 qps budget, so a URL
fetched twice is budget spent on nothing. It is also a symptom worth catching
on its own: it means a request reached the downloader without passing the
duplicate filter.

Two paths produced that, and only one is closed.

The first was MetaRefreshMiddleware, which builds its target with
source_request.replace() and so copies dont_filter along with everything else.
The spider sets that flag on requests it has already checked against the Bloom
filter, so a meta refresh target inherited a promise made about a different
URL and skipped the filter entirely. That path is off.

The second is still open, and is a property of redirects rather than a defect
in this code. A redirect target is fetched without passing the spider's
filter, because the spider only tests URLs it extracts from a page. Several
distinct URLs can therefore collapse onto one destination:

    https://parklogic.com/index.html   302 ->  https://parklogic.com/
    https://parklogic.com/Services     302 ->  https://parklogic.com/

Each source passes the filter legitimately, and each redirect then fetches the
same destination again. A 10 minute run spent 451 of 32,000 fetches that way,
1.4%. It costs throughput, never compliance: every fetch still goes through
the domain's timer, so ops/verify_politeness.py stays at zero.

A Bloom filter false positive can only ever drop a URL, never admit a
duplicate, so it cannot produce a finding here.

Exit code 0 means zero repeats. Until the redirect path is closed this reports
1-2% on any real run, so read the count as a rate rather than a pass/fail.
"""

from __future__ import annotations

import argparse
import glob
import sys
from collections import Counter
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from crawler.logwriter import read_lines  # noqa: E402


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("logs", nargs="*", help="crawled logs (default: data/crawled*.log.gz)")
    ap.add_argument("--show", type=int, default=20, help="max repeats to print")
    args = ap.parse_args()

    root = Path(__file__).resolve().parent.parent
    if args.logs:
        paths = [Path(p) for p in args.logs]
    else:
        paths = [Path(p) for p in sorted(glob.glob(str(root / "data" / "crawled*.log.gz")))]
    paths = [p for p in paths if p.exists()]
    if not paths:
        print("no crawled logs found", file=sys.stderr)
        return 2

    # Counting URLs rather than holding them all: a 48 hour run reaches a few
    # million fetches, which a Counter handles, unlike the ~127M discovered.
    counts: Counter[str] = Counter()
    for path in paths:
        for line in read_lines(path):
            parts = line.rstrip("\n").split("\t")
            if len(parts) >= 3:
                counts[parts[2]] += 1

    repeats = [(url, n) for url, n in counts.items() if n > 1]

    print(f"files       : {len(paths)}")
    print(f"fetches     : {sum(counts.values())}")
    print(f"distinct    : {len(counts)}")

    if repeats:
        repeats.sort(key=lambda r: -r[1])
        wasted = sum(n - 1 for _, n in repeats)
        print(f"\nREPEATS     : {len(repeats)} urls, {wasted} wasted fetches")
        for url, n in repeats[: args.show]:
            print(f"  {n}x  {url}")
        if len(repeats) > args.show:
            print(f"  ... and {len(repeats) - args.show} more")
        return 1

    print("\nREPEATS     : 0")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
