#!/usr/bin/env python3
"""Build the seed list from the Tranco top sites ranking.

Throughput is bounded by the number of domains in flight, so the seed list's
job is to open as many independent 5 second pipelines as the assignment
allows. Raw rank order is a poor way to do that: the top of the list is
dominated by CDN and API hostnames that serve no HTML, and by .com.

Three rules shape the selection.

1. Drop infrastructure hosts. About 4% of the top 3,000 are names like
   gstatic.com or akamai.net, which return no links to follow.
2. One URL per registered domain, since the crawler rate-limits on eTLD+1 and
   a second host on the same domain buys no extra parallelism.
3. Sample across TLDs rather than taking a prefix. The top 3,000 is 55% .com;
   a prefix would concentrate the crawl in one slice of the web.

Tranco is a research ranking that averages four commercial lists over 30 days
to resist manipulation, which makes it citable in the report.

Usage:
    uv run python seeds/build_seeds.py --out seeds/seeds_1000.txt
"""

from __future__ import annotations

import argparse
import csv
import io
import sys
import urllib.request
import zipfile
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from crawler.slot import slot_key  # noqa: E402

TRANCO_URL = "https://tranco-list.eu/top-1m.csv.zip"

# Substrings that mark a hostname as infrastructure. These resolve and answer,
# but serve assets, APIs or DNS rather than pages with links.
INFRASTRUCTURE_MARKERS = (
    "gstatic",
    "akamai",
    "googleapis",
    "amazonaws",
    "cloudfront",
    "fbcdn",
    "doubleclick",
    "googlesyndication",
    "googletagmanager",
    "google-analytics",
    "gvt1",
    "gvt2",
    "cloudflare-dns",
    "azureedge",
    "windowsupdate",
    "root-servers",
    "in-addr",
    "ntp.org",
    "llnwd",
    "edgekey",
    "edgesuite",
    "cdn77",
    "fastly",
    "trafficmanager",
    "cloudapp",
    "elasticbeanstalk",
    # Authoritative and registry nameservers. They rank high because every
    # lookup touches them, but they serve no pages.
    "gtld-servers",
    "nstld",
    "-ns.",
    "ns1.",
    "ns2.",
    "dnsnode",
    "nic.",
    "domaincontrol",
    "googlevideo",
    "ytimg",
    "gvt0",
    "cloudsink",
    "cdninstagram",
    "licdn",
    "twimg",
    "sharepoint",
    "office365",
    "azure-dns",
    "awsdns",
)


def fetch_tranco(cache: Path) -> list[str]:
    """Return ranked domains, downloading the list once and caching it."""
    if not cache.exists():
        print(f"downloading {TRANCO_URL}", file=sys.stderr)
        cache.parent.mkdir(parents=True, exist_ok=True)
        with urllib.request.urlopen(TRANCO_URL, timeout=120) as resp:
            cache.write_bytes(resp.read())

    with zipfile.ZipFile(cache) as zf:
        name = zf.namelist()[0]
        with zf.open(name) as fh:
            reader = csv.reader(io.TextIOWrapper(fh, encoding="utf-8"))
            return [row[1] for row in reader if len(row) >= 2]


def is_infrastructure(domain: str) -> bool:
    return any(marker in domain for marker in INFRASTRUCTURE_MARKERS)


def select(domains: list[str], count: int, pool: int, max_share: float = 0.35) -> list[str]:
    """Pick `count` domains from the top `pool`, spread across TLDs.

    Each TLD gets a quota proportional to its share of the pool, so .com still
    contributes the most, capped at `max_share` of the list so one TLD cannot
    crowd out the rest. Leftover capacity goes back to the largest buckets in
    rank order.

    Pure round-robin was tried first and over-corrected: 448 TLDs each got 4
    slots and .com ended up with 4, which is not what the web looks like.
    """
    by_tld: dict[str, list[str]] = defaultdict(list)
    seen_registered: set[str] = set()

    for domain in domains[:pool]:
        if is_infrastructure(domain):
            continue
        # The crawler rate-limits per eTLD+1, so two hosts on one registered
        # domain share a timer and add no parallelism.
        registered = slot_key(f"https://{domain}/")
        if registered in seen_registered:
            continue
        seen_registered.add(registered)
        by_tld[domain.rsplit(".", 1)[-1]].append(domain)

    total = sum(len(v) for v in by_tld.values())
    if not total:
        return []

    cap = max(1, int(count * max_share))
    order = sorted(by_tld, key=lambda t: -len(by_tld[t]))

    # Proportional quota, but every TLD present gets at least one slot so the
    # long tail is represented.
    quota = {t: min(cap, max(1, round(count * len(by_tld[t]) / total))) for t in order}

    picked: list[str] = []
    for tld in order:
        picked.extend(by_tld[tld][: quota[tld]])
        if len(picked) >= count:
            return picked[:count]

    # Under quota because of rounding: top up from the biggest buckets.
    taken = {t: quota[t] for t in order}
    depth_exhausted = False
    while len(picked) < count and not depth_exhausted:
        depth_exhausted = True
        for tld in order:
            if taken[tld] < len(by_tld[tld]):
                picked.append(by_tld[tld][taken[tld]])
                taken[tld] += 1
                depth_exhausted = False
                if len(picked) == count:
                    return picked
    return picked


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="seeds/candidates.txt")
    # Overshoot by default: seeds/probe.py rejects the candidates that serve no
    # followable links, and the assignment allows 1,000 survivors.
    ap.add_argument("--count", type=int, default=2000, help="assignment allows up to 1000 seeds")
    ap.add_argument("--pool", type=int, default=20_000, help="ranks to choose from")
    ap.add_argument("--cache", default="seeds/.tranco-top-1m.csv.zip")
    args = ap.parse_args()

    root = Path(__file__).resolve().parent.parent
    domains = fetch_tranco(root / args.cache)
    print(f"ranked domains available : {len(domains):,}", file=sys.stderr)

    picked = select(domains, args.count, args.pool)
    if len(picked) < args.count:
        print(f"warning: only {len(picked)} selected from a pool of {args.pool}", file=sys.stderr)

    out = root / args.out
    out.parent.mkdir(parents=True, exist_ok=True)
    with out.open("w", encoding="utf-8") as fh:
        fh.write(f"# {len(picked)} candidates from Tranco top {args.pool}\n")
        fh.write("# Built by seeds/build_seeds.py: infrastructure hosts dropped,\n")
        fh.write("# one URL per registered domain, sampled across TLDs.\n")
        fh.write("# Run seeds/probe.py next to keep only those serving real links.\n")
        for domain in picked:
            fh.write(f"https://{domain}/\n")

    tlds = defaultdict(int)
    for d in picked:
        tlds[d.rsplit(".", 1)[-1]] += 1
    top = sorted(tlds.items(), key=lambda kv: -kv[1])[:10]

    print(f"wrote {len(picked):,} seeds to {out}", file=sys.stderr)
    print(f"distinct TLDs : {len(tlds)}", file=sys.stderr)
    print(f"top TLDs      : {', '.join(f'{t}={n}' for t, n in top)}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
