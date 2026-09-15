#!/usr/bin/env python3
"""Keep only the seeds that actually serve a page with links.

The keyword filter in build_seeds.py catches obvious infrastructure names, but
it cannot catch every one: aaplimg.com, appsflyersdk.com and
googleusercontent.com all rank high, resolve fine, and return nothing a
crawler can follow. A seed like that wastes one of the 1,000 slots, and each
slot is worth a whole 5 second pipeline for 48 hours.

This fetches each candidate once and keeps it only if the response is HTML
with at least MIN_LINKS links. It is the same test the crawler applies, run
ahead of time.

Politeness: one request per registered domain, so no domain is hit twice and
the 0.2 qps rule cannot be broken by this script. Concurrency is across
domains, never within one.

Usage:
    uv run python seeds/probe.py --in seeds/candidates.txt --out seeds/seeds_1000.txt
"""

from __future__ import annotations

import argparse
import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from crawler.config import load_config  # noqa: E402
from crawler.extract import iter_hrefs  # noqa: E402
from crawler.filters import UrlFilter  # noqa: E402
from crawler.slot import slot_key  # noqa: E402

MIN_LINKS = 5
# 512 KB matches the crawler's own download_maxsize. Some homepages carry a
# 24 KB <head> before the first <a>, so a smaller window judges them linkless.
READ_LIMIT = 524_288


async def probe(url: str, session, ufilter: UrlFilter, timeout: float) -> tuple[str, int, str]:
    """Return (url, usable link count, note). A count of 0 means reject."""
    try:
        async with session.get(url, timeout=timeout, allow_redirects=True) as resp:
            ctype = resp.headers.get("Content-Type", "").lower()
            if "html" not in ctype:
                return url, 0, f"not html ({ctype.split(';')[0] or 'none'})"
            # content.read(n) returns whatever one chunk holds, not n bytes.
            # A single chunk of github.com is 24 KB of <head> with no <a> in
            # it, which made every such site look linkless. readexactly is
            # wrong too, since it raises on a short final chunk.
            body = await resp.content.read(READ_LIMIT)
            while len(body) < READ_LIMIT:
                chunk = await resp.content.read(READ_LIMIT - len(body))
                if not chunk:
                    break
                body += chunk
    except TimeoutError:
        return url, 0, "timeout"
    except Exception as e:
        return url, 0, type(e).__name__

    # Count links the crawler would actually follow, not raw hrefs.
    base = str(resp.url)
    seen: set[str] = set()
    for href in iter_hrefs(body):
        normalized = ufilter.normalize(href, base)
        if normalized:
            seen.add(normalized)
    return url, len(seen), "ok"


async def run(urls: list[str], concurrency: int, timeout: float) -> list[tuple[str, int, str]]:
    import aiohttp

    cfg = load_config()
    ufilter = UrlFilter(cfg)
    headers = {"User-Agent": cfg["identity"]["user_agent"]}

    sem = asyncio.Semaphore(concurrency)
    connector = aiohttp.TCPConnector(limit=concurrency, ttl_dns_cache=300)

    async with aiohttp.ClientSession(headers=headers, connector=connector) as session:

        async def bounded(url: str):
            async with sem:
                return await probe(url, session, ufilter, timeout)

        results = []
        tasks = [asyncio.create_task(bounded(u)) for u in urls]
        for i, task in enumerate(asyncio.as_completed(tasks), 1):
            results.append(await task)
            if i % 100 == 0:
                kept = sum(1 for _, n, _ in results if n >= MIN_LINKS)
                print(f"  probed {i}/{len(urls)}, keeping {kept}", file=sys.stderr)
        return results


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--in", dest="infile", default="seeds/candidates.txt")
    ap.add_argument("--out", default="seeds/seeds_1000.txt")
    ap.add_argument("--count", type=int, default=1000)
    ap.add_argument("--concurrency", type=int, default=100)
    ap.add_argument("--timeout", type=float, default=10.0)
    ap.add_argument("--report", default="", help="write the full probe result here")
    args = ap.parse_args()

    root = Path(__file__).resolve().parent.parent
    lines = (root / args.infile).read_text(encoding="utf-8").splitlines()
    urls = [ln.strip() for ln in lines if ln.strip() and not ln.startswith("#")]

    # One probe per registered domain keeps this script itself polite.
    by_domain: dict[str, str] = {}
    for url in urls:
        by_domain.setdefault(slot_key(url), url)
    urls = list(by_domain.values())
    print(f"probing {len(urls):,} distinct domains", file=sys.stderr)

    results = asyncio.run(run(urls, args.concurrency, args.timeout))

    # Preserve the candidate file's ordering, which is Tranco rank.
    rank = {u: i for i, u in enumerate(urls)}
    kept = sorted(
        [(u, n) for u, n, _ in results if n >= MIN_LINKS],
        key=lambda t: rank[t[0]],
    )[: args.count]

    out = root / args.out
    with out.open("w", encoding="utf-8") as fh:
        fh.write(f"# {len(kept)} seeds, each verified to return HTML with >= {MIN_LINKS} links\n")
        fh.write("# Built by seeds/build_seeds.py, verified by seeds/probe.py\n")
        for url, _ in kept:
            fh.write(f"{url}\n")

    if args.report:
        with (root / args.report).open("w", encoding="utf-8") as fh:
            for url, n, note in sorted(results, key=lambda t: -t[1]):
                fh.write(f"{n}\t{note}\t{url}\n")

    rejected = len(results) - sum(1 for _, n, _ in results if n >= MIN_LINKS)
    notes: dict[str, int] = {}
    for _, n, note in results:
        if n < MIN_LINKS:
            notes[note] = notes.get(note, 0) + 1
    top = sorted(notes.items(), key=lambda kv: -kv[1])[:6]

    print(f"kept {len(kept):,} of {len(results):,} probed", file=sys.stderr)
    print(f"rejected {rejected:,}: {', '.join(f'{k}={v}' for k, v in top)}", file=sys.stderr)
    print(f"wrote {out}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
