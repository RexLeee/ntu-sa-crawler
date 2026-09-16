#!/usr/bin/env python3
"""Hold BloomDupeFilter.url_seen to Scrapy's own fingerprint.

The spider now tests a URL against the filter before it builds a Request, and
marks the resulting Request dont_filter so the scheduler does not test it
again. That is only safe while both paths compute the same value. If they ever
diverge, url_seen would record one fingerprint while the scheduler checks
another, and the crawl would fetch duplicates forever without noticing.

Run: uv run python tests/test_dupefilter.py
"""

from __future__ import annotations

import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from twisted.internet import asyncioreactor  # noqa: E402

asyncioreactor.install()

from scrapy import Request  # noqa: E402
from scrapy.utils.request import RequestFingerprinter  # noqa: E402

from crawler.config import load_config  # noqa: E402
from crawler.dupefilter import BloomDupeFilter  # noqa: E402
from crawler.filters import UrlFilter  # noqa: E402

# url_seen takes an already-canonical URL, so every URL below is passed
# through the same UrlFilter.normalize() the spider uses before it calls the
# filter. Feeding raw URLs here would test a path production never takes, and
# would hide a regression in exactly the direction that matters: normalize()
# growing a step that leaves a URL non-canonical.
_NORMALIZE = UrlFilter(load_config()).normalize


def canonical(url: str) -> str:
    """The URL as the spider would hand it to url_seen."""
    out = _NORMALIZE(url, url)
    assert out is not None, f"normalize rejected a test URL: {url}"
    return out


# Shapes a broad crawl actually meets: mixed case, default ports, fragments,
# unsorted query strings, percent-encoding, non-ASCII, dot segments, spaces.
RAW_URLS = [
    "https://example.com/",
    "https://example.com",
    "https://EXAMPLE.com/",
    "https://example.com:443/",
    "http://example.com:80/path",
    "https://example.com/a?b=2&a=1",
    "https://example.com/a?a=1&b=2",
    "https://example.com/p?z=1#frag",
    "https://example.com/%7Euser/",
    "https://example.com/~user/",
    "http://example.com/path/./x/../y",
    "https://例え.テスト/パス?q=値",
    "https://example.com/a b c",
    "https://example.com/?",
    "https://sub.example.co.uk/deep/path/here?x=1&y=2&z=3",
    "https://example.com/" + "a" * 500,
]

URLS = [canonical(u) for u in RAW_URLS]

failures = 0


def check(label: str, ok: bool, detail: str = "") -> None:
    global failures
    if not ok:
        failures += 1
    print(f"{'PASS' if ok else 'FAIL'}  {label}{(': ' + detail) if detail else ''}")


def _filter(tmp: str) -> BloomDupeFilter:
    return BloomDupeFilter(
        path=tmp,
        capacity=100_000,
        error_rate=1e-9,
        fingerprinter=RequestFingerprinter(),
    )


def main() -> int:
    fingerprinter = RequestFingerprinter()

    with tempfile.TemporaryDirectory() as tmp:
        df = _filter(tmp)

        # 1. url_seen must agree with request_seen on every URL. Run url_seen
        # on a fresh filter each time so recording does not mask a mismatch.
        mismatched = []
        for url in URLS:
            expected = fingerprinter.fingerprint(Request(url))
            with tempfile.TemporaryDirectory() as t2:
                probe = _filter(t2)
                probe.url_seen(url)
                # A fingerprint that landed in the filter makes the matching
                # Request a duplicate. That is the property the spider relies
                # on, and it is stronger than comparing digests directly.
                if not probe.request_seen(Request(url)):
                    mismatched.append(url)
                del expected
        check(
            "url_seen and request_seen agree on all URLs",
            not mismatched,
            f"{len(mismatched)} mismatched: {mismatched[:3]}",
        )

        # 2. The reverse direction, which is what a redirect exercises: the
        # scheduler records a Request, then the spider meets the same URL.
        reverse_bad = []
        for url in URLS:
            with tempfile.TemporaryDirectory() as t2:
                probe = _filter(t2)
                probe.request_seen(Request(url))
                if not probe.url_seen(url):
                    reverse_bad.append(url)
        check(
            "request_seen then url_seen reports a duplicate",
            not reverse_bad,
            f"{len(reverse_bad)} mismatched: {reverse_bad[:3]}",
        )

        # 3. First call False, second call True.
        df2 = _filter(tmp + "/x")
        first = df2.url_seen(canonical("https://first.example/page"))
        second = df2.url_seen(canonical("https://first.example/page"))
        check("url_seen records on first call", first is False and second is True)

        # 4. Distinct URLs stay distinct. At 1e-9 over 1,000 URLs a false
        # positive is far less likely than a real bug in the digest.
        df3 = _filter(tmp + "/y")
        collisions = sum(
            1
            for i in range(1000)
            if df3.url_seen(canonical(f"https://d{i}.example/p/{i}"))
        )
        check("1,000 distinct URLs produce no false duplicate", collisions == 0,
              f"{collisions} collisions")

        # 5. URLs that differ only cosmetically collapse, in both paths.
        df4 = _filter(tmp + "/z")
        df4.url_seen(canonical("https://example.com/a?b=2&a=1"))
        check(
            "cosmetic variants collapse",
            df4.url_seen(canonical("https://example.com/a?a=1&b=2#frag")),
        )

        # 5b. The precondition url_seen now relies on: normalize() returns a
        # canonical URL, so canonicalizing it again cannot change it. If this
        # ever fails, url_seen is fingerprinting a different string from the
        # one the scheduler will fingerprint, and the dupefilter silently
        # stops working.
        from w3lib.url import canonicalize_url

        not_canonical = [u for u in URLS if canonicalize_url(u) != u]
        check(
            "normalize() output is already canonical",
            not not_canonical,
            f"{len(not_canonical)} not canonical: {not_canonical[:3]}",
        )

        # 5c. A checkpoint must be loadable, and must not leave its temp file
        # behind. Until this existed only close() saved the filter, so an OOM
        # kill or a SIGKILL lost the whole run's dedupe state and the restarted
        # shard re-fetched every request in its resumed frontier.
        ckpt_dir = tmp + "/ckpt"
        df5 = BloomDupeFilter(
            path=ckpt_dir,
            capacity=100_000,
            error_rate=1e-9,
            checkpoint_interval=900,
            fingerprinter=RequestFingerprinter(),
        )
        recorded = [canonical(f"https://ck{i}.example/p") for i in range(50)]
        for url in recorded:
            df5.url_seen(url)
        df5.checkpoint()

        saved = Path(ckpt_dir) / "requests.bloom"
        check("checkpoint writes the filter", saved.exists())
        leftover = list(Path(ckpt_dir).glob("*.tmp"))
        check("checkpoint leaves no temp file", not leftover, str(leftover[:2]))

        # A fresh instance on the same path is what a restart gets.
        reloaded = BloomDupeFilter(
            path=ckpt_dir,
            capacity=100_000,
            error_rate=1e-9,
            fingerprinter=RequestFingerprinter(),
        )
        missing = [u for u in recorded if not reloaded.url_seen(u)]
        check(
            "a restart sees every url the checkpoint held",
            not missing,
            f"{len(missing)} of {len(recorded)} lost",
        )

        del df

    # 6. The spider must actually hold the filter. Crawler.crawl builds the
    # spider before the engine, and the engine is what builds the scheduler
    # and its dupefilter, so reading crawler.bloom_dupefilter in the spider's
    # from_crawler yields None. That failed silently: the crawl ran correctly
    # but did all the work the filter was meant to avoid. A run showed it only
    # as dupe_skipped stuck at 0 against 768,922 discovered URLs.
    check("spider binds the dupefilter at spider_opened", _spider_binds_filter())


def _spider_binds_filter() -> bool:
    from scrapy.utils.misc import build_from_crawler
    from scrapy.utils.test import get_crawler

    from crawler.spiders.broad import BroadSpider

    with tempfile.TemporaryDirectory() as tmp:
        crawler = get_crawler(
            BroadSpider,
            {
                "JOBDIR": tmp,
                "DUPEFILTER_CLASS": "crawler.dupefilter.BloomDupeFilter",
                "BLOOM_DUPEFILTER_CAPACITY": 10_000,
                "SCHEDULER_PRIORITY_QUEUE": "scrapy.pqueues.DownloaderAwarePriorityQueue",
                "CONCURRENT_REQUESTS_PER_IP": 0,
            },
        )
        crawler._apply_settings()
        # This order is the point of the test: Crawler.crawl creates the
        # spider first, and only then the engine, which builds the scheduler
        # and with it the dupefilter.
        spider = crawler._create_spider()
        crawler.engine = crawler._create_engine()
        scheduler = build_from_crawler(crawler.engine.scheduler_cls, crawler)
        scheduler.open(spider)

        spider._bind_dupefilter(spider)
        bound = spider.dupefilter is not None
        if bound:
            # It must be the live instance, not a second filter of its own.
            bound = spider.dupefilter is crawler.bloom_dupefilter
        scheduler.close("test")
        return bound

    print()
    if failures:
        print(f"{failures} check(s) failed")
        return 1
    print("all checks passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
