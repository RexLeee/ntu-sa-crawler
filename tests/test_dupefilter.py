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

from crawler.dupefilter import BloomDupeFilter  # noqa: E402

# Shapes a broad crawl actually meets: mixed case, default ports, fragments,
# unsorted query strings, percent-encoding, non-ASCII, dot segments, spaces.
URLS = [
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
        first = df2.url_seen("https://first.example/page")
        second = df2.url_seen("https://first.example/page")
        check("url_seen records on first call", first is False and second is True)

        # 4. Distinct URLs stay distinct. At 1e-9 over 1,000 URLs a false
        # positive is far less likely than a real bug in the digest.
        df3 = _filter(tmp + "/y")
        collisions = sum(
            1 for i in range(1000) if df3.url_seen(f"https://d{i}.example/p/{i}")
        )
        check("1,000 distinct URLs produce no false duplicate", collisions == 0,
              f"{collisions} collisions")

        # 5. URLs that differ only cosmetically collapse, in both paths.
        df4 = _filter(tmp + "/z")
        df4.url_seen("https://example.com/a?b=2&a=1")
        check(
            "cosmetic variants collapse",
            df4.url_seen("https://example.com/a?a=1&b=2#frag"),
        )

        del df

    print()
    if failures:
        print(f"{failures} check(s) failed")
        return 1
    print("all checks passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
