#!/usr/bin/env python3
"""Prove the bounded robots cache never weakens the robots.txt check.

RobotsTxtMiddleware keeps every parser it has built, keyed by hostname, and
never evicts. Measured sizes are 0.9 KB for a one-rule file and 369 KB for a
500-rule one, so a broad crawl reaching 500k hosts holds several GB. That is
what drove RSS to 5.3 GB in an 87 minute run.

Bounding the cache is only safe if three things hold:

  * an evicted host is re-fetched, never assumed allowed
  * concurrent requests for one host share a single fetch
  * a fetch that finishes after its cache entry was evicted still wakes
    everyone waiting on it

Run: uv run python tests/test_robots_cache.py
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from twisted.internet import asyncioreactor  # noqa: E402

asyncioreactor.install()

import asyncio  # noqa: E402

from scrapy import Request  # noqa: E402
from scrapy.exceptions import IgnoreRequest  # noqa: E402
from scrapy.http import TextResponse  # noqa: E402
from scrapy.utils.test import get_crawler  # noqa: E402

from crawler.middlewares.robots import PoliteRobotsTxtMiddleware  # noqa: E402
from crawler.spiders.broad import BroadSpider  # noqa: E402

ROBOTS_BODY = b"User-agent: *\nDisallow: /private\n"


class FakeEngine:
    """Engine stub that records robots fetches and serves a fixed body."""

    def __init__(self):
        self.fetched: list[str] = []

    async def download_async(self, request):
        self.fetched.append(request.url)
        return TextResponse(url=request.url, body=ROBOTS_BODY, encoding="utf-8")


def _middleware(cache_size: int) -> tuple[PoliteRobotsTxtMiddleware, FakeEngine]:
    crawler = get_crawler(
        BroadSpider,
        {
            "ROBOTS_CACHE_SIZE": cache_size,
            "DOWNLOAD_DELAY": 5.0,
            # The parent raises NotConfigured without this.
            "ROBOTSTXT_OBEY": True,
        },
    )
    crawler._apply_settings()
    crawler.spider = crawler._create_spider()
    mw = PoliteRobotsTxtMiddleware(crawler)
    engine = FakeEngine()
    crawler.engine = engine
    return mw, engine


def test_eviction_refetches() -> bool:
    """An evicted host must be fetched again, not treated as allowed."""
    mw, engine = _middleware(cache_size=2)

    async def run():
        for host in ("a.example", "b.example", "c.example"):
            await mw.robot_parser(Request(f"https://{host}/page"))
        # a.example was evicted by c.example. Asking again must re-fetch.
        await mw.robot_parser(Request("https://a.example/page"))

    asyncio.get_event_loop().run_until_complete(run())

    a_fetches = sum(1 for u in engine.fetched if "a.example" in u)
    if a_fetches != 2:
        print(f"FAIL: evicted host fetched {a_fetches} times, expected 2")
        return False
    print(f"PASS: eviction triggers a re-fetch ({len(engine.fetched)} fetches total)")
    return True


def test_cache_is_bounded() -> bool:
    mw, _ = _middleware(cache_size=10)

    async def run():
        for i in range(100):
            await mw.robot_parser(Request(f"https://h{i}.example/page"))

    asyncio.get_event_loop().run_until_complete(run())

    if len(mw._parsers) > 10:
        print(f"FAIL: cache holds {len(mw._parsers)}, limit was 10")
        return False
    print(f"PASS: cache bounded at {len(mw._parsers)} after 100 hosts")
    return True


def test_concurrent_requests_share_one_fetch() -> bool:
    """Ten requests for one host must produce one robots.txt fetch."""
    mw, engine = _middleware(cache_size=1000)

    async def run():
        await asyncio.gather(
            *(mw.robot_parser(Request("https://shared.example/p")) for _ in range(10))
        )

    asyncio.get_event_loop().run_until_complete(run())

    if len(engine.fetched) != 1:
        print(f"FAIL: 10 concurrent requests caused {len(engine.fetched)} fetches")
        return False
    print("PASS: concurrent requests share one fetch")
    return True


def test_inflight_is_drained() -> bool:
    """_inflight must not retain entries, or it becomes the new leak."""
    mw, _ = _middleware(cache_size=1000)

    async def run():
        for i in range(50):
            await mw.robot_parser(Request(f"https://d{i}.example/p"))

    asyncio.get_event_loop().run_until_complete(run())

    if mw._inflight:
        print(f"FAIL: _inflight retained {len(mw._inflight)} entries")
        return False
    print("PASS: _inflight fully drained")
    return True


def test_rules_are_actually_applied() -> bool:
    """The returned parser must still enforce the fetched rules."""
    mw, _ = _middleware(cache_size=1000)

    async def run():
        return await mw.robot_parser(Request("https://rules.example/p"))

    rp = asyncio.get_event_loop().run_until_complete(run())
    if rp is None:
        print("FAIL: no parser returned")
        return False
    if rp.allowed("https://rules.example/private", "*"):
        print("FAIL: disallowed path reported as allowed")
        return False
    if not rp.allowed("https://rules.example/public", "*"):
        print("FAIL: allowed path reported as disallowed")
        return False
    print("PASS: fetched rules are enforced")
    return True


class FailingEngine:
    """Engine stub whose robots.txt fetches fail, as a dead host's would."""

    def __init__(self):
        self.fetched: list[str] = []

    async def download_async(self, request):
        self.fetched.append(request.url)
        raise TimeoutError("robots.txt fetch timed out")


def _failing_middleware(**settings):
    crawler = get_crawler(
        BroadSpider,
        {"DOWNLOAD_DELAY": 5.0, "ROBOTSTXT_OBEY": True, "ROBOTS_CACHE_SIZE": 100, **settings},
    )
    crawler._apply_settings()
    crawler.spider = crawler._create_spider()
    mw = PoliteRobotsTxtMiddleware(crawler)
    engine = FailingEngine()
    crawler.engine = engine
    return mw, engine


def test_unreadable_robots_refuses() -> bool:
    """An unreadable robots.txt must block the request, not permit it.

    Scrapy's process_request_2 returns silently when the parser is None, which
    treats a failed fetch as blanket permission. A 2 hour run fetched two URLs
    under Disallow: /reports/ and Disallow: /survey on www.eia.gov exactly
    that way. Under load the failures are not rare: an earlier run timed out
    on 40,957 of 45,825 robots fetches.
    """
    mw, _ = _failing_middleware()

    async def run():
        request = Request("https://dead.example/page")
        rp = await mw.robot_parser(request)
        try:
            mw.process_request_2(rp, request)
        except IgnoreRequest:
            return True
        return False

    refused = asyncio.get_event_loop().run_until_complete(run())
    if not refused:
        print("FAIL: an unreadable robots.txt allowed the request")
        return False
    print("PASS: an unreadable robots.txt refuses the request")
    return True


def test_failure_expires() -> bool:
    """A cached failure must expire, or one timeout silences a host forever."""
    mw, engine = _failing_middleware(ROBOTS_FAILURE_TTL=0.0)

    async def run():
        await mw.robot_parser(Request("https://flaky.example/a"))
        # With a zero TTL the cached failure is already stale, so the next
        # request must try again rather than reuse it.
        await mw.robot_parser(Request("https://flaky.example/b"))

    asyncio.get_event_loop().run_until_complete(run())
    attempts = sum(1 for u in engine.fetched if "flaky.example" in u)
    if attempts != 2:
        print(f"FAIL: expired failure produced {attempts} fetches, expected 2")
        return False

    # And the opposite: within the TTL it must NOT re-fetch, or a dead host
    # would be retried on every single URL.
    mw2, engine2 = _failing_middleware(ROBOTS_FAILURE_TTL=3600.0)

    async def run2():
        await mw2.robot_parser(Request("https://dead.example/a"))
        await mw2.robot_parser(Request("https://dead.example/b"))

    asyncio.get_event_loop().run_until_complete(run2())
    held = sum(1 for u in engine2.fetched if "dead.example" in u)
    if held != 1:
        print(f"FAIL: failure within TTL produced {held} fetches, expected 1")
        return False

    print("PASS: cached failures expire after the TTL and are held within it")
    return True


def test_failure_backoff_grows() -> bool:
    """Each consecutive failure must wait longer, and the wait must be capped.

    A flat TTL re-asks a permanently dead host for the whole run: 24 retries
    in 2 hours but 576 in 48, each spending a dispatch and the domain's 5
    second slot.
    """
    mw, _ = _failing_middleware(
        ROBOTS_FAILURE_TTL=300.0, ROBOTS_FAILURE_TTL_MAX=14400.0
    )
    expected = [300.0, 600.0, 1200.0, 2400.0, 4800.0, 9600.0, 14400.0, 14400.0]
    for failures, want in enumerate(expected, start=1):
        got = mw._ttl_for(failures)
        if got != want:
            print(f"FAIL: {failures} failures gave ttl {got}, expected {want}")
            return False

    # A huge failure count must not compute 2**n before clamping.
    if mw._ttl_for(10_000) != 14400.0:
        print("FAIL: a large failure count did not clamp to the max")
        return False

    # The first failure must behave exactly as before this change, or the
    # existing TTL semantics regress.
    if mw._ttl_for(1) != 300.0 or mw._ttl_for(0) != 300.0:
        print("FAIL: the first failure no longer uses the configured ttl")
        return False

    # And the count must actually advance across real failures.
    async def run():
        await mw.robot_parser(Request("https://dead.example/a"))

    asyncio.get_event_loop().run_until_complete(run())
    entry = mw._failed_at.get("dead.example")
    if not entry or entry[1] != 1:
        print(f"FAIL: first failure recorded {entry}, expected a count of 1")
        return False

    print("PASS: the failure wait doubles per attempt and clamps at the max")
    return True


def main() -> int:
    results = [
        test_eviction_refetches(),
        test_cache_is_bounded(),
        test_concurrent_requests_share_one_fetch(),
        test_inflight_is_drained(),
        test_rules_are_actually_applied(),
        test_unreadable_robots_refuses(),
        test_failure_expires(),
        test_failure_backoff_grows(),
    ]
    return 0 if all(results) else 1


if __name__ == "__main__":
    raise SystemExit(main())
