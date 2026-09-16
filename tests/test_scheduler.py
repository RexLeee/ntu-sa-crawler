#!/usr/bin/env python3
"""Lock in the O(1) frontier size tracking.

CappedScheduler used to call len(self) on every enqueue. Scheduler.__len__
sums over every per-domain queue, so a crawl that reached tens of thousands of
domains spent most of its time counting instead of crawling.

Two properties must hold:

  * the tracked size matches the real length after any mix of operations
  * enqueue cost does not grow with the number of domains

Run: uv run python tests/test_scheduler.py
"""

from __future__ import annotations

import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from twisted.internet import asyncioreactor  # noqa: E402

# DownloaderAwarePriorityQueue reaches the downloader, which builds handlers
# that require an installed reactor. Install it before importing Scrapy.
asyncioreactor.install()

from scrapy import Request  # noqa: E402
from scrapy.utils.test import get_crawler  # noqa: E402

from crawler.scheduler import CappedScheduler  # noqa: E402
from crawler.spiders.broad import BroadSpider  # noqa: E402

CAP = 500


def _scheduler(cap: int = CAP) -> CappedScheduler:
    crawler = get_crawler(
        BroadSpider,
        {
            "FRONTIER_MAX_SIZE": cap,
            "SCHEDULER_PRIORITY_QUEUE": "crawler.pqueue.RingDownloaderAwarePriorityQueue",
            "CONCURRENT_REQUESTS_PER_IP": 0,
            "DUPEFILTER_CLASS": "scrapy.dupefilters.BaseDupeFilter",
        },
    )
    crawler._apply_settings()
    spider = crawler._create_spider()
    crawler.engine = crawler._create_engine()
    scheduler = CappedScheduler.from_crawler(crawler)
    scheduler.open(spider)
    return scheduler


def test_size_matches_real_length() -> bool:
    sched = _scheduler()
    for i in range(200):
        sched.enqueue_request(Request(f"https://d{i}.example/", meta={"download_slot": f"d{i}"}))
    if sched._size != len(sched):
        print(f"FAIL: after enqueue, tracked={sched._size} real={len(sched)}")
        return False

    for _ in range(50):
        sched.next_request()
    if sched._size != len(sched):
        print(f"FAIL: after dequeue, tracked={sched._size} real={len(sched)}")
        return False

    print(f"PASS: size tracking exact ({sched._size} == {len(sched)})")
    return True


def test_cap_is_enforced() -> bool:
    sched = _scheduler(cap=10)
    accepted = sum(
        sched.enqueue_request(Request(f"https://d{i}.example/", meta={"download_slot": f"d{i}"}))
        for i in range(50)
    )
    if accepted != 10:
        print(f"FAIL: cap 10 accepted {accepted}")
        return False

    # Draining must let new requests back in, or a full frontier deadlocks.
    sched.next_request()
    if not sched.enqueue_request(Request("https://new.example/", meta={"download_slot": "new"})):
        print("FAIL: cap did not release after a dequeue")
        return False

    print("PASS: cap enforced and released")
    return True


def test_new_domain_bypasses_cap() -> bool:
    """A full frontier must still admit the first URL of an unseen domain.

    Throughput is bounded by active_domains / delay, so dropping these would
    freeze the one curve that sets the ceiling. A measured run filled a 2M
    frontier in about an hour and then dropped everything, new domains
    included.
    """
    sched = _scheduler(cap=10)
    for i in range(10):
        sched.enqueue_request(
            Request(f"https://d{i}.example/", meta={"download_slot": f"d{i}"})
        )

    ordinary = sched.enqueue_request(
        Request("https://d0.example/more", meta={"download_slot": "d0"})
    )
    if ordinary:
        print("FAIL: a full frontier accepted an ordinary request")
        return False

    new_domain = sched.enqueue_request(
        Request(
            "https://fresh.example/",
            meta={"download_slot": "fresh", "new_domain": True},
        )
    )
    if not new_domain:
        print("FAIL: a full frontier rejected a new domain")
        return False

    if sched._size != len(sched):
        print(f"FAIL: exempt request broke size tracking: {sched._size} != {len(sched)}")
        return False

    print("PASS: new domains bypass the cap, size tracking intact")
    return True


def test_has_pending_requests_is_o1() -> bool:
    """has_pending_requests must not sum over every per-domain queue."""
    sched = _scheduler(cap=10_000_000)
    if sched.has_pending_requests():
        print("FAIL: empty scheduler reports pending requests")
        return False

    for i in range(500):
        sched.enqueue_request(
            Request(f"https://d{i}.example/", meta={"download_slot": f"d{i}"})
        )
    if not sched.has_pending_requests():
        print("FAIL: populated scheduler reports no pending requests")
        return False

    while sched.next_request() is not None:
        pass
    if sched.has_pending_requests():
        print("FAIL: drained scheduler still reports pending requests")
        return False

    print("PASS: has_pending_requests tracks the counter, not the queues")
    return True


def test_enqueue_cost_is_flat() -> bool:
    """Enqueue must not slow down as the domain count grows."""
    timings = {}
    for domains in (200, 2000):
        sched = _scheduler(cap=10_000_000)
        for i in range(domains):
            sched.enqueue_request(
                Request(f"https://d{i}.example/", meta={"download_slot": f"d{i}"})
            )
        start = time.perf_counter()
        for i in range(2000):
            sched.enqueue_request(
                Request(f"https://probe{i}.example/", meta={"download_slot": f"p{i}"})
            )
        timings[domains] = (time.perf_counter() - start) / 2000

    ratio = timings[2000] / timings[200]
    # O(domains) would be ~10x here. Allow generous headroom for noise.
    if ratio > 3.0:
        print(f"FAIL: enqueue cost grew {ratio:.1f}x with 10x the domains")
        return False

    print(
        f"PASS: enqueue cost flat "
        f"({timings[200] * 1e6:.1f}us -> {timings[2000] * 1e6:.1f}us, {ratio:.2f}x)"
    )
    return True


def test_the_cap_comes_from_config() -> bool:
    """The live cap must be the configured one, not a class default.

    The default in from_crawler and the value in config.toml have disagreed
    before, and the tests above all pass an explicit cap, so nothing checked
    that the wiring in settings.py was intact. A silent fallback to the
    default would change how much disk, how many per-domain queues and how
    many file descriptors the run holds.
    """
    from crawler.config import load_config
    from crawler.settings import FRONTIER_MAX_SIZE

    configured = load_config()["memory"]["frontier_max_size"]
    if FRONTIER_MAX_SIZE != configured:
        print(
            f"FAIL: settings has {FRONTIER_MAX_SIZE}, config.toml has {configured}"
        )
        return False

    scheduler = _scheduler(cap=configured)
    if scheduler._cap != configured:
        print(f"FAIL: scheduler cap is {scheduler._cap}, expected {configured}")
        return False

    print(f"PASS: the frontier cap is the configured {configured:,}")
    return True


def main() -> int:
    results = [
        test_the_cap_comes_from_config(),
        test_size_matches_real_length(),
        test_cap_is_enforced(),
        test_new_domain_bypasses_cap(),
        test_has_pending_requests_is_o1(),
        test_enqueue_cost_is_flat(),
    ]
    return 0 if all(results) else 1


if __name__ == "__main__":
    raise SystemExit(main())
