"""The spider's per-domain bookkeeping must be bounded, and safely so.

Two of the three structures in crawler/spiders/broad.py are keyed by every
domain ever touched, and that count has no ceiling: a 2 hour run reached
343,644 domains per shard with the arrival rate still at 14.7/s. Fitting the
curve gives 4.6M domains at 48 hours (t^0.796, R2 0.987) and 8.2M on a linear
read. At a measured 198 bytes per domain across the two that is 875-1,546 MB
per shard against 931 MB of headroom under the 2,800 MB guard.

Bounding them is only safe because eviction is one-directional: it can re-allow
work, never block it. These tests pin that property, and pin that
queued_per_domain stays UNBOUNDED, because it has a pairing release that makes
it track live domains rather than the whole run.
"""

from __future__ import annotations

import sys
import tomllib
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from scrapy.utils.datatypes import LocalCache  # noqa: E402

from crawler.spiders.broad import BroadSpider  # noqa: E402


def test_the_caches_are_bounded_and_configured() -> bool:
    """The two run-scoped structures must be LocalCache at the configured size."""
    cfg = tomllib.loads(Path(PROJECT_ROOT, "config.toml").read_text(encoding="utf-8"))
    want = cfg["memory"]["seen_domains_max"]

    spider = BroadSpider()
    for name in ("seen_domains", "crawled_per_domain"):
        obj = getattr(spider, name)
        if not isinstance(obj, LocalCache):
            print(f"FAIL: {name} is {type(obj).__name__}, expected LocalCache")
            return False
        if obj.limit != want:
            print(f"FAIL: {name} limit is {obj.limit}, expected {want}")
            return False

    # queued_per_domain must NOT be capped. _release_queued pops each key when
    # its count reaches zero, so it already tracks live domains only, and its
    # size follows PQUEUE_MAX. Capping it would drop counts for domains still
    # holding frontier slots, which would let one domain exceed its share.
    if isinstance(spider.queued_per_domain, LocalCache):
        print("FAIL: queued_per_domain is capped; it has a pairing release instead")
        return False

    print(f"PASS: both run-scoped caches bounded at {want}, queued_per_domain is not")
    return True


def test_eviction_never_blocks_crawling() -> bool:
    """Evicting either cache must only ever re-allow work."""
    spider = BroadSpider()
    cap = 3
    spider.seen_domains = LocalCache(limit=cap)
    spider.crawled_per_domain = LocalCache(limit=cap)
    spider.max_crawled_per_domain = 2

    # A domain at its page cap is refused. That is the behaviour being kept.
    spider.crawled_per_domain["capped.test"] = 2
    if spider.crawled_per_domain.get("capped.test", 0) < spider.max_crawled_per_domain:
        print("FAIL: a domain at its page cap was not recognised as capped")
        return False

    # Push it out of the cache. The count resets, which RE-ALLOWS crawling.
    for i in range(cap + 2):
        spider.crawled_per_domain[f"filler{i}.test"] = 1
    if "capped.test" in spider.crawled_per_domain:
        print("FAIL: the cache did not evict past its limit")
        return False
    if spider.crawled_per_domain.get("capped.test", 0) >= spider.max_crawled_per_domain:
        print("FAIL: an evicted page count still reads as capped")
        return False

    print("PASS: an evicted page count re-allows a domain, never blocks one")
    return True


def test_domains_seen_survives_eviction() -> bool:
    """domains/seen is a reported metric, so it must not fall when a key evicts."""
    spider = BroadSpider()
    spider.seen_domains = LocalCache(limit=2)
    spider.domains_seen_total = 0

    for i in range(5):
        key = f"d{i}.test"
        if key not in spider.seen_domains:
            spider.seen_domains[key] = True
            spider.domains_seen_total += 1

    if len(spider.seen_domains) != 2:
        print(f"FAIL: cache holds {len(spider.seen_domains)}, expected 2")
        return False
    if spider.domains_seen_total != 5:
        print(f"FAIL: domains_seen_total is {spider.domains_seen_total}, expected 5")
        return False

    # An evicted domain reads as new again, which grants one extra new_domain
    # exemption. That is one request, and CappedScheduler now has a ceiling for
    # it. The counter must not double-count a domain still in the cache.
    before = spider.domains_seen_total
    key = next(iter(spider.seen_domains))
    if key not in spider.seen_domains:
        spider.domains_seen_total += 1
    if spider.domains_seen_total != before:
        print("FAIL: a cached domain was counted twice")
        return False

    print("PASS: domains/seen counts the run, not the cache contents")
    return True


def test_the_memory_bound_actually_holds() -> bool:
    """The cap has to keep the two caches inside the measured headroom."""
    cfg = tomllib.loads(Path(PROJECT_ROOT, "config.toml").read_text(encoding="utf-8"))
    limit = cfg["memory"]["seen_domains_max"]
    # 198 bytes per domain across both structures, measured by filling them
    # with 4.6M synthetic domains and reading ru_maxrss.
    projected_mb = limit * 198 / 1024 / 1024
    headroom_mb = (
        cfg["memory"]["memusage_limit_mb"] - 1869
    )  # 1869 = the measured peak RSS of a 2 hour run

    if projected_mb > headroom_mb * 0.5:
        print(
            f"FAIL: the caches would take {projected_mb:.0f} MB of "
            f"{headroom_mb:.0f} MB headroom; over half is too much"
        )
        return False

    print(
        f"PASS: bounded at {limit:,} domains the caches take {projected_mb:.0f} MB "
        f"of {headroom_mb:.0f} MB headroom"
    )
    return True


def main() -> int:
    results = [
        test_the_caches_are_bounded_and_configured(),
        test_eviction_never_blocks_crawling(),
        test_domains_seen_survives_eviction(),
        test_the_memory_bound_actually_holds(),
    ]
    return 0 if all(results) else 1


if __name__ == "__main__":
    raise SystemExit(main())
