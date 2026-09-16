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


def _disk_scheduler(jobdir: str, cap: int = 10_000_000, queue_cap: int = 40_000,
                    checkpoint: float = 0.0) -> CappedScheduler:
    """A scheduler with a real JOBDIR, so self.dqs and the disk queues exist.

    The helper above deliberately has none, which leaves dqs at None: the
    resume, queue-cap and checkpoint paths all need the disk side.
    """
    crawler = get_crawler(
        BroadSpider,
        {
            "FRONTIER_MAX_SIZE": cap,
            "PQUEUE_MAX": queue_cap,
            "CHECKPOINT_INTERVAL": checkpoint,
            "JOBDIR": jobdir,
            "SCHEDULER_PRIORITY_QUEUE": "crawler.pqueue.RingDownloaderAwarePriorityQueue",
            "SCHEDULER_DISK_QUEUE": "crawler.squeue.RecoverablePickleFifoDiskQueue",
            "CONCURRENT_REQUESTS_PER_IP": 0,
            "DUPEFILTER_CLASS": "scrapy.dupefilters.BaseDupeFilter",
        },
    )
    crawler._apply_settings()
    spider = crawler._create_spider()
    crawler.spider = spider
    crawler.engine = crawler._create_engine()
    scheduler = CappedScheduler.from_crawler(crawler)
    scheduler.open(spider)
    return scheduler


def test_a_killed_frontier_is_reloaded() -> bool:
    """The whole point: a shard killed with work queued must resume it.

    Scrapy writes requests.queue/active.json only from Scheduler.close(), so
    an OOM-killed shard used to resume with zero queues. Worse, an empty queue
    gets closed and a closed empty queue deletes its chunks, so resuming
    destroyed the frontier instead of ignoring it. A measured run lost 2.6 GB
    and 3,003,701 queued requests that way, then exited in 1.66 seconds.
    """
    import shutil
    import tempfile

    tmp = tempfile.mkdtemp()
    try:
        jobdir = str(Path(tmp, "job"))
        sched = _disk_scheduler(jobdir)
        for i in range(60):
            sched.enqueue_request(
                Request(f"https://d{i % 12}.example/p{i}",
                        meta={"download_slot": f"d{i % 12}"})
            )
        # Persist the per-queue metadata the way a live run's timer would,
        # then abandon the scheduler without close(): no active.json is
        # written, exactly as after a SIGKILL.
        sched.checkpoint()
        queued = sched._size
        del sched

        active = Path(jobdir, "requests.queue", "active.json")
        if active.exists():
            print("FAIL: the setup wrote active.json, so this is not a kill")
            return False

        resumed = _disk_scheduler(jobdir)
        if resumed._size != queued:
            print(f"FAIL: resumed {resumed._size} of {queued} queued requests")
            return False
        if not active.exists():
            print("FAIL: open() did not rebuild active.json")
            return False

        seen = set()
        while (req := resumed.next_request()) is not None:
            seen.add(req.url)
        if len(seen) != queued:
            print(f"FAIL: drained {len(seen)} distinct urls, expected {queued}")
            return False

        print(f"PASS: all {queued} queued requests survived a kill")
        return True
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


def test_the_queue_cap_bounds_domains() -> bool:
    """PQUEUE_MAX must refuse new domains while still serving held ones.

    The request cap alone was not enough: a measured 2 hour run pinned the
    frontier at 3,000,810 requests while the per-domain queue count still grew
    from 46,788 to 132,779, because a new domain's first URL is exempt. Each
    queue costs two descriptors and a directory, so that count is what
    exhausted memory.
    """
    import shutil
    import tempfile

    tmp = tempfile.mkdtemp()
    try:
        sched = _disk_scheduler(str(Path(tmp, "job")), queue_cap=5)
        for i in range(12):
            sched.enqueue_request(
                Request(f"https://d{i}.example/",
                        meta={"download_slot": f"d{i}", "new_domain": True})
            )
        live = len(sched.dqs.pqueues)
        if live != 5:
            print(f"FAIL: {live} domain queues exist, cap was 5")
            return False

        # A held domain must still accept work, or the crawl stalls on the
        # domains it already owns.
        if not sched.enqueue_request(
            Request("https://d0.example/second", meta={"download_slot": "d0"})
        ):
            print("FAIL: the queue cap refused a domain already held")
            return False

        dropped = sched.stats.get_value("scheduler/dropped/over_queues", 0)
        if dropped != 7:
            print(f"FAIL: counted {dropped} refusals, expected 7")
            return False

        print(f"PASS: queue cap held 5 domains and refused {dropped} new ones")
        return True
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


def test_checkpoint_writes_every_queue() -> bool:
    """Each live queue must have metadata on disk after a checkpoint."""
    import json
    import shutil
    import tempfile

    tmp = tempfile.mkdtemp()
    try:
        jobdir = str(Path(tmp, "job"))
        sched = _disk_scheduler(jobdir)
        for i in range(8):
            sched.enqueue_request(
                Request(f"https://d{i}.example/", meta={"download_slot": f"d{i}"})
            )
        sched.checkpoint()

        infos = list(Path(jobdir, "requests.queue").glob("*/*/info.json"))
        if len(infos) != 8:
            print(f"FAIL: {len(infos)} info.json files for 8 queues")
            return False
        # clean=true would tell the next open to trust the metadata without
        # scanning. A checkpoint is not a close, so it must not claim that.
        flagged = [p for p in infos if json.loads(p.read_text()).get("clean")]
        if flagged:
            print(f"FAIL: {len(flagged)} checkpoints claimed a clean close")
            return False

        recorded = sched.stats.get_value("checkpoint/frontier_queues", 0)
        if recorded != 8:
            print(f"FAIL: stats recorded {recorded} queues, expected 8")
            return False

        print("PASS: checkpoint wrote all 8 queues without a clean marker")
        return True
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


def test_slot_names_round_trip() -> bool:
    """Directory names must invert to slots, or a domain gets two queues."""
    from scrapy.pqueues import _path_safe

    for slot in ("example.com", "a-b.co.uk", "blogspot.com", "1.2.3.4", "x.io"):
        name = _path_safe(slot)
        back = CappedScheduler._slot_from_dirname(name)
        if back != slot:
            print(f"FAIL: {slot!r} -> {name!r} -> {back!r}")
            return False

    # A name that is not a _path_safe output must be refused rather than
    # guessed at, because a wrong slot name would key a second queue for the
    # same domain and break the shared 5 second timer.
    for bogus in ("nohyphen", "bad-" + "0" * 32, "trailing-", "x-" + "0" * 31):
        if CappedScheduler._slot_from_dirname(bogus) is not None:
            print(f"FAIL: accepted a non-_path_safe name {bogus!r}")
            return False

    print("PASS: slot names invert, and unreadable ones are refused")
    return True


def test_the_caps_come_from_config() -> bool:
    """settings.py must expose the configured queue cap and interval."""
    from crawler.config import load_config
    from crawler.settings import CHECKPOINT_INTERVAL, PQUEUE_MAX

    mem = load_config()["memory"]
    if PQUEUE_MAX != mem["pqueue_max"]:
        print(f"FAIL: PQUEUE_MAX {PQUEUE_MAX} != config {mem['pqueue_max']}")
        return False
    if CHECKPOINT_INTERVAL != mem["checkpoint_interval"]:
        print(
            f"FAIL: CHECKPOINT_INTERVAL {CHECKPOINT_INTERVAL} "
            f"!= config {mem['checkpoint_interval']}"
        )
        return False
    print(
        f"PASS: queue cap {PQUEUE_MAX:,} and checkpoint "
        f"{CHECKPOINT_INTERVAL:.0f}s come from config"
    )
    return True


def main() -> int:
    results = [
        test_the_cap_comes_from_config(),
        test_the_caps_come_from_config(),
        test_size_matches_real_length(),
        test_cap_is_enforced(),
        test_new_domain_bypasses_cap(),
        test_has_pending_requests_is_o1(),
        test_enqueue_cost_is_flat(),
        test_slot_names_round_trip(),
        test_a_killed_frontier_is_reloaded(),
        test_the_queue_cap_bounds_domains(),
        test_checkpoint_writes_every_queue(),
    ]
    return 0 if all(results) else 1


if __name__ == "__main__":
    raise SystemExit(main())
