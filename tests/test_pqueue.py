#!/usr/bin/env python3
"""Prove the ring queue keeps the parent's behaviour without its cost.

DownloaderAwarePriorityQueue.pop() scans every per-domain queue. A py-spy
profile of a 2 hour run put that scan at 52% of all CPU on the reactor thread,
against 5.2% for parse(). crawler/pqueue.py replaces the scan with a rotating
deque that stops at the first domain with no active download.

Five properties have to hold, and the sixth is the point of the change:

  * an idle domain is preferred over a busy one, as the parent does
  * idle domains are served round-robin, so none is starved
  * pop() returns a request whenever queues hold one, even when every domain
    is busy. Scheduler.has_pending_requests() reports on __len__, so a None
    here would spin the engine
  * a domain whose queue empties and is pushed to again still gets served,
    with exactly one ring entry
  * a resumed crawl (slot_startprios) serves what it loaded
  * pop() cost does not grow with the domain count

The last check measures the parent class alongside the new one, so it is
self-validating: it can only pass if the parent really is the slower of the
two. The behavioural checks run against the parent with --parent, which is how
they were validated: a check the parent also passes is testing preserved
behaviour, which is what four of them are for.

Run: uv run python tests/test_pqueue.py
"""

from __future__ import annotations

import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from twisted.internet import asyncioreactor  # noqa: E402

# DownloaderInterface reaches crawler.engine.downloader, which builds download
# handlers, which require an installed reactor.
asyncioreactor.install()

from scrapy import Request  # noqa: E402
from scrapy.pqueues import DownloaderAwarePriorityQueue  # noqa: E402
from scrapy.squeues import FifoMemoryQueue  # noqa: E402
from scrapy.utils.test import get_crawler  # noqa: E402

from crawler.pqueue import RingDownloaderAwarePriorityQueue  # noqa: E402
from crawler.spiders.broad import BroadSpider  # noqa: E402

# Set by main() so every check runs against the same class.
QUEUE_CLS: type = RingDownloaderAwarePriorityQueue


def _queue(scan_limit: int = 256, slot_startprios=None):
    """Build a priority queue wired to a real Downloader, as Scrapy does."""
    crawler = get_crawler(
        BroadSpider,
        {
            "CONCURRENT_REQUESTS_PER_IP": 0,
            "CONCURRENT_REQUESTS_PER_DOMAIN": 1,
            "PQUEUE_SCAN_LIMIT": scan_limit,
        },
    )
    crawler._apply_settings()
    crawler.spider = crawler._create_spider()
    crawler.engine = crawler._create_engine()
    queue = QUEUE_CLS(
        crawler,
        FifoMemoryQueue,
        key="",
        slot_startprios=slot_startprios,
    )
    return queue, crawler


def _request(slot: str, n: int = 0) -> Request:
    return Request(f"https://{slot}/{n}", meta={"download_slot": slot})


def _mark_active(crawler, slot: str, count: int = 1) -> None:
    """Put fake in-flight requests on a download slot.

    _active_downloads reads len(downloader.slots[slot].active), so this is the
    same state a real download in progress would produce.
    """
    downloader = crawler.engine.downloader
    _, downloader_slot = downloader._get_slot(_request(slot))
    for i in range(count):
        downloader_slot.active.add(_request(f"{slot}-active", i))


def _slot_of(request: Request) -> str:
    return request.meta["download_slot"]


def test_prefers_idle_slot() -> bool:
    """A domain with nothing in flight must win over a busy one."""
    queue, crawler = _queue()
    for slot in ("busy1.example", "busy2.example", "idle.example"):
        queue.push(_request(slot))
    _mark_active(crawler, "busy1.example")
    _mark_active(crawler, "busy2.example")

    chosen = _slot_of(queue.pop())
    if chosen != "idle.example":
        print(f"FAIL: picked {chosen}, expected the idle domain")
        return False
    print("PASS: the idle domain is preferred")
    return True


def test_round_robin() -> bool:
    """Idle domains must take turns, or one domain starves the rest."""
    queue, _ = _queue()
    slots = [f"d{i}.example" for i in range(5)]
    for slot in slots:
        for n in range(2):
            queue.push(_request(slot, n))

    order = [_slot_of(queue.pop()) for _ in range(10)]
    if order[:5] != order[5:]:
        print(f"FAIL: not a round robin: {order}")
        return False
    if sorted(order[:5]) != sorted(slots):
        print(f"FAIL: first pass missed a domain: {order[:5]}")
        return False
    print(f"PASS: round robin over {len(slots)} idle domains")
    return True


def test_returns_none_only_when_empty() -> bool:
    """Every domain busy must still yield a request, not None.

    Scheduler.has_pending_requests() answers from __len__. Returning None
    while queues hold requests makes the engine call next_request() again on
    every heartbeat and get nothing, forever.
    """
    queue, crawler = _queue(scan_limit=2)
    slots = [f"b{i}.example" for i in range(10)]
    for slot in slots:
        queue.push(_request(slot))
        _mark_active(crawler, slot, count=3)

    popped = 0
    while len(queue) > 0:
        request = queue.pop()
        if request is None:
            print(f"FAIL: pop() returned None with {len(queue)} requests left")
            return False
        popped += 1
        if popped > len(slots):
            print("FAIL: popped more requests than were pushed")
            return False

    if popped != len(slots):
        print(f"FAIL: popped {popped} of {len(slots)}")
        return False
    if queue.pop() is not None:
        print("FAIL: an empty queue returned a request")
        return False
    print(f"PASS: all {popped} requests served with every domain busy")
    return True


def test_stale_entry_and_repush() -> bool:
    """A drained domain that comes back must be served exactly once per push.

    pop() leaves the ring entry of an emptied queue in place rather than
    paying O(ring) to remove it. That entry must not produce a duplicate when
    the domain is pushed to again, and must not shadow the new queue.
    """
    queue, _ = _queue()
    queue.push(_request("gone.example"))
    queue.push(_request("other.example"))
    queue.pop()
    queue.pop()

    if len(queue) != 0:
        print(f"FAIL: expected an empty queue, len={len(queue)}")
        return False

    queue.push(_request("gone.example", 1))
    request = queue.pop()
    if request is None or _slot_of(request) != "gone.example":
        print(f"FAIL: re-pushed domain not served, got {request}")
        return False

    # The ring bookkeeping is this class's own; the parent has no ring to
    # check, and passes the behavioural half above.
    ring = getattr(queue, "_ring", None)
    if ring is not None:
        copies = [s for s in ring if s == "gone.example"]
        if len(copies) > 1:
            print(f"FAIL: ring holds {len(copies)} copies of one domain")
            return False
    print("PASS: a drained domain revives without duplicating its ring entry")
    return True


def test_resume_from_startprios() -> bool:
    """A resumed crawl must serve the queues it rebuilt from disk state.

    The parent's __init__ builds one queue per slot_startprios entry. Those
    queues exist before push() is ever called, so the ring has to pick them up
    in __init__ or a resumed frontier is invisible.
    """
    queue, _ = _queue(slot_startprios={"resumed.example": [0]})
    members = getattr(queue, "_ring_members", None)
    if members is not None and "resumed.example" not in members:
        print("FAIL: a resumed slot never entered the ring")
        return False

    queue.pqueues["resumed.example"].push(_request("resumed.example"))
    request = queue.pop()
    if request is None or _slot_of(request) != "resumed.example":
        print(f"FAIL: resumed slot not served, got {request}")
        return False
    print("PASS: resumed slots enter the ring")
    return True


def test_scan_limit_does_not_starve() -> bool:
    """A domain beyond the scan limit must still get served.

    pop() only looks at the first PQUEUE_SCAN_LIMIT ring entries. If the ring
    did not rotate what it skips to the back, every domain past that window
    would be invisible forever, and the crawl would circle a handful of
    domains while the frontier grew.

    Run with every domain busy so the cap is reached on every single pop,
    which is the worst case for this property.
    """
    domains = 50
    queue, crawler = _queue(scan_limit=3)
    for i in range(domains):
        for n in range(4):
            queue.push(_request(f"d{i}.example", n))
        _mark_active(crawler, f"d{i}.example")

    served: dict[str, int] = {}
    for _ in range(domains * 4):
        request = queue.pop()
        if request is None:
            break
        slot = _slot_of(request)
        served[slot] = served.get(slot, 0) + 1

    if len(served) != domains:
        missing = domains - len(served)
        print(f"FAIL: {missing} of {domains} domains never served past a scan limit of 3")
        return False
    spread = max(served.values()) - min(served.values())
    if spread > 1:
        print(f"FAIL: uneven service, {min(served.values())} to {max(served.values())}")
        return False
    print(
        f"PASS: all {domains} domains served evenly with a scan limit of 3 "
        f"({min(served.values())} each)"
    )
    return True


def _time_pop(cls, domains: int, pops: int = 2_000) -> float:
    """Microseconds per pop() at a given domain count."""
    global QUEUE_CLS
    saved, QUEUE_CLS = QUEUE_CLS, cls
    try:
        queue, _ = _queue()
        for i in range(domains):
            for n in range(3):
                queue.push(_request(f"d{i}.example", n))
        start = time.perf_counter()
        for _ in range(pops):
            queue.pop()
        return (time.perf_counter() - start) / pops * 1e6
    finally:
        QUEUE_CLS = saved


def test_pop_cost_beats_the_parent() -> bool:
    """pop() must stay flat where the parent grows with the domain count.

    This is the whole point of the class, so it is measured against the class
    it replaces rather than against an absolute number: the ring's own cost is
    under 2us, where timer noise alone moves the ratio.

    Measured on this machine, microseconds per pop:

        domains    parent      ring
            200      3.98      0.30
          2,000    141.67      0.90
         20,000  1,734.12      1.09
         50,000  4,863.36      1.12

    The production crawl holds about 5,000.
    """
    small, large = 200, 20_000
    ring_small = _time_pop(RingDownloaderAwarePriorityQueue, small)
    ring_large = _time_pop(RingDownloaderAwarePriorityQueue, large)
    parent_large = _time_pop(DownloaderAwarePriorityQueue, large)

    # The parent is O(domains); anything under a large factor here means the
    # comparison is not measuring what it claims to.
    speedup = parent_large / ring_large
    if speedup < 50:
        print(
            f"FAIL: only {speedup:.1f}x faster than the parent at {large} domains "
            f"({parent_large:.1f}us vs {ring_large:.1f}us)"
        )
        return False

    # Flat in absolute terms: growth of a microsecond is fine, of a
    # millisecond is not.
    growth = ring_large - ring_small
    if growth > 5.0:
        print(f"FAIL: ring pop grew {growth:.1f}us over a 100x domain increase")
        return False

    print(
        f"PASS: pop flat ({ring_small:.2f}us -> {ring_large:.2f}us) and "
        f"{speedup:.0f}x the parent at {large} domains ({parent_large:.0f}us)"
    )
    return True


def main() -> int:
    global QUEUE_CLS
    if "--parent" in sys.argv:
        QUEUE_CLS = DownloaderAwarePriorityQueue
        print("running against scrapy's DownloaderAwarePriorityQueue\n")

    results = [
        test_prefers_idle_slot(),
        test_round_robin(),
        test_returns_none_only_when_empty(),
        test_stale_entry_and_repush(),
        test_resume_from_startprios(),
        test_scan_limit_does_not_starve(),
        test_pop_cost_beats_the_parent(),
    ]
    return 0 if all(results) else 1


if __name__ == "__main__":
    raise SystemExit(main())
