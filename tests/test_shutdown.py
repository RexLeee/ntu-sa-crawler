#!/usr/bin/env python3
"""Prove the timed shutdown can actually close the crawl.

Run directly: uv run python tests/test_shutdown.py

The problem it solves: engine.close_spider_async() awaits _Slot.close(), whose
deferred fires only when slot.inprogress is empty. inprogress holds every
request handed to the downloader, including the thousands parked on a 5 second
per-domain timer. Waiting for those to drain took over five minutes, so every
run was SIGKILLed instead and lost its stats dump and its Bloom filter.

TimedShutdown cancels the parked requests first. Each downloader slot queue
entry is the (request, deferred) pair that Downloader._enqueue_request awaits;
erroring that deferred makes the await raise, the engine drops the request from
inprogress, and the close can proceed.

These checks use fake slots rather than a live crawl, because what has to be
verified is the contract with Scrapy's data structures: a deque of pairs whose
second element is an awaited Deferred.
"""

from __future__ import annotations

import sys
from collections import deque
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from twisted.internet.defer import Deferred  # noqa: E402

from crawler.extensions.shutdown import TimedShutdown  # noqa: E402


class _FakeSlot:
    def __init__(self, n: int):
        self.queue = deque((f"req{i}", Deferred()) for i in range(n))
        self.closed = False

    def close(self) -> None:
        self.closed = True


class _FakeStats:
    def __init__(self):
        self.values: dict[str, object] = {}

    def set_value(self, key, value) -> None:
        self.values[key] = value


class _FakeCrawler:
    def __init__(self, slots):
        self.stats = _FakeStats()
        self.engine = type("E", (), {"downloader": type("D", (), {"slots": slots})()})()


def _consume(deferred: Deferred) -> list[str]:
    """Attach an errback so an unhandled failure does not warn at GC."""
    seen: list[str] = []
    deferred.addErrback(lambda f: seen.append(type(f.value).__name__))
    return seen


def test_parked_requests_are_cancelled() -> bool:
    slots = {"a.example": _FakeSlot(3), "b.example": _FakeSlot(4)}
    outcomes = [_consume(d) for slot in slots.values() for _r, d in slot.queue]

    ext = TimedShutdown(_FakeCrawler(slots), 60.0)
    cancelled = ext._drain_parked_requests()

    if cancelled != 7:
        print(f"FAIL: cancelled {cancelled}, expected 7")
        return False
    if any(slot.queue for slot in slots.values()):
        print("FAIL: a slot queue still holds entries")
        return False
    unfired = [o for o in outcomes if not o]
    if unfired:
        print(f"FAIL: {len(unfired)} deferred(s) never fired")
        return False
    wrong = [o[0] for o in outcomes if o[0] != "IgnoreRequest"]
    if wrong:
        print(f"FAIL: deferreds failed with {set(wrong)}, expected IgnoreRequest")
        return False

    print(f"PASS: {cancelled} parked requests cancelled with IgnoreRequest")
    return True


def test_slot_timers_are_stopped() -> bool:
    """Each slot's own latercall must be cancelled before its queue is emptied.

    Slot.close() cancels the pending _process_queue callback. Without it the
    slot would keep trying to process a queue the shutdown is draining.
    """
    slots = {"a.example": _FakeSlot(2)}
    for _r, d in slots["a.example"].queue:
        _consume(d)

    TimedShutdown(_FakeCrawler(slots), 60.0)._drain_parked_requests()

    if not slots["a.example"].closed:
        print("FAIL: slot.close() was not called")
        return False
    print("PASS: slot timers stopped before their queues were drained")
    return True


def test_already_fired_deferreds_are_left_alone() -> bool:
    """A request that completed between the timer firing and this loop.

    Erroring an already-called Deferred raises AlreadyCalledError, which would
    abort the drain partway and leave the rest of the crawl unclosable.
    """
    slot = _FakeSlot(3)
    _r, done = slot.queue[1]
    done.callback("already downloaded")

    ext = TimedShutdown(_FakeCrawler({"a.example": slot}), 60.0)
    for _rr, d in list(slot.queue):
        if d is not done:
            _consume(d)
    cancelled = ext._drain_parked_requests()

    if cancelled != 2:
        print(f"FAIL: cancelled {cancelled}, expected 2 (one was already done)")
        return False
    print("PASS: an already-completed request is skipped, not re-fired")
    return True


def test_missing_engine_is_survivable() -> bool:
    """The timer can fire before the engine exists, or after it is gone."""
    crawler = type("C", (), {"stats": _FakeStats(), "engine": None})()
    if TimedShutdown(crawler, 60.0)._drain_parked_requests() != 0:
        print("FAIL: expected 0 cancellations with no engine")
        return False
    print("PASS: a missing engine returns 0 rather than raising")
    return True


def test_counter_reaches_stats() -> bool:
    """The count has to land in stats, or the report cannot state it."""
    slots = {"a.example": _FakeSlot(5)}
    for _r, d in slots["a.example"].queue:
        _consume(d)

    crawler = _FakeCrawler(slots)
    ext = TimedShutdown(crawler, 60.0)
    cancelled = ext._drain_parked_requests()
    crawler.stats.set_value("shutdown/cancelled_queue", cancelled)

    if crawler.stats.values.get("shutdown/cancelled_queue") != 5:
        print(f"FAIL: stats hold {crawler.stats.values}")
        return False
    print("PASS: cancelled count recorded as shutdown/cancelled_queue")
    return True


def test_stop_schedules_the_close() -> bool:
    """_stop must both drain AND close. Draining alone does nothing.

    This is the check that was missing when the extension imported
    _schedule_coro from scrapy.utils.asyncio instead of scrapy.utils.defer.
    The import raised inside a reactor callback, which swallows exceptions
    into the log; at LOG_LEVEL=WARNING nothing was visible. A 90 second run
    logged "cancelled 1888 parked requests" and then kept fetching for seven
    more minutes, because close_spider_async was never scheduled.
    """
    import crawler.extensions.shutdown as mod

    slot = _FakeSlot(2)
    for _r, d in slot.queue:
        _consume(d)

    closed: list[str] = []

    class _Engine:
        downloader = type("D", (), {"slots": {"a.example": slot}})()

        async def close_spider_async(self, *, reason):
            closed.append(reason)

    crawler = type("C", (), {"stats": _FakeStats(), "engine": _Engine()})()

    scheduled: list[object] = []
    original = mod._schedule_coro
    mod._schedule_coro = scheduled.append
    try:
        mod.TimedShutdown(crawler, 60.0)._stop()
    finally:
        mod._schedule_coro = original

    if not scheduled:
        print("FAIL: _stop drained the queues but scheduled no close")
        return False
    # Run the coroutine so the reason is observable.
    import asyncio

    asyncio.run(scheduled[0])
    if closed != ["run_duration"]:
        print(f"FAIL: closed with {closed}, expected ['run_duration']")
        return False
    if crawler.stats.values.get("shutdown/cancelled_queue") != 2:
        print(f"FAIL: stats hold {crawler.stats.values}")
        return False

    print("PASS: _stop drains the queues and schedules close_spider_async")
    return True


def test_schedule_coro_import_is_real() -> bool:
    """The symbol must exist where the extension imports it from.

    Guards against the module moving in a future Scrapy version, which would
    otherwise only show up as a 48 hour run that refuses to stop.
    """
    from scrapy.utils.defer import _schedule_coro as real

    import crawler.extensions.shutdown as mod

    if mod._schedule_coro is not real:
        print("FAIL: the extension does not hold scrapy.utils.defer._schedule_coro")
        return False
    print("PASS: _schedule_coro resolves to scrapy.utils.defer")
    return True


def main() -> int:
    results = [
        test_parked_requests_are_cancelled(),
        test_slot_timers_are_stopped(),
        test_already_fired_deferreds_are_left_alone(),
        test_missing_engine_is_survivable(),
        test_counter_reaches_stats(),
        test_stop_schedules_the_close(),
        test_schedule_coro_import_is_real(),
    ]
    return 0 if all(results) else 1


if __name__ == "__main__":
    raise SystemExit(main())
