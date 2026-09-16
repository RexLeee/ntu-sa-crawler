"""Stop the crawl after a fixed duration, closing it cleanly enough to persist.

Why not CLOSESPIDER_TIMEOUT
---------------------------
Scrapy's own extension calls engine.close_spider_async(), which awaits
_Slot.close(). That deferred only fires once slot.inprogress is empty, and
inprogress holds every request the engine has handed to the downloader,
including the ones parked on a per-domain timer. At CONCURRENT_REQUESTS=3000
with a 5s gap per domain, draining that queue naturally takes as long as the
slowest domain's backlog: measured at over five minutes, and the run was
killed before it finished.

Why not a signal from outside
------------------------------
ops/run_hour.sh used to send SIGTERM. Two things went wrong. The signal went
to the `uv run` wrapper, which does not forward it, and even when it reached
Python the shutdown still had to drain the same queue. Measured at GRACE=90:
both shards used the whole window and were SIGKILLed without writing "Dumping
Scrapy stats".

What this does instead
----------------------
Cancel the parked requests first, then close. Every entry in a downloader
slot's queue is a (request, deferred) pair whose deferred is awaited by
Downloader._enqueue_request. Erroring it back makes that await raise, the
engine removes the request from inprogress, and _Slot.close() can fire. What
remains is only what is genuinely on the wire, bounded by download_timeout,
plus up to one min_gap of sleep inside crawler/dispatch.py.

Cancelling is safe because a parked request was never sent. It is dropped, not
failed: nothing was written to a socket, so no politeness gap is affected.

Getting here is what produces the stats dump, the persisted Bloom filter, the
final runstats sample and every spider_closed handler. All four were lost on
every run before this existed.
"""

from __future__ import annotations

import logging

from scrapy import signals
from scrapy.exceptions import IgnoreRequest

# scrapy.utils.defer, not scrapy.utils.asyncio. Imported at module scope so a
# wrong path fails when the extension loads rather than inside the reactor
# callback that stops the crawl. scrapy/extensions/closespider.py takes it
# from here too.
from scrapy.utils.defer import _schedule_coro
from twisted.python.failure import Failure

logger = logging.getLogger(__name__)


class TimedShutdown:
    """Close the spider after RUN_DURATION seconds."""

    def __init__(self, crawler, duration: float):
        self.crawler = crawler
        self.duration = duration
        self._call = None

    @classmethod
    def from_crawler(cls, crawler):
        duration = crawler.settings.getfloat("RUN_DURATION", 0.0)
        ext = cls(crawler, duration)
        if duration > 0:
            crawler.signals.connect(ext.spider_opened, signal=signals.spider_opened)
            crawler.signals.connect(ext.spider_closed, signal=signals.spider_closed)
        return ext

    def spider_opened(self, spider) -> None:
        from twisted.internet import reactor

        self._call = reactor.callLater(self.duration, self._stop)
        logger.info("run will stop after %.0fs", self.duration)

    def spider_closed(self, spider) -> None:
        if self._call is not None and self._call.active():
            self._call.cancel()
        self._call = None

    def _stop(self) -> None:
        self._call = None
        try:
            cancelled = self._drain_parked_requests()
            self.crawler.stats.set_value("shutdown/cancelled_queue", cancelled)
            logger.warning(
                "run duration reached, cancelled %d parked requests, closing", cancelled
            )
            _schedule_coro(
                self.crawler.engine.close_spider_async(reason="run_duration")
            )
        except Exception:
            # A reactor callback swallows exceptions into the log, and at
            # LOG_LEVEL=WARNING an error here was invisible: a measured run
            # drained its queues, logged the cancellation, and then kept
            # fetching for seven minutes because the close never ran. If this
            # path fails the run cannot stop itself, so say so loudly.
            logger.exception("TIMED SHUTDOWN FAILED, the crawl will not stop itself")
            raise

    def _drain_parked_requests(self) -> int:
        """Fail every request still waiting on a domain timer.

        Each queue entry is the (request, deferred) pair that
        Downloader._enqueue_request awaits. Erroring the deferred makes that
        await raise, which removes the request from the engine's inprogress
        set and lets _Slot.close() fire.
        """
        engine = getattr(self.crawler, "engine", None)
        downloader = getattr(engine, "downloader", None)
        if downloader is None:
            return 0

        cancelled = 0
        for slot in list(getattr(downloader, "slots", {}).values()):
            # Stop the slot's own timer first, or it would keep trying to
            # process a queue this loop is emptying.
            slot.close()
            while slot.queue:
                _request, deferred = slot.queue.popleft()
                if not deferred.called:
                    deferred.errback(
                        Failure(IgnoreRequest("cancelled: run duration reached"))
                    )
                    cancelled += 1
        return cancelled
