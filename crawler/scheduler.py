"""Scheduler that refuses to let the frontier grow without bound.

Scrapy has no setting that caps the queue: enqueue_request rejects a request
only when the dupefilter has seen it. In a breadth-first broad crawl that is
not enough. Each page yields around 50 links but costs one domain five
seconds of budget, so the frontier fills roughly 33 times faster than it
drains and memory tracks it linearly.

A measured hour: 40,773 pages crawled against 1.4M URLs discovered, with RSS
rising 1.6 MB/s and no sign of flattening.

Dropped requests are deliberately not marked as seen. They can be
rediscovered later when there is room, which costs a little CPU and keeps the
crawl correct.
"""

from __future__ import annotations

import logging

from scrapy.core.scheduler import Scheduler

logger = logging.getLogger(__name__)


class CappedScheduler(Scheduler):
    """Rejects new requests once the frontier reaches FRONTIER_MAX_SIZE."""

    @classmethod
    def from_crawler(cls, crawler):
        obj = super().from_crawler(crawler)
        obj._cap = crawler.settings.getint("FRONTIER_MAX_SIZE", 500_000)
        obj._warned = False
        return obj

    def enqueue_request(self, request) -> bool:
        if len(self) >= self._cap:
            if not self._warned:
                logger.info(
                    "frontier reached %d requests; dropping new ones until it drains",
                    self._cap,
                )
                self._warned = True
            if self.stats is not None:
                self.stats.inc_value("scheduler/dropped/over_capacity")
            # Returning False makes the engine fire request_dropped, which is
            # the documented contract for BaseScheduler.enqueue_request.
            return False
        return super().enqueue_request(request)
