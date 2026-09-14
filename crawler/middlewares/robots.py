"""robots.txt handling with Crawl-delay support.

Scrapy's RobotsTxtMiddleware leaves two gaps for a politeness-critical crawl:

1. It builds the robots.txt Request itself without our download_slot, so the
   fetch lands on a hostname-keyed slot and bypasses the domain timer.
2. It parses Crawl-delay but never applies it (scrapy/scrapy#892). A site
   asking for 10s would still be hit every 5s.

This subclass closes both.
"""

from __future__ import annotations

import logging

from scrapy import Request, signals
from scrapy.downloadermiddlewares.robotstxt import RobotsTxtMiddleware
from scrapy.exceptions import IgnoreRequest
from scrapy.http.request import NO_CALLBACK
from scrapy.utils.defer import maybe_deferred_to_future
from scrapy.utils.httpobj import urlparse_cached
from twisted.internet.defer import Deferred

from crawler.slot import slot_key

logger = logging.getLogger(__name__)

MAX_CRAWL_DELAY = 60.0


class PoliteRobotsTxtMiddleware(RobotsTxtMiddleware):
    """RobotsTxtMiddleware that shares our slots and honours Crawl-delay."""

    def __init__(self, crawler):
        super().__init__(crawler)
        self._floor_delay: float = crawler.settings.getfloat("DOWNLOAD_DELAY")
        # The Downloader creates slots lazily, so a delay learned before the
        # slot exists must be replayed once it does.
        self._pending_delays: dict[str, float] = {}
        crawler.signals.connect(self._on_robots_parsed, signal=signals.robots_parsed)

    async def robot_parser(self, request: Request):
        """Fetch and cache robots.txt, keeping the fetch on our domain slot.

        Reimplemented rather than delegated because the parent builds its own
        Request with no download_slot, which would give robots.txt a separate
        rate limiter from the pages of the same site.
        """
        url = urlparse_cached(request)
        netloc = url.netloc

        if netloc not in self._parsers:
            self._parsers[netloc] = Deferred()
            robotsurl = f"{url.scheme}://{netloc}/robots.txt"
            robotsreq = Request(
                robotsurl,
                priority=self.DOWNLOAD_PRIORITY,
                meta={
                    "dont_obey_robotstxt": True,
                    # Share the page slot so robots.txt obeys the same 5s gap.
                    "download_slot": slot_key(robotsurl),
                },
                callback=NO_CALLBACK,
            )
            try:
                resp = await self.crawler.engine.download_async(robotsreq)
                await self._parse_robots(resp, netloc, request)
            except Exception as e:
                if not isinstance(e, IgnoreRequest):
                    logger.warning(
                        "robots.txt fetch failed for %s: %s", netloc, e
                    )
                self._robots_error(e, netloc)
            self._stats.inc_value("robotstxt/request_count")

        parser = self._parsers[netloc]
        if isinstance(parser, Deferred):
            return await maybe_deferred_to_future(parser)
        return parser

    def _on_robots_parsed(self, robotparser, request: Request) -> None:
        """Apply Crawl-delay once a site's robots.txt has been parsed."""
        try:
            useragent = self._robotstxt_useragent or self._default_useragent
            delay = robotparser.crawl_delay(useragent)
        except Exception:  # a malformed robots.txt must never break the crawl
            return

        if delay is None:
            return

        requested = float(delay)
        if requested <= self._floor_delay:
            return

        key = slot_key(request.url)
        capped = min(requested, MAX_CRAWL_DELAY)
        self._pending_delays[key] = capped
        self._apply_delay(key, capped)

    def _apply_delay(self, key: str, delay: float) -> None:
        slot = self.crawler.engine.downloader.slots.get(key)
        if slot is None:
            return  # replayed from process_request_2 once the slot exists
        if slot.delay < delay:
            slot.delay = delay
            logger.info("Crawl-delay %.1fs applied to slot %s", delay, key)

    def process_request_2(self, rp, request: Request) -> None:
        key = request.meta.get("download_slot")
        if key and key in self._pending_delays:
            self._apply_delay(key, self._pending_delays[key])
        super().process_request_2(rp, request)
