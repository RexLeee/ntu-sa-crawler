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
from scrapy.utils.datatypes import LocalCache
from scrapy.utils.defer import maybe_deferred_to_future
from scrapy.utils.httpobj import urlparse_cached
from scrapy.utils.misc import build_from_crawler
from twisted.internet.defer import Deferred

from crawler.slot import slot_key

logger = logging.getLogger(__name__)

MAX_CRAWL_DELAY = 60.0

# _parsers stores None for a host whose robots.txt could not be fetched, so
# None cannot double as "not cached". This sentinel distinguishes the two.
_MISSING = object()


class PoliteRobotsTxtMiddleware(RobotsTxtMiddleware):
    """RobotsTxtMiddleware that shares our slots and honours Crawl-delay."""

    def __init__(self, crawler):
        super().__init__(crawler)
        self._floor_delay: float = crawler.settings.getfloat("DOWNLOAD_DELAY")

        # The parent keeps every parser it has ever built in a plain dict keyed
        # by hostname, and never evicts. Measured sizes: 0.9 KB for a one-rule
        # file, 7.1 KB for a typical one, 369 KB for a 500-rule one. A broad
        # crawl reaching 500k hosts would hold several GB in this dict alone,
        # which is what drove RSS to 5.3 GB in a 87 minute run.
        #
        # Evicting is safe. A miss re-fetches robots.txt and waits for it; it
        # never skips the check. The only cost is one extra fetch on that
        # domain's own 5s slot.
        cache_size = crawler.settings.getint("ROBOTS_CACHE_SIZE", 50_000)
        self._parsers = LocalCache(limit=cache_size)

        # Fetches currently running. Unbounded on purpose, but its size is the
        # number of concurrent robots fetches, not the number of hosts ever
        # seen, and every entry is removed when its fetch settles.
        self._inflight: dict[str, Deferred] = {}

        # The Downloader creates slots lazily, so a delay learned before the
        # slot exists must be replayed once it does. Bounded for the same
        # reason as the parsers: it is keyed by domain and would otherwise
        # grow for the whole 48 hours.
        self._pending_delays = LocalCache(limit=cache_size)
        crawler.signals.connect(self._on_robots_parsed, signal=signals.robots_parsed)
        # The runstats extension reports cache occupancy and in-flight count.
        crawler.robots_middleware = self

    async def robot_parser(self, request: Request):
        """Fetch and cache robots.txt, keeping the fetch on our domain slot.

        Reimplemented rather than delegated because the parent builds its own
        Request with no download_slot, which would give robots.txt a separate
        rate limiter from the pages of the same site.

        In-flight fetches live in a separate unbounded dict. _parsers is a
        LocalCache that evicts oldest-first, so a Deferred placeholder left
        there could be evicted mid-fetch: a second caller would then start a
        duplicate fetch and the first would await a Deferred nobody fires.
        _inflight is keyed the same way but only ever holds the handful of
        fetches actually running, and each entry is removed when it completes.
        """
        url = urlparse_cached(request)
        netloc = url.netloc

        parser = self._parsers.get(netloc, _MISSING)
        if parser is not _MISSING:
            return parser

        pending = self._inflight.get(netloc)
        if pending is not None:
            return await maybe_deferred_to_future(pending)

        self._inflight[netloc] = Deferred()
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
                # DEBUG, not WARNING. A broad crawl reaches dead hosts
                # constantly: 17,742 of these in a 28 minute run. Every one is
                # already counted in robotstxt/exception_count/*, which is the
                # form the report needs.
                logger.debug("robots.txt fetch failed for %s: %s", netloc, e)
            self._robots_error(e, netloc)
        finally:
            # _parse_robots and _robots_error read this entry to fire the
            # Deferred, so it must outlive them. Removing it here also keeps a
            # failed fetch from blocking a later retry for the same host.
            self._inflight.pop(netloc, None)
        self._stats.inc_value("robotstxt/request_count")

        # _parse_robots / _robots_error stored the outcome just above. An
        # eviction between those two lines is possible but harmless: the rules
        # were already applied to this request via the signal, and the next
        # request for this host simply re-fetches.
        return self._parsers.get(netloc)

    async def _parse_robots(self, response, netloc: str, request: Request) -> None:
        """Store the parsed rules and wake every waiter.

        The parent reads self._parsers[netloc] to find the Deferred to fire and
        asserts it is one. Since _parsers now evicts, that entry may be gone by
        the time the fetch returns, which would raise KeyError and leave the
        waiters hanging. _inflight is the authoritative handle instead.
        """
        self._stats.inc_value("robotstxt/response_count")
        self._stats.inc_value(f"robotstxt/response_status_count/{response.status}")
        rp = build_from_crawler(self._parserimpl, self.crawler, response.body)
        await self.crawler.signals.send_catch_log_async(
            signal=signals.robots_parsed,
            robotparser=rp,
            request=request,
        )
        self._parsers[netloc] = rp
        rp_dfd = self._inflight.get(netloc)
        if rp_dfd is not None:
            rp_dfd.callback(rp)

    def _robots_error(self, exc: Exception, netloc: str) -> None:
        """Record the failure and wake every waiter. See _parse_robots."""
        if not isinstance(exc, IgnoreRequest):
            self._stats.inc_value(f"robotstxt/exception_count/{type(exc)}")
        self._parsers[netloc] = None
        rp_dfd = self._inflight.get(netloc)
        if rp_dfd is not None:
            rp_dfd.callback(None)

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
