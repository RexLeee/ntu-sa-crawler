"""Broad crawl spider.

Throughput is bounded by active_domains / delay, not by machine speed, so the
spider's main job is to keep discovering new domains and to stop any single
domain from monopolising the frontier.
"""

from __future__ import annotations

import logging
from collections import defaultdict

from scrapy import Request, Spider, signals
from scrapy.exceptions import CloseSpider
from scrapy.utils.datatypes import LocalCache

from crawler.config import PROJECT_ROOT, data_dir, load_config, state_dir
from crawler.dispatch import DISPATCH_TIME, close_dispatch_log
from crawler.dispatch import configure as configure_dispatch
from crawler.dispatch import set_stats as set_dispatch_stats
from crawler.extract import find_base_href, iter_hrefs
from crawler.filters import UrlFilter
from crawler.handoff import Handoff
from crawler.logwriter import GzipLineWriter, now
from crawler.slot import shard_of, slot_key

logger = logging.getLogger(__name__)

_HTML_HINTS = ("text/html", "application/xhtml")


class BroadSpider(Spider):
    name = "broad"

    def __init__(self, seeds: str | None = None, shard: int = 0, shards: int = 1, **kw):
        super().__init__(**kw)
        self.cfg = load_config()
        # The hard floor the assignment sets, enforced at the last point
        # before the socket write. DOWNLOAD_DELAY schedules above it; this
        # catches the cases where event-loop congestion would let a pair slip
        # under. See crawler/dispatch.py.
        # The trace records any dispatch pair that lands closer than the floor,
        # with the slot and timer state that produced it. It writes only on a
        # short gap, so it stays on for the whole run.
        self.shard = int(shard)
        self.shards = int(shards)
        suffix = f"-{self.shard}" if self.shards > 1 else ""

        # The floor the guard in crawler/dispatch.py refuses below. The
        # limiter itself is crawler/downloader.py, which counts the delay from
        # the previous response's completion; this is the backstop that turns
        # any remaining short gap into a dropped page rather than a violation.
        self._trace_path = None
        if self.cfg["politeness"].get("trace_short_gaps", False):
            self._trace_path = str(data_dir() / f"violation-trace{suffix}.log")
        # The complete record of what went out, including robots.txt and the
        # requests that fail. crawled.log.gz cannot serve as compliance
        # evidence on its own; see config.toml politeness.log_dispatches.
        self._dispatch_log_path = None
        if self.cfg["politeness"].get("log_dispatches", False):
            self._dispatch_log_path = str(data_dir() / f"dispatched{suffix}.log.gz")
        self.filter = UrlFilter(self.cfg)

        limits = self.cfg["limits"]
        self.max_crawled_per_domain: int = limits["max_crawled_per_domain"]
        self.max_queued_per_domain: int = limits["max_queued_per_domain"]

        # Set in from_crawler. Checking the filter here rather than letting the
        # scheduler do it avoids building a Request for a URL we have seen.
        self.dupefilter = None
        self.dupe_skipped = 0
        self.discovered_raw = 0
        # Responses that arrived without a dispatch stamp, and URLs another
        # shard sent us that we do not own. Both should stay at 0; anything
        # else is a defect, so they are counted rather than ignored.
        self.missing_stamp = 0
        self.handoff_foreign = 0
        # URLs from the inbox that could not be turned into a Request. Expected
        # to be 0 now that crawler/filters.py rejects a malformed port, but
        # counted rather than assumed: the loop that reads the inbox must
        # survive whatever arrives in it.
        self.handoff_bad = 0

        # Cross-shard URLs are handed to their owner rather than dropped. A
        # broad crawl discovers nearly every new domain through a cross-domain
        # link, and throughput is bounded by the domains a process holds, so
        # discarding them would leave N processes crawling little more than
        # one. See crawler/handoff.py.
        sharding = self.cfg["sharding"]
        self.handoff = Handoff(
            self.shard,
            self.shards,
            state_dir() / "handoff",
            segment_bytes=int(sharding["handoff_segment_bytes"]),
            read_bytes=int(sharding["handoff_read_bytes"]),
        )
        self._handoff_interval = float(sharding["handoff_poll_interval"])
        self._handoff_batch = int(sharding["handoff_batch_max"])
        self._handoff_loop = None

        # -a seeds=... wins, then the production list, then the smoke list.
        seeds_path = seeds or self.cfg["seeds"]["seeds_file"]
        self.seeds_path = (PROJECT_ROOT / seeds_path).resolve()

        # Per-domain bookkeeping. Two of these are keyed by every domain ever
        # touched, and that count has no ceiling: a 2 hour run reached 343,644
        # domains per shard and the arrival rate was still 14.7/s at the end.
        # Fitting the curve gives 4.6M domains at 48 hours (t^0.796, R2 0.987)
        # and 8.2M on a linear read. At a measured 198 bytes per domain across
        # the two, that is 875-1,546 MB per shard against 931 MB of headroom
        # under the 2,800 MB guard, so both are bounded here.
        #
        # Evicting is safe for both. crawled_per_domain losing an entry resets
        # that domain's page count, which can only re-allow crawling a domain
        # that had been capped -- it never blocks one. seen_domains losing an
        # entry re-grants one new_domain exemption, worth a single request.
        #
        # queued_per_domain is NOT bounded here and must not be: _release_queued
        # pops each key when its count reaches zero, so it already tracks live
        # domains only, and its size follows PQUEUE_MAX rather than the run.
        # Capping it would drop counts for domains still holding frontier slots.
        domain_cap = self.cfg["memory"]["seen_domains_max"]
        self.crawled_per_domain: LocalCache = LocalCache(limit=domain_cap)
        self.queued_per_domain: dict[str, int] = defaultdict(int)
        self.seen_domains: LocalCache = LocalCache(limit=domain_cap)
        # len(seen_domains) stops being the domain total once it evicts, and
        # domains/seen is a reported metric, so count separately.
        self.domains_seen_total = 0

        d = data_dir()
        self.crawled_log = GzipLineWriter(d / f"crawled{suffix}.log.gz")
        self.discovered_log = GzipLineWriter(d / f"discovered{suffix}.log.gz")

    # --- lifecycle ----------------------------------------------------------

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super().from_crawler(crawler, *args, **kwargs)
        # Open the floor and the two diagnostic files here, before the engine
        # and so before any dispatch can happen. crawler.stats does not exist
        # yet in Scrapy 2.19 -- it is set when the crawl starts -- so the
        # guard's counter is attached at spider_opened instead.
        configure_dispatch(
            spider.cfg["politeness"]["required_min_gap"],
            spider._trace_path,
            spider._dispatch_log_path,
        )
        crawler.signals.connect(spider._bind_dispatch_stats, signal=signals.spider_opened)
        # queued_per_domain counts requests waiting in the frontier, so every
        # request that leaves it has to release its slot. Without this the
        # counter only grows: an 87 minute run saw 9,684 downloads fail and
        # 14,084 robots fetches fail, none of which reach parse(), so a busy
        # domain hits max_queued_per_domain and is silenced for good.
        #
        # request_left_downloader covers both outcomes, success and failure.
        # request_dropped covers the requests CappedScheduler refuses before
        # they ever reach the downloader.
        crawler.signals.connect(
            spider._on_request_settled, signal=signals.request_left_downloader
        )
        crawler.signals.connect(spider._on_request_settled, signal=signals.request_dropped)
        # The dupefilter cannot be read here. Crawler.crawl builds the spider
        # before it builds the engine, and the engine is what constructs the
        # scheduler and its dupefilter, so crawler.bloom_dupefilter does not
        # exist yet. Binding it at spider_opened is the earliest safe point;
        # reading it here silently left the filter at None and disabled the
        # whole optimisation, which a run showed as dupe_skipped stuck at 0.
        crawler.signals.connect(spider._bind_dupefilter, signal=signals.spider_opened)
        crawler.signals.connect(spider._start_handoff, signal=signals.spider_opened)
        crawler.signals.connect(spider._stop_handoff, signal=signals.spider_closed)
        crawler.signals.connect(spider._record_stats, signal=signals.spider_closed)
        return spider

    def _start_handoff(self, spider) -> None:
        """Begin draining the inbox other shards write to.

        A timer rather than a generator because the URLs arrive continuously
        for the whole run, while Spider.start() is consumed once. The engine
        also stops pulling from start() under backpressure, which would stall
        handoff delivery exactly when the other shards are busiest.
        """
        if self.shards <= 1:
            return
        from scrapy.utils.asyncio import create_looping_call

        self._handoff_loop = create_looping_call(self._drain_handoff)
        self._handoff_loop.start(self._handoff_interval, now=False)

    def _stop_handoff(self, spider, reason) -> None:
        if self._handoff_loop is not None and getattr(
            self._handoff_loop, "running", False
        ):
            self._handoff_loop.stop()
        self.handoff.close()

    def _drain_handoff(self) -> None:
        """Inject URLs other shards handed us.

        Batched because this runs on the reactor thread: an unbounded inbox
        would block every download for as long as it took to admit.

        engine.crawl goes through the scheduler, so the frontier cap and the
        dupefilter apply exactly as they do to a link found locally. These
        URLs are deliberately not counted into discovered_raw: the shard that
        found them already did, and counting again would inflate the Tier 1
        metric by the number of times a URL crosses a process boundary.
        """
        try:
            urls = self.handoff.poll(self._handoff_batch)
        except Exception as exc:  # a broken inbox must not stop the crawl
            logger.warning("handoff poll failed: %s", exc)
            return

        for url in urls:
            try:
                self._inject_handoff(url)
            except Exception as exc:
                # One bad URL used to stop the whole loop. AsyncioLoopingCall
                # does not reschedule after its function raises, so a single
                # http://host:void(0)/ ended cross-shard delivery for the rest
                # of the run: a measured hour sent 1.79M URLs and received
                # 181k, because both loops died in the first nine minutes.
                # crawler/filters.py now rejects that shape at the source;
                # this is the backstop that keeps the loop alive regardless.
                self.handoff_bad += 1
                if self.handoff_bad == 1:
                    logger.warning(
                        "handoff url rejected: %s (%s); further ones counted "
                        "in stats as handoff/bad_url",
                        url,
                        exc,
                    )

    def _inject_handoff(self, url: str) -> None:
        """Admit one URL from the inbox. Raises on a malformed URL."""
        key = slot_key(url)
        # Verify ownership rather than trust the sender. Two processes crawling
        # one domain would each run their own 5s timer with no shared state
        # able to notice, which is the one failure mode sharding must not have.
        # A stale inbox is enough to cause it: the files are written under the
        # modulus of whichever run created them, so restarting with a
        # different shard count redirects every URL already sitting there.
        if self.shards > 1 and shard_of(key, self.shards) != self.shard:
            self.handoff_foreign += 1
            return
        if self.crawled_per_domain.get(key, 0) >= self.max_crawled_per_domain:
            return
        if self.queued_per_domain.get(key, 0) >= self.max_queued_per_domain:
            return
        request = self._admit(url, key)
        if request is not None:
            self.crawler.engine.crawl(request)

    def _bind_dispatch_stats(self, spider) -> None:
        """Give the politeness guard somewhere to report refusals.

        Separate from configure() because Crawler.stats raises until the crawl
        starts, while the guard's floor and its log files have to be in place
        before the Downloader exists.
        """
        set_dispatch_stats(self.crawler.stats)

    def _bind_dupefilter(self, spider) -> None:
        self.dupefilter = getattr(self.crawler, "bloom_dupefilter", None)
        if self.dupefilter is None:
            logger.warning(
                "no bloom dupefilter on the crawler; every discovered URL will "
                "be built into a Request and filtered by the scheduler instead"
            )

    def _record_stats(self, spider, reason) -> None:
        """Put the spider-side counters where the stats dump can see them."""
        stats = self.crawler.stats
        stats.set_value("discovered/raw", self.discovered_raw)
        stats.set_value("discovered/unique", self.discovered_log.count)
        stats.set_value("dupefilter/skipped_before_request", self.dupe_skipped)
        stats.set_value("domains/seen", self.domains_seen_total)
        stats.set_value("handoff/sent", self.handoff.sent)
        stats.set_value("handoff/received", self.handoff.received)
        stats.set_value("handoff/foreign_dropped", self.handoff_foreign)
        stats.set_value("handoff/bad_url", self.handoff_bad)
        stats.set_value("dispatch/missing_stamp", self.missing_stamp)

    def _release_queued(self, key: str) -> None:
        remaining = self.queued_per_domain.get(key, 0) - 1
        if remaining > 0:
            self.queued_per_domain[key] = remaining
        else:
            # Drop the key entirely so the dict tracks live domains only.
            self.queued_per_domain.pop(key, None)

    def _on_request_settled(self, request, spider) -> None:
        self._release_queued(slot_key(request.url))

    def start_requests(self):
        yield from self._seed_requests()

    async def start(self):
        # Scrapy >= 2.13 prefers the async start(); keep both for compatibility.
        for req in self._seed_requests():
            yield req

    def _seed_requests(self):
        """Yield the seeds this shard owns.

        dont_filter stays False, which means a resumed run yields nothing here:
        the Bloom checkpoint remembers all 1,000 seeds. That is correct
        deduplication, and it is deliberately not worked around. The starting
        work for a resumed shard is its disk frontier, which
        crawler/scheduler.py reloads in open(). A measured run where that
        reload was missing exited in 1.66 seconds having crawled nothing, and
        the defect was the unreadable frontier, not the filtered seeds.
        """
        if not self.seeds_path.exists():
            raise CloseSpider(f"seeds file not found: {self.seeds_path}")

        count = 0
        for line in self.seeds_path.read_text(encoding="utf-8").splitlines():
            url = line.strip()
            if not url or url.startswith("#"):
                continue
            key = slot_key(url)
            # Not handed over. Every process reads the same seed file, so the
            # owner finds its own seeds without being told.
            if self.shards > 1 and shard_of(key, self.shards) != self.shard:
                continue
            if key not in self.seen_domains:
                self.domains_seen_total += 1
            self.seen_domains[key] = True
            count += 1
            yield Request(
                url, callback=self.parse, meta={"seed": True}, dont_filter=False
            )
        logger.info("loaded %d seeds from %s", count, self.seeds_path)

    def closed(self, reason):
        self.crawled_log.close()
        self.discovered_log.close()
        close_dispatch_log()
        logger.info(
            "closed(%s): crawled=%d discovered=%d domains=%d handoff_sent=%d "
            "handoff_received=%d",
            reason,
            self.crawled_log.count,
            self.discovered_log.count,
            self.domains_seen_total,
            self.handoff.sent,
            self.handoff.received,
        )

    # --- parsing ------------------------------------------------------------

    def parse(self, response):
        key = slot_key(response.url)
        ctype = response.headers.get(b"Content-Type", b"").decode("latin-1", "replace")

        body = response.body
        links = self._extract_links(response, body) if self._is_html(ctype) else []

        # DOWNLOAD_DELAY spaces request starts apart, so compliance is measured
        # on dispatch time. See crawler/dispatch.py for why this is the only
        # correct source for it.
        #
        # No fallback to recv. A missing stamp used to be silently replaced by
        # the receive time, which is always later than the real dispatch and so
        # always widens the measured gap: the one case that could hide a
        # violation was the one case the fallback covered up. Writing NA
        # instead makes ops/verify_politeness.py count it and report it.
        recv = now()
        stamp = response.meta.get(DISPATCH_TIME)
        if stamp is None:
            self.missing_stamp += 1
            if self.missing_stamp == 1:
                logger.warning(
                    "response with no dispatch_time: %s (further ones counted "
                    "in stats as dispatch/missing_stamp)",
                    response.url,
                )
            sent_field = "NA"
        else:
            sent_field = f"{float(stamp):.3f}"
        self.crawled_log.write(
            sent_field,
            f"{recv:.3f}",
            response.url,
            response.status,
            ctype.split(";")[0],
            len(links),
            len(body),
        )
        # LocalCache has no defaultdict behaviour, so read-then-write.
        self.crawled_per_domain[key] = self.crawled_per_domain.get(key, 0) + 1
        # The queued counter is released by the request_left_downloader
        # signal, which fires for failures too. Doing it here as well would
        # double-count.

        for url in links:
            yield from self._maybe_follow(url, response)

    def _is_html(self, content_type: str) -> bool:
        lowered = content_type.lower()
        return any(h in lowered for h in _HTML_HINTS) or not lowered

    def _extract_links(self, response, body: bytes) -> list[str]:
        base = find_base_href(body) or response.url
        out: list[str] = []
        seen_on_page: set[str] = set()
        for href in iter_hrefs(body):
            url = self.filter.normalize(href, base)
            if url and url not in seen_on_page:
                seen_on_page.add(url)
                out.append(url)
        return out

    def _maybe_follow(self, url: str, response):
        """Decide what to do with a URL found on a page.

        Order matters and is deliberate:

          1. per-domain limits, plain dict lookups
          2. sharding, one md5
          3. the Bloom filter, which RECORDS the URL as seen

        The filter has to come last because it is the only step with a side
        effect. A URL rejected by a limit stays unseen and can be rediscovered
        once that domain drains; a URL added to the filter never comes back.

        Sharding sits before the filter for the same reason. A URL another
        shard owns must not be recorded here, or this process would suppress
        it while never fetching it.
        """
        key = slot_key(url)

        # Every URL that survives filtering counts as discovered, even when we
        # choose not to fetch it, and even when it belongs to another shard.
        # The metric is discovery, not fetching, and each URL reaches this
        # line in exactly one process.
        self.discovered_raw += 1

        # .get() rather than [key]: defaultdict.__getitem__ inserts, so
        # indexing here would grow both dicts with every DISCOVERED domain
        # instead of every crawled one.
        if self.crawled_per_domain.get(key, 0) >= self.max_crawled_per_domain:
            return
        if self.queued_per_domain.get(key, 0) >= self.max_queued_per_domain:
            return

        if self.shards > 1:
            owner = shard_of(key, self.shards)
            if owner != self.shard:
                self.handoff.send(url, owner)
                return

        request = self._admit(url, key)
        if request is not None:
            yield request

    def _admit(self, url: str, key: str) -> Request | None:
        """Turn a URL this shard owns into a Request, or refuse it.

        Shared by page links and by URLs handed over from another shard, so
        both face the same duplicate filter and the same logging.
        """
        # Test the filter here rather than let the scheduler do it. Three
        # quarters of yielded requests are duplicates, and reaching the
        # scheduler means each one was allocated, passed through the spider
        # middleware chain and handed to the engine before being dropped.
        # A measured 28 minutes spent that on 3,319,826 requests.
        if self.dupefilter is not None:
            if self.dupefilter.url_seen(url):
                self.dupe_skipped += 1
                return None
            # Already recorded above, so the scheduler must not test again.
            dont_filter = True
        else:
            dont_filter = False

        # Written after the filter, so this log holds distinct URLs only. The
        # raw count goes to the stats dump as discovered/raw.
        self.discovered_log.write(url)

        is_new_domain = key not in self.seen_domains
        if is_new_domain:
            self.seen_domains[key] = True
            self.domains_seen_total += 1

        self.queued_per_domain[key] += 1
        # No download_slot. crawler/downloader.py derives it from the URL, so
        # putting it in meta would only create a second copy that a request
        # built from this one could carry to a different domain.
        meta = {}
        if is_new_domain:
            # CappedScheduler lets this past a full frontier. See its
            # enqueue_request: domain count is what bounds throughput.
            meta["new_domain"] = True
        return Request(url, callback=self.parse, meta=meta, dont_filter=dont_filter)
