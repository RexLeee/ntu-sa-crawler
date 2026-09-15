"""Broad crawl spider.

Throughput is bounded by active_domains / delay, not by machine speed, so the
spider's main job is to keep discovering new domains and to stop any single
domain from monopolising the frontier.
"""

from __future__ import annotations

import hashlib
import logging
from collections import defaultdict

from scrapy import Request, Spider, signals
from scrapy.exceptions import CloseSpider

from crawler.config import PROJECT_ROOT, data_dir, load_config
from crawler.dispatch import DISPATCH_TIME
from crawler.dispatch import install as install_dispatch_timer
from crawler.extract import find_base_href, iter_hrefs
from crawler.filters import UrlFilter
from crawler.logwriter import GzipLineWriter, now
from crawler.slot import slot_key

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
        install_dispatch_timer(self.cfg["politeness"]["required_min_gap"])
        self.filter = UrlFilter(self.cfg)

        limits = self.cfg["limits"]
        self.max_crawled_per_domain: int = limits["max_crawled_per_domain"]
        self.max_queued_per_domain: int = limits["max_queued_per_domain"]

        # Set in from_crawler. Checking the filter here rather than letting the
        # scheduler do it avoids building a Request for a URL we have seen.
        self.dupefilter = None
        self.dupe_skipped = 0
        self.discovered_raw = 0

        self.shard = int(shard)
        self.shards = int(shards)

        # -a seeds=... wins, then the production list, then the smoke list.
        seeds_path = seeds or self.cfg["seeds"]["seeds_file"]
        self.seeds_path = (PROJECT_ROOT / seeds_path).resolve()

        # Per-domain bookkeeping. Bounded by the number of domains we touch.
        self.crawled_per_domain: dict[str, int] = defaultdict(int)
        self.queued_per_domain: dict[str, int] = defaultdict(int)
        self.seen_domains: set[str] = set()

        d = data_dir()
        suffix = f"-{self.shard}" if self.shards > 1 else ""
        self.crawled_log = GzipLineWriter(d / f"crawled{suffix}.log.gz")
        self.discovered_log = GzipLineWriter(d / f"discovered{suffix}.log.gz")

    # --- lifecycle ----------------------------------------------------------

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super().from_crawler(crawler, *args, **kwargs)
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
        # BloomDupeFilter publishes itself here in its own from_crawler.
        spider.dupefilter = getattr(crawler, "bloom_dupefilter", None)
        spider.crawler.signals.connect(spider._record_stats, signal=signals.spider_closed)
        return spider

    def _record_stats(self, spider, reason) -> None:
        """Put the spider-side counters where the stats dump can see them."""
        stats = self.crawler.stats
        stats.set_value("discovered/raw", self.discovered_raw)
        stats.set_value("discovered/unique", self.discovered_log.count)
        stats.set_value("dupefilter/skipped_before_request", self.dupe_skipped)
        stats.set_value("domains/seen", len(self.seen_domains))

    def _release_queued(self, key: str) -> None:
        remaining = self.queued_per_domain.get(key, 0) - 1
        if remaining > 0:
            self.queued_per_domain[key] = remaining
        else:
            # Drop the key entirely so the dict tracks live domains only.
            self.queued_per_domain.pop(key, None)

    def _on_request_settled(self, request, spider) -> None:
        self._release_queued(request.meta.get("download_slot") or slot_key(request.url))

    def start_requests(self):
        yield from self._seed_requests()

    async def start(self):
        # Scrapy >= 2.13 prefers the async start(); keep both for compatibility.
        for req in self._seed_requests():
            yield req

    def _seed_requests(self):
        if not self.seeds_path.exists():
            raise CloseSpider(f"seeds file not found: {self.seeds_path}")

        count = 0
        for line in self.seeds_path.read_text(encoding="utf-8").splitlines():
            url = line.strip()
            if not url or url.startswith("#"):
                continue
            key = slot_key(url)
            if self.shards > 1 and self._shard_of(key) != self.shard:
                continue
            self.seen_domains.add(key)
            count += 1
            yield Request(
                url,
                callback=self.parse,
                meta={"download_slot": key, "seed": True},
                dont_filter=False,
            )
        logger.info("loaded %d seeds from %s", count, self.seeds_path)

    def closed(self, reason):
        self.crawled_log.close()
        self.discovered_log.close()
        logger.info(
            "closed(%s): crawled=%d discovered=%d domains=%d",
            reason,
            self.crawled_log.count,
            self.discovered_log.count,
            len(self.seen_domains),
        )

    # --- parsing ------------------------------------------------------------

    def parse(self, response):
        key = response.meta.get("download_slot") or slot_key(response.url)
        ctype = response.headers.get(b"Content-Type", b"").decode("latin-1", "replace")

        body = response.body
        links = self._extract_links(response, body) if self._is_html(ctype) else []

        # DOWNLOAD_DELAY spaces request starts apart, so compliance is measured
        # on dispatch time. See crawler/dispatch.py for why this is the only
        # correct source for it.
        recv = now()
        sent = float(response.meta.get(DISPATCH_TIME, recv))
        self.crawled_log.write(
            f"{sent:.3f}",
            f"{recv:.3f}",
            response.url,
            response.status,
            ctype.split(";")[0],
            len(links),
        )
        self.crawled_per_domain[key] += 1
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
        """Decide whether to fetch a discovered URL, cheapest test first.

        Order matters and is deliberate:

          1. per-domain limits, plain dict lookups
          2. sharding, one md5
          3. the Bloom filter, which RECORDS the URL as seen

        The filter has to come last because it is the only step with a side
        effect. A URL rejected by a limit stays unseen and can be rediscovered
        once that domain drains; a URL added to the filter never comes back.
        """
        key = slot_key(url)

        # Every URL that survives filtering counts as discovered, even when we
        # choose not to fetch it. The metric is discovery, not fetching.
        self.discovered_raw += 1

        # .get() rather than [key]: defaultdict.__getitem__ inserts, so
        # indexing here would grow both dicts with every DISCOVERED domain
        # instead of every crawled one.
        if self.crawled_per_domain.get(key, 0) >= self.max_crawled_per_domain:
            return
        if self.queued_per_domain.get(key, 0) >= self.max_queued_per_domain:
            return
        if self.shards > 1 and self._shard_of(key) != self.shard:
            return  # another worker owns this domain

        # Test the filter here rather than let the scheduler do it. Three
        # quarters of yielded requests are duplicates, and reaching the
        # scheduler means each one was allocated, passed through the spider
        # middleware chain and handed to the engine before being dropped.
        # A measured 28 minutes spent that on 3,319,826 requests.
        if self.dupefilter is not None:
            if self.dupefilter.url_seen(url):
                self.dupe_skipped += 1
                return
            # Already recorded above, so the scheduler must not test again.
            dont_filter = True
        else:
            dont_filter = False

        # Written after the filter, so this log holds distinct URLs only. The
        # raw count goes to the stats dump as discovered/raw.
        self.discovered_log.write(url)

        is_new_domain = key not in self.seen_domains
        if is_new_domain:
            self.seen_domains.add(key)

        self.queued_per_domain[key] += 1
        meta = {"download_slot": key}
        if is_new_domain:
            # CappedScheduler lets this past a full frontier. See its
            # enqueue_request: domain count is what bounds throughput.
            meta["new_domain"] = True
        yield Request(url, callback=self.parse, meta=meta, dont_filter=dont_filter)

    def _shard_of(self, key: str) -> int:
        """Assign a domain to a worker. Stable across processes, unlike hash().

        Hashes the whole key rather than its first 8 bytes. Domains share
        prefixes heavily, so truncating skews the split. Measured over 5,994
        real domains, max/min load per shard:

            shards   first 8 bytes   md5
                 4          1.263x   1.062x
                 6          1.171x   1.099x
                 8          2.012x   1.124x

        At 8 workers, the value this project would use, one process would
        otherwise carry twice the load of another while cores sit idle.
        """
        digest = hashlib.md5(key.encode("utf-8"), usedforsecurity=False).digest()
        return int.from_bytes(digest[:8], "big") % self.shards
