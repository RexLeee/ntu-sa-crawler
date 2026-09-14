"""Broad crawl spider.

Throughput is bounded by active_domains / delay, not by machine speed, so the
spider's main job is to keep discovering new domains and to stop any single
domain from monopolising the frontier.
"""

from __future__ import annotations

import logging
from collections import defaultdict

from scrapy import Request, Spider
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
        install_dispatch_timer()
        self.cfg = load_config()
        self.filter = UrlFilter(self.cfg)

        limits = self.cfg["limits"]
        self.max_crawled_per_domain: int = limits["max_crawled_per_domain"]
        self.max_queued_per_domain: int = limits["max_queued_per_domain"]
        self.new_domain_bonus: int = limits["new_domain_priority_bonus"]

        self.shard = int(shard)
        self.shards = int(shards)

        seeds_path = seeds or self.cfg["test"]["seeds_file"]
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
                priority=self.new_domain_bonus,
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
        key = slot_key(url)

        # Every URL that survives filtering counts as discovered, even when we
        # choose not to fetch it. The metric is discovery, not fetching.
        self.discovered_log.write(url)

        if self.crawled_per_domain[key] >= self.max_crawled_per_domain:
            return
        if self.queued_per_domain[key] >= self.max_queued_per_domain:
            return
        if self.shards > 1 and self._shard_of(key) != self.shard:
            return  # another worker owns this domain

        is_new_domain = key not in self.seen_domains
        if is_new_domain:
            self.seen_domains.add(key)

        self.queued_per_domain[key] += 1
        yield Request(
            url,
            callback=self.parse,
            meta={"download_slot": key},
            # A new domain opens another parallel 5s pipeline, so it is worth
            # more than another page on a domain we already hold.
            priority=self.new_domain_bonus if is_new_domain else 0,
        )

    def _shard_of(self, key: str) -> int:
        # Stable across processes, unlike hash().
        return int.from_bytes(key.encode("utf-8")[:8].ljust(8, b"\0"), "big") % self.shards
