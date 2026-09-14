"""Approximate duplicate filter backed by a Bloom filter.

Scrapy's RFPDupeFilter keeps every 20-byte fingerprint in a Python set, which
costs about 131 bytes per URL once object headers, the hash table slot and
allocator overhead are counted. At the tens of millions of URLs a 48 hour
broad crawl reaches, that set alone would exceed the machine's memory.

A Bloom filter sized for 50M URLs at a 1e-6 false positive rate costs 171 MB
instead of roughly 6.5 GB.

The trade is real and one-directional: a false positive drops a URL that was
never actually crawled. At 1e-6 over 50M URLs that is roughly 50 lost URLs,
which is acceptable when the metric is aggregate volume. It would not be
acceptable if any single URL had to be fetched.

Capacity is a ceiling, not a target. Past it the false positive rate climbs
sharply, so size for the end of the run rather than its current state.
"""

from __future__ import annotations

import logging
from pathlib import Path

from rbloom import Bloom
from scrapy.dupefilters import BaseDupeFilter
from scrapy.utils.job import job_dir

logger = logging.getLogger(__name__)


def _hash128(digest: bytes) -> int:
    """Map a request fingerprint to the signed 128-bit int rbloom expects.

    rbloom defaults to the builtin hash(), which is salted per process by
    PYTHONHASHSEED and only 64 bits wide. A filter built with it cannot be
    reloaded after a restart, so derive the value from the digest instead.
    """
    return int.from_bytes(digest[:16], "big", signed=True)


class BloomDupeFilter(BaseDupeFilter):
    """Drop-in replacement for RFPDupeFilter with bounded memory."""

    def __init__(
        self,
        path: str | None = None,
        capacity: int = 50_000_000,
        error_rate: float = 1e-6,
        debug: bool = False,
        *,
        fingerprinter=None,
    ):
        self.fingerprinter = fingerprinter
        self.debug = debug
        self.logdupes = True
        self._path = Path(path, "requests.bloom") if path else None

        if self._path and self._path.exists():
            self.bloom = Bloom.load(str(self._path), _hash128)
            logger.info("loaded bloom filter holding ~%d items", self.bloom.approx_items)
        else:
            self.bloom = Bloom(capacity, error_rate, _hash128)
            logger.info("new bloom filter: capacity=%d error_rate=%g", capacity, error_rate)

    @classmethod
    def from_crawler(cls, crawler):
        # from_settings was removed in Scrapy 2.12; from_crawler is the only hook.
        settings = crawler.settings
        return cls(
            job_dir(settings),
            capacity=settings.getint("BLOOM_DUPEFILTER_CAPACITY", 50_000_000),
            error_rate=settings.getfloat("BLOOM_DUPEFILTER_ERROR_RATE", 1e-6),
            debug=settings.getbool("DUPEFILTER_DEBUG"),
            fingerprinter=crawler.request_fingerprinter,
        )

    def request_seen(self, request) -> bool:
        fp = self.fingerprinter.fingerprint(request)
        if fp in self.bloom:
            return True
        self.bloom.add(fp)
        return False

    def close(self, reason: str) -> None:
        # Only a clean shutdown persists the filter. A kill -9 loses it, and
        # the crawl would re-fetch what it had already seen.
        if self._path:
            self._path.parent.mkdir(parents=True, exist_ok=True)
            self.bloom.save(str(self._path))
            logger.info("saved bloom filter holding ~%d items", self.bloom.approx_items)

    def log(self, request, spider) -> None:
        # BaseDupeFilter.log raises a deprecation warning, so it must be
        # overridden rather than inherited.
        if self.debug:
            logger.debug("filtered duplicate: %(request)s", {"request": request})
        elif self.logdupes:
            logger.debug(
                "filtered duplicate: %(request)s - further duplicates not logged",
                {"request": request},
            )
            self.logdupes = False
        spider.crawler.stats.inc_value("dupefilter/filtered", spider=spider)
