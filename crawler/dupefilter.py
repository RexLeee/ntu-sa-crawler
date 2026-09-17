"""Approximate duplicate filter backed by a Bloom filter.

Scrapy's RFPDupeFilter keeps every 20-byte fingerprint in a Python set, which
costs about 131 bytes per URL once object headers, the hash table slot and
allocator overhead are counted. At the tens of millions of URLs a 48 hour
broad crawl reaches, that set alone would exceed the machine's memory.

A Bloom filter sized for 100M URLs at a 1e-4 false positive rate costs 240 MB
instead of roughly 13 GB.

The trade is real and one-directional: a false positive drops a URL that was
never actually crawled. At 1e-4 over 100M URLs that is roughly 10,000 lost
URLs, which is acceptable when the metric is aggregate volume. It would not
be acceptable if any single URL had to be fetched.

Capacity is a ceiling, not a target. Past it the false positive rate climbs
sharply, and how sharply depends on the error rate: a tighter rate uses more
hash functions and degrades faster past capacity. See config.toml [memory]
for the measured numbers behind the current pair.

The filter is checkpointed on a timer as well as saved at close, so a kill
loses one interval of dedupe state rather than the whole run.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
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
        capacity: int = 100_000_000,
        error_rate: float = 1e-4,
        debug: bool = False,
        checkpoint_interval: float = 0.0,
        checkpoint_offset: float = 0.0,
        *,
        fingerprinter=None,
    ):
        self.fingerprinter = fingerprinter
        self.debug = debug
        self.logdupes = True
        self._path = Path(path, "requests.bloom") if path else None
        self._checkpoint_interval = float(checkpoint_interval)
        self._checkpoint_offset = float(checkpoint_offset)
        self._checkpoint_loop = None
        # close() may run before the deferred start fires, and a loop started
        # after that would never be stopped.
        self._closed = False

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
        obj = cls(
            job_dir(settings),
            capacity=settings.getint("BLOOM_DUPEFILTER_CAPACITY", 100_000_000),
            error_rate=settings.getfloat("BLOOM_DUPEFILTER_ERROR_RATE", 1e-4),
            debug=settings.getbool("DUPEFILTER_DEBUG"),
            checkpoint_interval=settings.getfloat("CHECKPOINT_INTERVAL", 0.0),
            checkpoint_offset=settings.getfloat("BLOOM_CHECKPOINT_OFFSET", 0.0),
            fingerprinter=crawler.request_fingerprinter,
        )
        # The spider checks URLs against this filter before it builds a
        # Request, so it needs a handle on the same instance.
        crawler.bloom_dupefilter = obj
        return obj

    def open(self) -> None:
        """Start the periodic save. Called by the scheduler at spider open."""
        if self._path is None or self._checkpoint_interval <= 0:
            return
        if self._checkpoint_offset > 0:
            # The frontier saver in crawler/scheduler.py runs on the same
            # thread and the same interval, so without an offset both land on
            # the same tick and their stalls add. Delay the first save by the
            # offset; both then repeat on the same period and stay apart.
            from twisted.internet import reactor

            reactor.callLater(self._checkpoint_offset, self._start_loop)
            return
        self._start_loop()

    def _start_loop(self) -> None:
        from scrapy.utils.asyncio import create_looping_call

        # callLater may fire after the crawl has already closed.
        if self._path is None or self._closed:
            return
        self._checkpoint_loop = create_looping_call(self.checkpoint)
        self._checkpoint_loop.start(self._checkpoint_interval, now=False)

    def checkpoint(self) -> None:
        """Write the filter without stopping the crawl.

        A failure here must not take the crawl down: the checkpoint exists to
        reduce the cost of a crash, so it cannot be allowed to cause one.
        """
        try:
            self._save()
        except Exception as exc:
            logger.warning("bloom checkpoint failed: %s", exc)

    def _save(self) -> None:
        """Persist atomically, so a kill mid-write leaves the previous file.

        rbloom writes the whole filter, 240 MB at the configured capacity. A
        process killed partway through that write would otherwise leave a
        truncated file, and Bloom.load on the next start would fail or, worse,
        succeed on garbage.
        """
        if self._path is None:
            return
        self._path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self._path.with_suffix(".bloom.tmp")
        self.bloom.save(str(tmp))
        os.replace(tmp, self._path)

    def request_seen(self, request) -> bool:
        fp = self.fingerprinter.fingerprint(request)
        if fp in self.bloom:
            return True
        self.bloom.add(fp)
        return False

    def url_seen(self, url: str) -> bool:
        """Test and record a URL that is ALREADY canonical.

        Three quarters of the requests a broad crawl yields are duplicates: a
        measured 28 minutes produced 3,319,826 filtered against 1,106,885
        scheduled. Reaching request_seen means each of those has already been
        allocated as a Request, carried through the spider middleware chain
        and handed to the engine, only to be dropped.

        The fingerprint must match request_fingerprint exactly, or the
        scheduler would filter a second time on a different value and the
        saving would be lost. That function hashes
        sha1(json({method, url, body, headers})) with sorted keys, and for our
        requests method is GET with an empty body and no included headers, so
        only the canonical URL varies. tests/test_dupefilter.py holds the two
        implementations against each other on real URLs.

        The caller must pass the output of UrlFilter.normalize(), which ends in
        canonicalize_url(). This method used to call it a second time, which
        cannot change the value because canonicalize_url is idempotent, and
        cost 10.77 us of the 12.79 us this method took, measured over 20,000
        URLs. Both callers satisfy the precondition: spider _maybe_follow()
        passes a normalize() result directly, and _drain_handoff() passes a
        line from the inbox, which the sending shard wrote from its own
        normalize() result without modifying it. Seeds do not reach here at
        all; they are yielded with dont_filter=False and go through Scrapy's
        own request_seen(), which fingerprints from scratch.
        """
        payload = json.dumps(
            {
                "method": "GET",
                "url": url,
                "body": "",
                "headers": {},
            },
            sort_keys=True,
        )
        fp = hashlib.sha1(payload.encode(), usedforsecurity=False).digest()
        if fp in self.bloom:
            return True
        self.bloom.add(fp)
        return False

    def close(self, reason: str) -> None:
        # A clean shutdown saves here. An unclean one keeps whatever the last
        # checkpoint wrote, which bounds the loss to one interval instead of
        # the whole run.
        self._closed = True
        if self._checkpoint_loop is not None:
            if getattr(self._checkpoint_loop, "running", False):
                self._checkpoint_loop.stop()
            self._checkpoint_loop = None
        if self._path:
            self._save()
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
