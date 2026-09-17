"""Scheduler that bounds the frontier, the queue count, and the cost of a kill.

Scrapy has no setting that caps the queue: enqueue_request rejects a request
only when the dupefilter has seen it. In a breadth-first broad crawl that is
not enough. Each page yields around 50 links but costs one domain five
seconds of budget, so the frontier fills roughly 33 times faster than it
drains and memory tracks it linearly.

A measured hour: 40,773 pages crawled against 1.4M URLs discovered, with RSS
rising 1.6 MB/s and no sign of flattening.

Three separate limits, three separate resources
-----------------------------------------------
Capping the number of queued REQUESTS was not enough, because the binding
resource is the number of per-domain QUEUES, and those are not the same thing.
A measured 2 hour run pinned the frontier at 3,000,810 requests and still grew
from 46,788 to 132,779 queues, because a new domain's first URL is exempt from
the request cap. RSS, file descriptors and disk all tracked the queue count,
not the request count.

  * FRONTIER_MAX_SIZE bounds queued requests. It bounds disk bytes and the
    pickled request objects.
  * PQUEUE_MAX bounds live per-domain queues. Each one costs two open files,
    their kernel structures, and a directory on disk, so this is what bounds
    RSS, descriptors and inode pressure.
  * limits.max_queued_per_domain bounds one domain's share, so a single busy
    domain cannot consume either budget.

What a drop costs, precisely
----------------------------
A dropped URL is gone. The spider's _admit() records it in the Bloom filter
before it builds the Request, so a URL this scheduler refuses will be filtered
out if it is ever linked again.

Neither reported metric suffers for it:

  * discovered/unique is written in _admit(), before the scheduler sees the
    request, so a drop does not reduce it.
  * crawled is bounded by active domains / delay, not by frontier depth. A
    measured hour dequeued 6.6% of what it enqueued, so the URLs beyond the
    cap were never going to be fetched in the time available.

Surviving a kill
----------------
Scrapy persists the set of active per-domain queues to requests.queue/
active.json, and it writes that file only from Scheduler.close(). A shard that
is OOM-killed therefore resumes with no queues at all, and because
ScrapyPriorityQueue closes any queue that reports empty, and queuelib's
close() deletes the chunks of an empty queue, resuming would DELETE the
frontier rather than merely ignore it. A measured run lost a 2.6 GB frontier
that way.

open() rebuilds active.json from the directories on disk instead of trusting
the file, and checkpoint() saves each queue's metadata on a timer. See
crawler/squeue.py for the other half of the recovery.
"""

from __future__ import annotations

import hashlib
import json
import logging
import time
from pathlib import Path

from scrapy.core.scheduler import Scheduler

logger = logging.getLogger(__name__)


class CappedScheduler(Scheduler):
    """Bounds the frontier and the queue count, and resumes after a kill.

    The frontier size is tracked incrementally rather than read from len(self).
    Scheduler.__len__ sums len() over every per-domain queue, so it costs
    O(domains) and runs on every enqueue. Measured on this machine:

        1,000 domains      33 us     30,043 enqueues/s
       10,000 domains     333 us      3,001 enqueues/s
      100,000 domains   3,371 us        297 enqueues/s

    One page yields about 49 requests, so 100 pages/s needs 4,900 enqueues/s.
    A run that had reached 12,269 domains was therefore capped near 61 pages/s
    by this method alone.
    """

    @classmethod
    def from_crawler(cls, crawler):
        obj = super().from_crawler(crawler)
        obj._cap = crawler.settings.getint("FRONTIER_MAX_SIZE", 3_000_000)
        obj._queue_cap = crawler.settings.getint("PQUEUE_MAX", 40_000)
        # Ceiling for the new_domain exemption. See enqueue_request.
        obj._exempt_cap = int(
            obj._cap * crawler.settings.getfloat("FRONTIER_EXEMPT_SLACK", 1.1)
        )
        obj._checkpoint_interval = crawler.settings.getfloat(
            "CHECKPOINT_INTERVAL", 0.0
        )
        # A whole-frontier save blocked the reactor for 25s, so one pass is
        # spread over this many ticks. See _checkpoint_slice.
        obj._slices = max(1, crawler.settings.getint("CHECKPOINT_SLICES", 1))
        obj._checkpoint_cursor = 0
        obj._size = 0
        obj._warned = False
        obj._queue_warned = False
        obj._checkpoint_loop = None
        # The runstats extension reads _size and the per-domain queue count
        # from here rather than reaching into engine internals.
        crawler.capped_scheduler = obj
        return obj

    def has_pending_requests(self) -> bool:
        # The parent computes len(self) > 0, which sums over every per-domain
        # queue. It runs on every idle check, so use the tracked size.
        return self._size > 0

    def open(self, spider):
        # Rebuild the queue index before the parent reads it. Scrapy writes
        # active.json only on a clean close, so after a kill the file is
        # absent or stale while the queues themselves are on disk.
        if self.dqdir:
            self._rebuild_dqs_state(self.dqdir)
        result = super().open(spider)
        # A resumed run starts with whatever the JOBDIR held. This is the one
        # place the O(domains) count is acceptable: it runs once.
        self._size = len(self)
        if self._size:
            logger.info(
                "resumed a frontier of %d request(s) across %d domain queue(s)",
                self._size,
                self._live_queue_count(),
            )
        self._start_checkpoint()
        return result

    def close(self, reason: str):
        if self._checkpoint_loop is not None:
            if getattr(self._checkpoint_loop, "running", False):
                self._checkpoint_loop.stop()
            self._checkpoint_loop = None
        # The parent writes active.json here. That write is still useful, it
        # just cannot be relied upon, so open() rebuilds the file regardless.
        return super().close(reason)

    # --- resuming after a kill ----------------------------------------------

    def _rebuild_dqs_state(self, dqdir: str) -> None:
        """Write active.json from the per-domain directories that exist.

        DownloaderAwarePriorityQueue stores each domain under
        requests.queue/<_path_safe(slot)>/<priority>/, and _path_safe appends
        the md5 of the slot to a sanitised copy of it. That hash is what makes
        the directory name reversible: split at the last '-', and if the md5 of
        the left side equals the right side, the left side IS the slot.

        Every registered domain is made of [A-Za-z0-9.-], so sanitising
        changes nothing and the check always passes. An IDN or an unusual host
        can fail it, and such a directory is skipped with a warning rather
        than guessed at: a wrong slot name would give that domain a second
        queue under a different key.
        """
        root = Path(dqdir)
        if not root.is_dir():
            return

        state: dict[str, list[int]] = {}
        unreadable = 0
        for entry in root.iterdir():
            if not entry.is_dir():
                continue
            slot = self._slot_from_dirname(entry.name)
            if slot is None:
                unreadable += 1
                continue
            prios = [
                int(p.name)
                for p in entry.iterdir()
                if p.is_dir() and p.name.lstrip("-").isdigit() and any(p.glob("q[0-9]*"))
            ]
            if prios:
                state[slot] = sorted(prios)

        if unreadable:
            logger.warning(
                "%d frontier director(ies) had an unreadable slot name and were "
                "skipped; their requests stay on disk unqueued",
                unreadable,
            )
        if not state:
            return

        # Written the same way Scheduler._write_dqs_state writes it, so the
        # parent's _read_dqs_state loads it unchanged.
        path = root / "active.json"
        tmp = path.with_suffix(".json.tmp")
        tmp.write_text(json.dumps(state), encoding="utf-8")
        tmp.replace(path)
        logger.info(
            "rebuilt %s from disk: %d domain queue(s)", path, len(state)
        )

    @staticmethod
    def _slot_from_dirname(name: str) -> str | None:
        """Invert scrapy.pqueues._path_safe, or return None if it cannot be."""
        head, _, digest = name.rpartition("-")
        if not head or len(digest) != 32:
            return None
        if hashlib.md5(head.encode("utf8"), usedforsecurity=False).hexdigest() != digest:
            return None
        return head

    # --- checkpointing ------------------------------------------------------

    def _start_checkpoint(self) -> None:
        if self.dqs is None or self._checkpoint_interval <= 0:
            return
        from scrapy.utils.asyncio import create_looping_call

        # One pass is split across _slices ticks, so the loop has to fire that
        # many times per interval to still cover every queue once per interval.
        self._checkpoint_loop = create_looping_call(self._checkpoint_slice)
        period = self._checkpoint_interval / self._slices
        self._checkpoint_loop.start(period, now=False)

    def _iter_savable(self):
        """Yield every queue that can persist its own metadata, in a stable order."""
        for pq in list(getattr(self.dqs, "pqueues", {}).values()):
            for attr in ("queues", "_start_queues"):
                for queue in list(getattr(pq, attr, {}).values()):
                    if getattr(queue, "save_info", None) is not None:
                        yield queue

    def _checkpoint_slice(self) -> None:
        """Save one slice of the live queues, resuming where the last tick stopped.

        Saving all 40,000 queues in one call blocked the reactor for 24.9 to
        27.2 seconds, measured as checkpoint/frontier_ms, and the reactor lag
        samples at those moments showed 27.8 to 36.4 second stalls while
        gc_max_ms was only 82 to 215 ms. So the cost is the synchronous writes
        themselves, not garbage collection and not CPU contention.

        That matters because DOWNLOAD_TIMEOUT is 10 seconds: any socket whose
        timer expires inside the stall fails spuriously when the reactor
        resumes. Slicing keeps each tick short while still covering every
        queue once per checkpoint_interval.

        Politeness is unaffected either way: the delay is anchored to the
        previous response's completion, so a pause can only widen a gap. A
        failure must not take the crawl down, since the checkpoint exists to
        make a crash cheaper.
        """
        started = time.monotonic()
        saved = 0
        try:
            queues = list(self._iter_savable())
            total = len(queues)
            if total:
                # Cursor walks the whole list across _slices ticks. The set of
                # queues changes between ticks, so this is a best-effort sweep
                # rather than an exact partition: a queue missed on one pass is
                # picked up on the next, and its recovery scan just covers a
                # little more than one interval.
                per_tick = max(1, (total + self._slices - 1) // self._slices)
                start = self._checkpoint_cursor % total
                for i in range(min(per_tick, total)):
                    queues[(start + i) % total].save_info()
                    saved += 1
                self._checkpoint_cursor = (start + saved) % total
        except Exception as exc:
            logger.warning("frontier checkpoint failed: %s", exc)
        if self.stats is not None:
            elapsed_ms = (time.monotonic() - started) * 1000.0
            self.stats.max_value("checkpoint/frontier_slice_ms", elapsed_ms)
            self.stats.inc_value("checkpoint/frontier_queues", saved)

    def checkpoint(self) -> None:
        """Save every live queue's metadata in one pass.

        _checkpoint_slice is what the periodic loop calls; this is the
        whole-frontier version, used at close and by the tests.
        """
        started = time.monotonic()
        saved = 0
        try:
            for queue in self._iter_savable():
                queue.save_info()
                saved += 1
        except Exception as exc:
            logger.warning("frontier checkpoint failed: %s", exc)
        if self.stats is not None:
            elapsed_ms = (time.monotonic() - started) * 1000.0
            self.stats.max_value("checkpoint/frontier_ms", elapsed_ms)
            self.stats.set_value("checkpoint/frontier_queues", saved)

    # --- admission ----------------------------------------------------------

    def _live_queue_count(self) -> int:
        return len(getattr(self.dqs, "pqueues", {}) or {})

    def enqueue_request(self, request) -> bool:
        # Bound the number of per-domain queues, which is what actually bounds
        # memory, descriptors and disk directories. The test is "would this
        # open a new queue", not "is this a new domain": a domain whose queue
        # drained had it closed and removed, so its next URL opens a new one.
        if self.dqs is not None and self._live_queue_count() >= self._queue_cap:
            slot = self.dqs._downloader_interface.get_slot_key(request)
            if slot not in self.dqs.pqueues:
                if not self._queue_warned:
                    logger.info(
                        "frontier holds %d domain queues; refusing new domains "
                        "until some drain",
                        self._queue_cap,
                    )
                    self._queue_warned = True
                if self.stats is not None:
                    self.stats.inc_value("scheduler/dropped/over_queues")
                return False

        # Throughput is capped at active_domains / delay, so the first URL of
        # an unseen domain is worth more than any number of extra pages on a
        # domain already held. Dropping it because the frontier is full would
        # freeze domain growth, which is the one curve that sets the ceiling.
        #
        # The exemption is bounded by PQUEUE_MAX above, which is what the
        # earlier version of this comment was missing: on its own the
        # exemption let the queue count grow without limit, and that is what
        # exhausted memory in a measured run.
        #
        # It also needs a ceiling of its own. The spider decides new_domain
        # from process memory, so both a restart and an eviction from its
        # bounded domain cache re-open the exemption for domains already in
        # the frontier. A 2 hour run overshot by only 339 requests of
        # 3,000,000, but that run never restarted; the count of domains that
        # could claim the exemption again is the whole cache, so cap the
        # overshoot rather than trusting the spider's bookkeeping to survive.
        exempt = request.meta.get("new_domain") and self._size < self._exempt_cap
        if self._size >= self._cap and not exempt:
            if not self._warned:
                logger.info(
                    "frontier reached %d requests; dropping new ones until it drains",
                    self._cap,
                )
                self._warned = True
            if self.stats is not None:
                self.stats.inc_value("scheduler/dropped/over_capacity")
                if request.meta.get("new_domain"):
                    # Separated so the report can tell a normal cap hit from
                    # the exemption ceiling being reached.
                    self.stats.inc_value("scheduler/dropped/over_exempt_cap")
            # Returning False makes the engine fire request_dropped, which is
            # the documented contract for BaseScheduler.enqueue_request.
            return False
        if super().enqueue_request(request):
            self._size += 1
            return True
        return False

    def next_request(self):
        request = super().next_request()
        if request is not None:
            self._size -= 1
        return request
