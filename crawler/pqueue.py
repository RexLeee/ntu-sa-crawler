"""A priority queue whose pop() does not scan every domain.

DownloaderAwarePriorityQueue.pop() does three O(domains) things on every call:

  * DownloaderInterface.stats() builds an (active, slot) tuple per queue
  * _active_downloads() looks each slot up in downloader.slots twice
  * _next_slot() scans that whole list for the minimum

pop() is called once per dispatch, roughly 100-200 times a second. At the
5,000 per-domain queues a broad crawl reaches, that is half a million tuple
allocations a second. A py-spy profile of a 2 hour run put the three together
at 52% of all CPU on the reactor thread, against 5% for parse(). Reactor lag
sat at 1.2-2.0s at the median, which is why TLS handshakes were timing out on
hosts that were in fact answering.

The selection rule DownloaderAwarePriorityQueue implements is "fewest active
downloads first, ties broken by rotating through slot names in order". With
CONCURRENT_REQUESTS_PER_DOMAIN=1 almost every queue has 0 or 1 active
downloads, so in practice the rule is "find a domain with nothing in flight".
Finding one does not require looking at all of them.

This subclass keeps the domains in a deque and stops at the first one with no
active downloads, moving what it skips to the back. The expected scan length
is close to 1 because a skipped domain is not looked at again until every
other domain has had a turn.

The scan is capped (PQUEUE_SCAN_LIMIT). On reaching the cap it falls back to
the least-active domain it saw, which is the parent's answer restricted to a
window. What it must never do is return None while queues hold requests:
Scheduler.has_pending_requests() reports on __len__, and the engine would spin
on a queue that never yields anything.
"""

from __future__ import annotations

import logging
from collections import deque
from contextlib import suppress
from pathlib import Path

from scrapy.pqueues import DownloaderAwarePriorityQueue, _path_safe

logger = logging.getLogger(__name__)


class RingDownloaderAwarePriorityQueue(DownloaderAwarePriorityQueue):
    """DownloaderAwarePriorityQueue with an O(1) expected pop()."""

    def __init__(self, crawler, downstream_queue_cls, key, slot_startprios=None, **kw):
        # The ring has to exist before the parent runs: its __init__ builds a
        # queue per entry in slot_startprios when a crawl is resumed.
        self._ring: deque[str] = deque()
        self._ring_members: set[str] = set()
        self._scan_limit: int = crawler.settings.getint("PQUEUE_SCAN_LIMIT", 256)
        super().__init__(crawler, downstream_queue_cls, key, slot_startprios, **kw)
        for slot in self.pqueues:
            self._ring_add(slot)

    def _ring_add(self, slot: str) -> None:
        if slot not in self._ring_members:
            self._ring_members.add(slot)
            self._ring.append(slot)

    def push(self, request) -> None:
        slot = self._downloader_interface.get_slot_key(request)
        if slot not in self.pqueues:
            self.pqueues[slot] = self.pqfactory(slot)
        self.pqueues[slot].push(request)
        self._ring_add(slot)

    def _drop_queue(self, slot: str) -> None:
        """Close an emptied queue, mirroring the parent's disk cleanup.

        The ring entry is deliberately left in place. Removing it would cost
        O(len(ring)); instead it becomes stale and is dropped the next time
        pop() walks past it. _ring_members keeps the slot so a re-push does
        not append a second copy: it revives the stale entry instead.
        """
        queue = self.pqueues.pop(slot)
        queue.close()
        if self.key:
            with suppress(OSError):
                Path(self.key, _path_safe(slot)).rmdir()

    def _select_slot(self, *, consume: bool) -> str | None:
        """Return the slot to serve next, or None when the ring holds none.

        Walks the ring from the front. The first slot with no active downloads
        wins immediately. Anything skipped rotates to the back, so the next
        call starts where this one left off and no slot is starved.

        Returning None means every ring entry was stale, not that the frontier
        is empty. pop() handles that case; it must not become a None to the
        engine while self.pqueues still holds requests.
        """
        best_slot: str | None = None
        best_active: int | None = None
        scanned = 0
        active_downloads = self._downloader_interface._active_downloads

        # Bounded by the ring length so a ring of stale entries terminates.
        for _ in range(len(self._ring)):
            if scanned >= self._scan_limit:
                break
            slot = self._ring[0]
            if slot not in self.pqueues:
                # Stale: the queue emptied and was dropped. Forget it.
                self._ring.popleft()
                self._ring_members.discard(slot)
                continue
            active = active_downloads(slot)
            scanned += 1
            if active == 0:
                best_slot, best_active = slot, 0
                break
            # Busy. Rotate it away so the next call does not start here again.
            self._ring.rotate(-1)
            if best_active is None or active < best_active:
                best_slot, best_active = slot, active

        if best_slot is None:
            return None
        # Move the winner behind everything else so the next pop starts
        # elsewhere. It sits at the front only when it was chosen on an
        # active==0 hit; the fallback winner has already rotated past.
        if consume and self._ring and self._ring[0] == best_slot:
            self._ring.rotate(-1)
        return best_slot

    def pop(self):
        while self.pqueues:
            slot = self._select_slot(consume=True)
            if slot is None:
                # Every ring entry was stale but pqueues is not empty, which
                # means a queue exists with no ring entry. That is a bug in
                # the bookkeeping rather than an empty frontier, so rebuild
                # rather than return None and stall the engine.
                logger.warning(
                    "pqueue ring lost %d queue(s); rebuilding", len(self.pqueues)
                )
                self._ring = deque(self.pqueues)
                self._ring_members = set(self.pqueues)
                continue
            queue = self.pqueues[slot]
            request = queue.pop()
            if len(queue) == 0:
                self._drop_queue(slot)
            return request
        return None

    def peek(self):
        if not self.pqueues:
            return None
        slot = self._select_slot(consume=False)
        if slot is None:
            return None
        return self.pqueues[slot].peek()

    def close(self) -> dict[str, list[int]]:
        active = super().close()
        self._ring.clear()
        self._ring_members.clear()
        return active
