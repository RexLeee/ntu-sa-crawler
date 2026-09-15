"""Stamp and enforce the real dispatch time of every request.

Compliance is defined on request spacing as the server sees it, which is the
moment the socket write happens. Two jobs live here, and both need the same
hook.

Measuring
---------
Three nearer-looking hooks are all wrong:

  * Downloader middlewares run at enqueue time, because Downloader.fetch wraps
    _enqueue_request in the middleware chain. Every queued request gets the
    same timestamp.
  * The request_reached_downloader signal fires from _enqueue_request, so it
    has the same problem.
  * meta['download_latency'] covers only the transfer, excluding DNS and
    TCP/TLS setup, so recv - latency lands late and understates the gap.

Downloader._download is the first thing to run after the slot's delay timer
releases a request, which makes it the correct measurement point.

Enforcing
---------
Scrapy's Slot._process_queue sets slot.lastseen to the moment it *schedules*
the download coroutine, then calls _schedule_coro. The coroutine runs whenever
the event loop reaches it. At high concurrency that is not immediate: a
measured run saw the loop blocked for up to 1.4s at a time.

When the first request of a pair is delayed by congestion and the second is
not, the gap the server sees is shorter than the configured delay. A run at
CONCURRENT_REQUESTS=3000 produced 50 such violations, the worst at 4.694s
against a 5.0s limit. Raising DOWNLOAD_DELAY to 5.5s cut that to one at
4.941s: the jitter is a long tail (p50 36ms, p99 302ms, max 559ms), so margin
alone cannot close it.

Re-checking the clock here does. This is the last point before the request
reaches the network, so a gap measured from the previous request's real
dispatch cannot be shortened by anything downstream.

Both behaviours are verified by tests/test_dispatch.py.
"""

from __future__ import annotations

import asyncio
import time

import scrapy.core.downloader as _dl

from crawler.slot import slot_key

DISPATCH_TIME = "dispatch_time"

_installed = False


def install(min_gap: float = 0.0) -> None:
    """Stamp meta['dispatch_time'] and hold each slot to min_gap seconds.

    A min_gap of 0 only measures, which is what the dispatch timing test needs
    when it exercises Scrapy's own delay.
    """
    global _installed
    if _installed:
        return

    original = _dl.Downloader._download

    # The earliest time each slot may dispatch again. Storing the *next*
    # allowed time rather than the last dispatch is what makes this safe under
    # concurrency: each coroutine claims its turn before awaiting, so two
    # coroutines entering together get consecutive turns instead of reading
    # the same "last dispatch" and waking at the same instant.
    #
    # Keyed by the DOMAIN, not by id(slot). Downloader._slot_gc destroys any
    # slot idle for 60s, and CPython reuses the freed address almost
    # immediately: a direct test recycled the id 1,999 times out of 2,000. So
    # id(slot) is neither stable for one domain nor unique across domains.
    # A two hour run produced exactly this failure twice on panasonic.jp, both
    # after gaps of over 120s, which is long enough for the slot to have been
    # collected and rebuilt. The rebuilt slot started from lastseen=0 and its
    # entry here was gone, so two requests dispatched 0.001s apart.
    #
    # The domain string is stable across slot GC and unique between domains,
    # which is exactly the identity the rate limit is defined on.
    next_allowed: dict[str, float] = {}

    async def _download_with_stamp(self, slot, request):
        if min_gap > 0:
            slot_id = request.meta.get(_dl.Downloader.DOWNLOAD_SLOT) or slot_key(
                request.url
            )
            # Claim a turn before awaiting. Without this, coroutines released
            # together all read the same previous time and wake at the same
            # instant; a measured run left two of three gaps at 0.000s.
            turn = max(time.time(), next_allowed.get(slot_id, 0.0))
            next_allowed[slot_id] = turn + min_gap

            # asyncio.sleep guarantees a lower bound, not an exact wake time:
            # the wake-up queues behind whatever the loop is already running.
            # Sleeping once to the claimed turn therefore still lands late by
            # a varying amount, and when the previous request landed later
            # than this one the server sees them too close. Re-checking after
            # each sleep converts that into a real elapsed-time guarantee.
            while True:
                remaining = turn - time.time()
                if remaining <= 0:
                    break
                await asyncio.sleep(remaining)

            # Advance from the moment the request actually goes out, not from
            # the moment it was scheduled to, so lateness never compounds into
            # a short gap for the next one.
            dispatched = time.time()
            next_allowed[slot_id] = dispatched + min_gap

            # Bound the dict. Entries whose gap has already elapsed can never
            # constrain a future request, so dropping them is safe: the next
            # request for that domain claims max(now, missing) == now, which
            # is the same answer the entry would have given.
            #
            # This must NOT evict by "is the slot still live". A collected
            # slot is exactly the case that caused the violation: the domain
            # comes back, and its gap has to come back with it.
            if len(next_allowed) > 100_000:
                cutoff = dispatched
                for key in [k for k, v in next_allowed.items() if v <= cutoff]:
                    del next_allowed[key]

            request.meta[DISPATCH_TIME] = dispatched
        else:
            request.meta[DISPATCH_TIME] = time.time()
        return await original(self, slot, request)

    _dl.Downloader._download = _download_with_stamp
    _installed = True
