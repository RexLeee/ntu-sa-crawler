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
import os
import time
import traceback

import scrapy.core.downloader as _dl

from crawler.slot import slot_key

DISPATCH_TIME = "dispatch_time"

_installed = False


def _open_trace(path: str):
    """Open the diagnostic log, or return None if it cannot be written.

    Diagnostics must never take the crawl down, so a failure here is silent
    and simply disables tracing.
    """
    try:
        directory = os.path.dirname(path)
        if directory:
            os.makedirs(directory, exist_ok=True)
        return open(path, "a", buffering=1, encoding="utf-8")
    except OSError:
        return None


def install(min_gap: float = 0.0, trace_path: str | None = None) -> None:
    """Stamp meta['dispatch_time'] and hold each slot to min_gap seconds.

    A min_gap of 0 only measures, which is what the dispatch timing test needs
    when it exercises Scrapy's own delay.

    When trace_path is set, every dispatch closer than min_gap to the previous
    one on the same slot is dumped there with the state that produced it. A 10
    minute run put two panasonic.jp requests at the identical timestamp and
    none of the offline reproductions could recreate it, so the remaining way
    to find the cause is to record it as it happens. The check is two dict
    operations per dispatch and only writes when a gap is short, so it is
    cheap enough to leave on for the full 48 hours.
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

    # Diagnostics. last_dispatch records what actually went out per slot, which
    # is what the compliance check measures; next_allowed records what was
    # promised. A short gap means those two disagree, and the dump says how.
    trace_fh = _open_trace(trace_path) if trace_path else None
    last_dispatch: dict[str, tuple[float, str, float]] = {}

    def _trace_short_gap(downloader, slot, request, slot_id, claimed_turn, dispatched):
        previous = last_dispatch.get(slot_id)
        last_dispatch[slot_id] = (dispatched, request.url, claimed_turn)
        if previous is None:
            return
        gap = dispatched - previous[0]
        if gap >= min_gap - 0.05:
            return
        interesting = (
            "download_slot",
            "new_domain",
            "seed",
            "depth",
            "redirect_urls",
            "redirect_times",
            "dont_obey_robotstxt",
        )
        meta = {k: v for k, v in request.meta.items() if k in interesting}
        trace_fh.write(
            f"=== SHORT GAP {gap:.6f}s slot={slot_id!r} min_gap={min_gap}\n"
            f"  prev  dispatched={previous[0]:.6f} claimed_turn={previous[2]:.6f}"
            f" url={previous[1]}\n"
            f"  curr  dispatched={dispatched:.6f} claimed_turn={claimed_turn:.6f}"
            f" url={request.url}\n"
            f"  next_allowed[slot]={next_allowed.get(slot_id, float('nan')):.6f}"
            f"  dict_size={len(next_allowed)}\n"
            f"  slot obj={id(slot)} delay={getattr(slot, 'delay', '?')}"
            f" concurrency={getattr(slot, 'concurrency', '?')}"
            f" lastseen={getattr(slot, 'lastseen', '?')}\n"
            f"  len(active)={len(getattr(slot, 'active', ()))}"
            f" len(transferring)={len(getattr(slot, 'transferring', ()))}"
            f" len(queue)={len(getattr(slot, 'queue', ()))}\n"
            f"  slot_is_live={getattr(downloader, 'slots', {}).get(slot_id) is slot}\n"
            f"  meta={meta}\n"
            f"  stack:\n"
        )
        for frame in traceback.format_stack(limit=20):
            trace_fh.write("    " + frame.rstrip().replace("\n", "\n    ") + "\n")
        trace_fh.write("\n")

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
            #
            # max(), not a plain assignment. While this coroutine slept, a
            # sibling on the same slot may have claimed a later turn and
            # written it here. Overwriting that with dispatched + min_gap
            # would hand the sibling's reserved turn to the next arrival, and
            # the two would go out together. Only ever move the reservation
            # forward.
            dispatched = time.time()
            next_allowed[slot_id] = max(
                next_allowed.get(slot_id, 0.0), dispatched + min_gap
            )

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
            if trace_fh is not None:
                _trace_short_gap(self, slot, request, slot_id, turn, dispatched)
        else:
            request.meta[DISPATCH_TIME] = time.time()
        return await original(self, slot, request)

    _dl.Downloader._download = _download_with_stamp
    _installed = True
