"""Record the real dispatch time of every request, and refuse a short one.

This module used to be the rate limiter. It is now the measurement point and
a last-resort guard; crawler/downloader.py holds the limiter. The move is
worth stating because the history explains three of the four politeness bugs
this crawl has had.

Why the timer moved out
-----------------------
The old design let this module claim a turn per request and honour it with
`await asyncio.sleep(...)`. Two properties made that unsafe:

  * A sleeping coroutine is not in slot.transferring, so Scrapy sees a free
    transfer slot and pops the next request for that domain. Two coroutines
    for one domain then slept at once.
  * asyncio.sleep is a lower bound. Two sleeps overrun by different amounts,
    so the one that claimed the LATER turn can wake first. A 20 minute run
    dispatched two requests to poki.com 0.000520s apart exactly that way: one
    had claimed turn 247.97 and the other 253.07, and the second went out
    first at 254.160805, leaving the first to wake with its own turn six
    seconds past and go straight out.

Bounding the wait by the last real dispatch as well as by the claimed turn
closed that particular hole, but the shape of the design was still "two
independent timers that must agree". crawler/downloader.py instead makes the
single timer Scrapy already has count from the previous response's
completion, which no amount of reactor lateness can shorten. See its
docstring for the proof.

Where the measurement belongs
-----------------------------
Compliance is defined on request spacing as the server sees it, which is the
moment of the socket write. Three nearer-looking hooks are all wrong:

  * Downloader middlewares run at enqueue time, because Downloader.fetch
    wraps _enqueue_request in the middleware chain. Every queued request gets
    the same timestamp.
  * The request_reached_downloader signal fires from _enqueue_request, so it
    has the same problem.
  * meta['download_latency'] covers only the transfer, excluding DNS and
    TCP/TLS setup, so recv - latency lands late and understates the gap.

record_dispatch is called from Downloader._download, the first thing to run
after the slot releases a request and the last before the handler opens the
socket.

The guard
---------
record_dispatch raises IgnoreRequest when a dispatch lands closer than the
floor to the previous one on the same domain. Under the completion-anchored
design this cannot happen, so the guard should never fire: it exists because
"should never" has been wrong three times on this crawl, and losing one page
is cheaper than one violation. dispatch/guard_refused is asserted to be 0 in
the smoke acceptance list.
"""

from __future__ import annotations

import logging
import os
import time
import traceback
from pathlib import Path

from scrapy.exceptions import IgnoreRequest

from crawler.slot import slot_key

logger = logging.getLogger(__name__)

DISPATCH_TIME = "dispatch_time"

# Configured once from BroadSpider.__init__.
_min_gap = 0.0
_dispatch_log = None
_trace_fh = None
_stats = None

# When each domain last actually dispatched. This is the quantity
# ops/verify_politeness.py measures, so the guard and the offline check cannot
# disagree about what a gap is.
#
# Keyed by the domain the URL names, not by id(slot) and not by meta.
#
# Not id(slot): Downloader._slot_gc destroys any slot idle for 60s, and
# CPython reuses the freed address almost immediately, measured at 1,999
# recycles in 2,000 allocations. So id(slot) is neither stable for one domain
# nor unique across domains. A two hour run produced exactly that failure
# twice on panasonic.jp, both after quiet periods long enough for the slot to
# be collected and rebuilt.
#
# Not meta: a request built from another request inherits its meta, so a
# redirect or a meta refresh carries the source domain's key to a target on a
# different domain. A 10 minute run produced six violations that way, every
# one of them invisible because the timer believed it was looking at the
# source domain's history.
_last_out: dict[str, float] = {}
_LAST_OUT_MAX = 100_000

# (dispatched, url) of the previous dispatch per domain, for the trace only.
_last_trace: dict[str, tuple[float, str]] = {}
_LAST_TRACE_MAX = 200_000


def _open_trace(path: str):
    """Open the diagnostic log, or return None if it cannot be written.

    Diagnostics must never take the crawl down, so a failure here disables
    tracing rather than raising. It is logged: an empty trace file is read as
    evidence of compliance, and "never opened" must not look the same as
    "opened and stayed empty".
    """
    try:
        directory = os.path.dirname(path)
        if directory:
            os.makedirs(directory, exist_ok=True)
        return open(path, "a", buffering=1, encoding="utf-8")
    except OSError as exc:
        logger.warning("short-gap tracing disabled, cannot open %s: %s", path, exc)
        return None


def configure(
    min_gap: float = 0.0,
    trace_path: str | None = None,
    log_path: str | None = None,
) -> None:
    """Set the floor the guard enforces and open the two diagnostic files.

    A min_gap of 0 only measures, which is what a plain `scrapy crawl` gets.

    When log_path is set, every dispatch is recorded there. This is the only
    complete record of what the crawler sent: data/crawled.log.gz is written
    from parse(), so it holds successful HTML fetches only and omits both
    failed requests and every robots.txt fetch, even though all of them
    consumed a real turn on the domain's timer. In one measured run 40,957 of
    45,825 robots fetches timed out, so the majority of what the crawler sent
    was invisible to the response-side log. ops/verify_politeness.py can read
    either file, and the compliance claim rests on this one.

    When trace_path is set, every dispatch closer than min_gap to the previous
    one on the same domain is dumped there with the state that produced it,
    and refused.
    """
    global _min_gap, _dispatch_log, _trace_fh
    _min_gap = float(min_gap)
    _last_out.clear()
    _last_trace.clear()

    if trace_path:
        _trace_fh = _open_trace(trace_path)
    if log_path:
        from crawler.logwriter import GzipLineWriter

        _dispatch_log = GzipLineWriter(Path(log_path))


def set_stats(stats) -> None:
    """Attach the stats collector the guard reports refusals through.

    Separate from configure() because Crawler.stats raises until the crawl
    starts, while the floor and the log files must be set before the
    Downloader is built. A refusal before this point is still logged and
    traced; only the counter would miss it, and nothing can dispatch that
    early anyway.
    """
    global _stats
    _stats = stats


def _trace(request, slot_id: str, dispatched: float, observed: float) -> None:
    previous = _last_trace.get(slot_id)
    _trace_fh.write(
        f"=== SHORT GAP {observed:.6f}s slot={slot_id!r} floor={_min_gap}\n"
        f"  prev  dispatched={previous[0]:.6f} url={previous[1]}\n"
        f"  curr  dispatched={dispatched:.6f} url={request.url}\n"
        f"  meta={ {k: v for k, v in request.meta.items() if k in _INTERESTING} }\n"
        f"  stack:\n"
    )
    for frame in traceback.format_stack(limit=20):
        _trace_fh.write("    " + frame.rstrip().replace("\n", "\n    ") + "\n")
    _trace_fh.write("\n")


_INTERESTING = (
    "download_slot",
    "new_domain",
    "seed",
    "depth",
    "redirect_urls",
    "redirect_times",
    "dont_obey_robotstxt",
)


def record_dispatch(request) -> None:
    """Stamp the request, log it, and refuse it if the gap is short.

    Called from crawler/downloader.py at the top of _download. Raises
    IgnoreRequest rather than returning a verdict, because the caller's next
    statement opens a socket: there is no safe way to continue.
    """
    dispatched = time.time()
    # Derived from the URL, never read from meta. See the _last_out comment.
    slot_id = slot_key(request.url)

    if _min_gap > 0:
        previous = _last_out.get(slot_id)
        if previous is not None:
            observed = dispatched - previous
            # 0.05s of slack, matching ops/verify_politeness.py, so the guard
            # and the offline check agree on the verdict for every pair.
            if observed < _min_gap - 0.05:
                if _stats is not None:
                    _stats.inc_value("dispatch/guard_refused")
                if _trace_fh is not None:
                    _trace(request, slot_id, dispatched, observed)
                logger.warning(
                    "politeness guard refused %s: %.6fs since the previous "
                    "dispatch to %s, floor %.2fs",
                    request.url, observed, slot_id, _min_gap,
                )
                raise IgnoreRequest("politeness guard: gap too short")

        if _trace_fh is not None:
            if slot_id not in _last_trace and len(_last_trace) >= _LAST_TRACE_MAX:
                # Drop the whole history rather than pay to find the oldest
                # entry. One gap then goes unchecked per domain, and the real
                # compliance check reads the dispatch log and is unaffected.
                _last_trace.clear()
            _last_trace[slot_id] = (dispatched, request.url)

        # Bound the dict. An entry whose gap has already elapsed can never
        # refuse a future request, so dropping it is safe: a missing entry
        # means the next dispatch is unconstrained, which is the same answer
        # the entry would have given.
        #
        # This must NOT evict by "is the slot still live". A collected slot is
        # exactly the case that caused an earlier violation: the domain comes
        # back, and its history has to come back with it.
        if len(_last_out) > _LAST_OUT_MAX:
            stale = dispatched - _min_gap
            for key in [k for k, v in _last_out.items() if v <= stale]:
                del _last_out[key]
        _last_out[slot_id] = dispatched

    request.meta[DISPATCH_TIME] = dispatched
    if _dispatch_log is not None:
        _dispatch_log.write(f"{dispatched:.3f}", slot_id, request.url)


def close_dispatch_log() -> None:
    """Flush the dispatch record. Called from the spider's closed()."""
    global _dispatch_log, _trace_fh
    if _dispatch_log is not None:
        _dispatch_log.close()
        _dispatch_log = None
    if _trace_fh is not None:
        _trace_fh.close()
        _trace_fh = None
