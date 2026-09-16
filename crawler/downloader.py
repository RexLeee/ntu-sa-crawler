"""The Downloader that decides which domain a request belongs to, and when.

Two overrides live here. Both exist because Scrapy's defaults are defined on
quantities that are not the ones compliance is defined on.

Which domain: get_slot_key
--------------------------
Scrapy's Downloader.get_slot_key trusts meta['download_slot'] whenever it is
present, and falls back to the hostname otherwise. Both halves are wrong for a
crawl whose compliance is defined on the registered domain:

  * Neither hostname is the unit the rate limit is defined on.
    www.example.com and cdn.example.com would each run their own 5s timer.
  * meta is copied, not derived. Any component that manufactures a request
    from another one carries the source's slot key to a different URL. A meta
    refresh from home-assistant.io to openhomefoundation.org produced exactly
    that: the target queued on the source's timer and dispatched 0.056s after
    a request the target's own timer had just released.

Deriving the key from the URL removes the whole class. There is no state to
copy, so no path through the code can carry a stale identity: the key a
request gets is a pure function of where it is going, which is the same
definition ops/verify_politeness.py checks against.

Scrapy still writes the resolved key back into meta from _enqueue_request, so
everything downstream that reads meta['download_slot'] sees the right value
without knowing this class exists.

When: _download
---------------
Scrapy spaces the POPS from slot.queue, not the actual requests.
_process_queue writes slot.lastseen at the moment it pops, then hands the
download to _schedule_coro; the coroutine runs whenever the event loop reaches
it. So DOWNLOAD_DELAY bounds the interval between two scheduling decisions,
and nothing bounds the interval between the two socket writes that follow.

That distinction is not academic on this crawl. A measured 20 minute run held
the reactor thread at 95-98% CPU for its whole length, and a 0.5s timer fired
1.3 to 4.6 seconds late at the median. Under that load the gap between two
pops has almost no relationship to the gap the server sees.

Anchoring the delay to the COMPLETION of the previous response fixes it
structurally rather than by margin:

    pop(B)   >= complete(A) + delay     this override, via slot.lastseen
    stamp(B) >= pop(B)                  the stamp is taken in this coroutine
    wire(A)  <= complete(A)             a socket write precedes its response
    => wire(B) >= wire(A) + delay

Every inequality points the same way, so reactor lateness can only widen the
gap. CONCURRENT_REQUESTS_PER_DOMAIN=1 is the other half: B cannot be popped
while A is still in slot.transferring, so two coroutines for one domain are
never in flight together and there is no ordering to get wrong.

This is what Heritrix calls min-delay-ms, "a minimum delay from the last
request completion", and what Nutch's FetchItemQueue implements by recording
the time the last request finished. Both are the reference implementations for
a politeness-critical broad crawl.

The cost is real and was accepted: a domain now yields one page per
(delay + round trip) rather than one per delay. At a 1.5s median round trip
that is 6.5s instead of 5.1s per page per domain, about 20% fewer pages from
a single domain. It buys nothing back in throughput terms here because the
bottleneck is the reactor thread at 97% CPU, not the supply of ready domains:
the same run held 5,000-10,000 live domains, which at 5.1s allows 1,568
pages/s against the 28 actually achieved.

Surviving slot collection
-------------------------
slot.lastseen lives on the Slot, and Downloader._slot_gc destroys any slot
idle for 60s. A rebuilt slot starts at lastseen = 0, so _process_queue sees
no penalty and pops immediately: a domain that goes quiet and comes back gets
no delay at all. tests/test_dispatch.py slot_gc reproduced this, and it is
the same shape as a real violation from a two hour run where two requests to
panasonic.jp dispatched 0.001s apart.

_last_complete holds the completion time per domain OUTSIDE the slot, and
_get_slot seeds a newly built slot's lastseen from it. The dict is keyed by
the registered domain, which is stable across collection, unlike id(slot)
which CPython reuses almost immediately.
"""

from __future__ import annotations

from time import monotonic

from scrapy import Request, signals
from scrapy.core.downloader import Downloader, Slot
from scrapy.utils.defer import _process_pending_io

from crawler.dispatch import record_dispatch
from crawler.slot import slot_key


class DomainSlotDownloader(Downloader):
    """Key every slot by registered domain; delay from the last completion."""

    # Eviction threshold for _last_complete. Entries older than the largest
    # delay in play can never hold a request back, so dropping them gives the
    # same answer a live entry would.
    _LAST_COMPLETE_MAX = 100_000

    def __init__(self, crawler):
        super().__init__(crawler)
        # When each domain's last request finished, kept outside the Slot so
        # it survives _slot_gc. See the docstring.
        self._last_complete: dict[str, float] = {}
        self._max_delay: float = self._delay

    def get_slot_key(self, request: Request) -> str:
        return slot_key(request.url)

    def _get_slot(self, request: Request, spider=None) -> tuple[str, Slot]:
        """Build slots as the parent does, but never with an empty history.

        A slot the GC destroyed and this call rebuilds would otherwise start
        at lastseen = 0, which _process_queue reads as "this domain has never
        been touched" and dispatches immediately.
        """
        key = self.get_slot_key(request)
        existed = key in self.slots
        key, slot = super()._get_slot(request)
        if not existed:
            last = self._last_complete.get(key)
            if last is not None:
                slot.lastseen = last
        return key, slot

    def _record_completion(self, key: str, when: float) -> None:
        if len(self._last_complete) > self._LAST_COMPLETE_MAX:
            stale = when - self._max_delay
            for k in [k for k, v in self._last_complete.items() if v <= stale]:
                del self._last_complete[k]
        self._last_complete[key] = when

    async def _download(self, slot, request: Request):
        """Copied from Scrapy 2.19.0 Downloader._download, with two changes.

        Added: the record_dispatch call, and the slot.lastseen assignment in
        the finally block.

        Copied rather than wrapped around super()._download(). The parent's
        own finally block calls self._process_queue(slot), which is what reads
        slot.lastseen to decide the next pop. Anything a subclass does after
        awaiting the parent therefore runs too late to affect that decision.

        tests/test_downloader.py pins scrapy.__version__ so an upgrade forces
        this copy to be re-compared against the original.
        """
        # Measured and guarded here: this is the last point before the request
        # reaches the network. See crawler/dispatch.py.
        record_dispatch(request)
        slot.transferring.add(request)
        try:
            response = await self.handlers.download_request_async(request)
            self.signals.send_catch_log(
                signal=signals.response_downloaded,
                response=response,
                request=request,
                spider=self.crawler.spider,
            )
            return response
        except Exception:
            await _process_pending_io()
            raise
        finally:
            slot.transferring.remove(request)
            # The delay counts from here, the completion, rather than from the
            # pop the parent stamps. Must precede _process_queue, which is the
            # call that reads it. Failures reach this line too, so a timeout
            # or a DNS error still costs the domain its full delay.
            done = monotonic()
            slot.lastseen = done
            # Also outside the slot, so _slot_gc cannot erase it.
            self._record_completion(self.get_slot_key(request), done)
            if slot.delay > self._max_delay:
                # A robots.txt Crawl-delay raised this slot. Eviction must not
                # discard a completion that is still holding a request back.
                self._max_delay = slot.delay
            self._process_queue(slot)
            self.signals.send_catch_log(
                signal=signals.request_left_downloader,
                request=request,
                spider=self.crawler.spider,
            )
