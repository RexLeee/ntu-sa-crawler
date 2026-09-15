"""The Downloader that decides which domain a request belongs to.

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
"""

from __future__ import annotations

from scrapy import Request
from scrapy.core.downloader import Downloader

from crawler.slot import slot_key


class DomainSlotDownloader(Downloader):
    """Key every download slot by registered domain, derived from the URL."""

    def get_slot_key(self, request: Request) -> str:
        return slot_key(request.url)
