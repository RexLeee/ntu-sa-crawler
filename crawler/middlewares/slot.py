"""Force every request onto its registered-domain download slot."""

from __future__ import annotations

from crawler.slot import slot_key

DOWNLOAD_SLOT = "download_slot"


class SlotKeyMiddleware:
    """Sets meta['download_slot'] to the eTLD+1 of each request.

    Scrapy's Downloader honours this key when it exists, so all subdomains of a
    site share one delay timer and one concurrency limit.

    Note: downloader middlewares run at enqueue time, not dispatch time
    (Downloader.fetch wraps _enqueue_request in the middleware chain), so this
    is the wrong place to stamp a send time. The spider derives it from
    meta['download_latency'] instead.
    """

    def process_request(self, request, spider):
        if DOWNLOAD_SLOT not in request.meta:
            request.meta[DOWNLOAD_SLOT] = slot_key(request.url)
        return None
