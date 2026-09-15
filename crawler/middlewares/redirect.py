"""Redirect handling that restores filtering and respects shard ownership.

Scrapy's RedirectMiddleware builds the follow-up request with
`source_request.replace(url=...)`, which copies the source's attributes
wholesale. Two of them must not travel to a different URL.

The download slot used to be the dangerous one: a redirect from youtube.com to
google.com landed on youtube's timer and skipped google's rate limit
entirely, which a 10 minute run turned into two identically timestamped
requests to developer.google.com. That is now handled a layer down.
crawler/downloader.py derives the slot from the URL, so no inherited value can
reach it and this middleware no longer has to correct one.

dont_filter still has to be corrected here, because nothing else can.

A redirect is also the one path that can move a request onto a domain this
process does not own. Seeds and page links are both checked before a Request
exists, but a redirect chooses its destination after the fetch. Following it
would put two processes on one domain's timer, which is exactly what sharding
must never allow, so the target is handed to its owner instead.
"""

from __future__ import annotations

import logging

from scrapy.downloadermiddlewares.redirect import RedirectMiddleware
from scrapy.exceptions import IgnoreRequest

from crawler.slot import shard_of, slot_key

logger = logging.getLogger(__name__)


class SlotAwareRedirectMiddleware(RedirectMiddleware):
    """Filter redirect targets, and refuse the ones another shard owns."""

    def _build_redirect_request(self, source_request, response, *, url: str, **kwargs):
        redirect_request = super()._build_redirect_request(
            source_request, response, url=url, **kwargs
        )
        # The spider sets dont_filter on requests it has already checked
        # against the Bloom filter, and replace() copies that flag. A redirect
        # target is a different URL that nothing has checked, so restore
        # filtering or a redirect loop between two URLs would never terminate.
        redirect_request.dont_filter = False

        spider = self.crawler.spider
        shards = getattr(spider, "shards", 1)
        if shards > 1:
            owner = shard_of(slot_key(redirect_request.url), shards)
            if owner != spider.shard:
                # Hand it over and stop here. The owner applies its own
                # duplicate filter and per-domain limits, so this must not
                # pre-empt them.
                spider.handoff.send(redirect_request.url, owner)
                self.crawler.stats.inc_value("handoff/redirect_out")
                raise IgnoreRequest(f"redirect target belongs to shard {owner}")

        return redirect_request
