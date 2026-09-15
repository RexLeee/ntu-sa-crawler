"""Redirect handling that restores duplicate filtering.

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
"""

from __future__ import annotations

from scrapy.downloadermiddlewares.redirect import RedirectMiddleware


class SlotAwareRedirectMiddleware(RedirectMiddleware):
    """Make every redirect target face the duplicate filter."""

    def _build_redirect_request(self, source_request, response, *, url: str, **kwargs):
        redirect_request = super()._build_redirect_request(
            source_request, response, url=url, **kwargs
        )
        # The spider sets dont_filter on requests it has already checked
        # against the Bloom filter, and replace() copies that flag. A redirect
        # target is a different URL that nothing has checked, so restore
        # filtering or a redirect loop between two URLs would never terminate.
        redirect_request.dont_filter = False
        return redirect_request
