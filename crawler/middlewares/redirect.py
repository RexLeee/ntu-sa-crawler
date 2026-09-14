"""Redirect handling that re-keys the download slot.

Scrapy's RedirectMiddleware builds the follow-up request with
`source_request.replace(url=...)`, which copies meta wholesale. That carries
the *original* download_slot to the new URL, so a redirect crossing domains
(youtube.com -> google.com) lands on the source domain's timer and bypasses
the target's rate limit entirely.

Observed in a 10 minute run: two developer.google.com requests dispatched at
the identical timestamp because both arrived via redirects from other hosts.
"""

from __future__ import annotations

from scrapy.downloadermiddlewares.redirect import RedirectMiddleware

from crawler.slot import slot_key


class SlotAwareRedirectMiddleware(RedirectMiddleware):
    """Re-assign download_slot whenever a redirect changes the domain."""

    def _build_redirect_request(self, source_request, response, *, url: str, **kwargs):
        redirect_request = super()._build_redirect_request(
            source_request, response, url=url, **kwargs
        )
        # Re-key on the destination, not the source.
        redirect_request.meta["download_slot"] = slot_key(redirect_request.url)
        return redirect_request
