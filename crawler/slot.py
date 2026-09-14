"""Download slot keys.

The rate limit is defined per domain, so every request to the same registered
domain (eTLD+1) must share one Scrapy download slot. Scrapy keys slots by
hostname by default, which would let a.example.com and b.example.com each run
their own 5s timer and double the effective request rate.
"""

from __future__ import annotations

import ipaddress
from functools import lru_cache
from urllib.parse import urlsplit

import tldextract

# include_psl_private_domains=True treats blogspot.com, github.io etc. as
# public suffixes, so foo.blogspot.com and bar.blogspot.com are separate
# domains. They are separate sites on separate infrastructure.
_extract = tldextract.TLDExtract(include_psl_private_domains=True)


@lru_cache(maxsize=200_000)
def _slot_for_host(host: str) -> str:
    if not host:
        return "__nohost__"

    # Literal IP addresses have no registered domain; key on the address itself.
    bare = host.strip("[]")
    try:
        ipaddress.ip_address(bare)
        return bare
    except ValueError:
        pass

    parts = _extract(host)
    if parts.registered_domain:
        return parts.registered_domain
    # Intranet names and unknown suffixes fall back to the hostname.
    return host


def slot_key(url: str) -> str:
    """Return the politeness key for a URL: its eTLD+1 where one exists."""
    host = urlsplit(url).hostname or ""
    return _slot_for_host(host.lower())
