"""Download slot keys.

The rate limit is defined per domain, so every request to the same registered
domain (eTLD+1) must share one Scrapy download slot. Scrapy keys slots by
hostname by default, which would let a.example.com and b.example.com each run
their own 5s timer and double the effective request rate.
"""

from __future__ import annotations

import hashlib
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


def shard_of(key: str, shards: int) -> int:
    """Assign a domain to a worker. Stable across processes, unlike hash().

    This is what makes sharding safe for politeness. Every request for a
    domain is issued by exactly one process, so the 5 second timer never has
    to span processes and no shared state is needed to enforce it.

    Hashes the whole key rather than its first 8 bytes. Domains share prefixes
    heavily, so truncating skews the split. Measured over 5,994 real domains,
    max/min load per shard:

        shards   first 8 bytes   md5
             4          1.263x   1.062x
             6          1.171x   1.099x
             8          2.012x   1.124x

    At 8 workers one process would otherwise carry twice the load of another
    while cores sit idle.

    The ratio converges as domains accumulate, so a short run understates how
    even the split will be. Over the 2,229 domains a 10 minute run reached:
    1.077x at 4 shards, 1.106x at 6, 1.187x at 8.
    """
    digest = hashlib.md5(key.encode("utf-8"), usedforsecurity=False).digest()
    return int.from_bytes(digest[:8], "big") % shards
