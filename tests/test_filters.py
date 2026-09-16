#!/usr/bin/env python3
"""Prove normalize() rejects URLs that only fail later, inside Request().

One shape did real damage. urlsplit() accepts anything after the colon in
host:port and only parses it when .port is read; canonicalize_url() never
reads it. So http://host:void(0)/ passed every check in normalize() and
raised ValueError from safe_url_string inside Request.__init__ instead.

That exception surfaced in two places, both costly:

  * _drain_handoff, where AsyncioLoopingCall does not reschedule after its
    function raises. Cross-shard delivery ended 5 minutes into a measured
    hour: 1.79M urls sent, 181k received.
  * parse(), where it aborted the generator and lost every remaining link on
    that page. A measured hour recorded 14 of these.

Run: uv run python tests/test_filters.py
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from scrapy import Request  # noqa: E402

from crawler.config import load_config  # noqa: E402
from crawler.filters import UrlFilter  # noqa: E402

BASE = "https://example.com/page"


def test_malformed_port_is_rejected() -> bool:
    """A port that is not an integer must not survive normalization."""
    f = UrlFilter(load_config())
    bad = [
        "http://host.example:void(0)/x",
        "http://host.example:javascript/x",
        "https://host.example:80x/",
        "http://host.example:-1/",
        "http://host.example:{{port}}/",
    ]
    for url in bad:
        got = f.normalize(url, BASE)
        if got is not None:
            print(f"FAIL: {url} normalized to {got}, expected None")
            return False
    print(f"PASS: {len(bad)} malformed ports rejected")
    return True


def test_every_accepted_url_builds_a_request() -> bool:
    """The real invariant: nothing normalize() returns may raise in Request().

    Rejecting the one known shape is not the point. The point is that the
    filter's output is safe to hand to Request(), because two call sites do
    exactly that and neither can afford an exception.
    """
    f = UrlFilter(load_config())
    hrefs = [
        "http://host.example:void(0)/x",
        "http://host.example:8080/ok",
        "https://host.example:443/ok",
        "//host.example/protocol-relative",
        "/relative/path",
        "?query=1",
        "javascript:void(0)",
        "mailto:someone@example.com",
        "tel:+1234",
        "#fragment-only",
        "https://host.example/path with spaces",
        "https://host.example/%zz",
        "https://[::1]:8080/v6",
        "https://[::1]:bad/v6",
        "http://user:pass@host.example/auth",
        "https://xn--t-x57c59vkf.example/idn",
        "https://host.example/" + "a" * 3000,
        "https://host.example:/empty-port",
    ]
    checked = 0
    for href in hrefs:
        url = f.normalize(href, BASE)
        if url is None:
            continue
        checked += 1
        try:
            Request(url)
        except Exception as exc:
            print(f"FAIL: normalize({href!r}) -> {url!r} raised in Request: {exc}")
            return False
    if checked == 0:
        print("FAIL: no url was accepted, nothing was exercised")
        return False
    print(f"PASS: all {checked} accepted urls build a Request")
    return True


def test_valid_ports_still_pass() -> bool:
    """The fix must not reject legitimate ports."""
    f = UrlFilter(load_config())
    good = {
        "http://host.example:8080/ok": 8080,
        "https://host.example:443/ok": 443,
        "http://host.example/ok": None,
    }
    for url, _port in good.items():
        if f.normalize(url, BASE) is None:
            print(f"FAIL: {url} was rejected")
            return False
    print(f"PASS: {len(good)} valid urls unaffected")
    return True


def main() -> int:
    results = [
        test_malformed_port_is_rejected(),
        test_valid_ports_still_pass(),
        test_every_accepted_url_builds_a_request(),
    ]
    return 0 if all(results) else 1


if __name__ == "__main__":
    raise SystemExit(main())
