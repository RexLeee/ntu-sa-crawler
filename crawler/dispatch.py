"""Record the instant each request actually starts transferring.

Compliance is defined on request spacing, so we must timestamp the moment the
Downloader begins a transfer. Two nearer-looking hooks are both wrong:

  * Downloader middlewares run at enqueue time, because Downloader.fetch wraps
    _enqueue_request in the middleware chain. Every queued request gets the
    same timestamp.
  * The request_reached_downloader signal fires from _enqueue_request, so it
    has the same problem.
  * meta['download_latency'] covers only the transfer, excluding DNS and
    TCP/TLS setup, so recv - latency lands late and understates the gap.

Downloader._download is the first thing to run after the slot's delay timer
releases a request, which makes it the correct measurement point. Patching it
is intrusive, so it is isolated here and verified by tests/test_dispatch.py.
"""

from __future__ import annotations

import time

import scrapy.core.downloader as _dl

DISPATCH_TIME = "dispatch_time"

_installed = False


def install() -> None:
    """Stamp meta['dispatch_time'] when a request starts transferring."""
    global _installed
    if _installed:
        return

    original = _dl.Downloader._download

    async def _download_with_stamp(self, slot, request):
        request.meta[DISPATCH_TIME] = time.time()
        return await original(self, slot, request)

    _dl.Downloader._download = _download_with_stamp
    _installed = True
