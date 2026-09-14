#!/usr/bin/env python3
"""Prove the dispatch timer measures real request spacing.

Run directly: uv run python tests/test_dispatch.py

Uses a local HTTP server so the assertion depends on Scrapy's scheduling only,
not on any external site. Four requests share one download slot, so their
transfer starts must be DOWNLOAD_DELAY apart.
"""

from __future__ import annotations

import sys
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from scrapy import Request, Spider  # noqa: E402
from scrapy.crawler import CrawlerProcess  # noqa: E402

from crawler.dispatch import DISPATCH_TIME, install  # noqa: E402

DELAY = 2.0
REQUESTS = 4
TOLERANCE = 0.05

observed: list[float] = []


class _Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        body = b"<html><body>ok</body></html>"
        self.send_response(200)
        self.send_header("Content-Type", "text/html")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *args):
        pass


class _Spider(Spider):
    name = "dispatch_probe"
    custom_settings = {
        "DOWNLOAD_DELAY": DELAY,
        "DOWNLOAD_DELAY_JITTER": 0,
        "CONCURRENT_REQUESTS_PER_DOMAIN": 1,
        "CONCURRENT_REQUESTS": 16,
        "ROBOTSTXT_OBEY": False,
        "LOG_LEVEL": "ERROR",
    }

    def __init__(self, port: int, **kw):
        super().__init__(**kw)
        self.port = int(port)

    async def start(self):
        for i in range(REQUESTS):
            yield Request(
                f"http://127.0.0.1:{self.port}/?i={i}",
                meta={"download_slot": "probe"},
                dont_filter=True,
                callback=self.parse,
            )

    def parse(self, response):
        observed.append(float(response.meta[DISPATCH_TIME]))


def main() -> int:
    install()

    server = HTTPServer(("127.0.0.1", 0), _Handler)
    port = server.server_address[1]
    threading.Thread(target=server.serve_forever, daemon=True).start()

    process = CrawlerProcess(settings={"LOG_LEVEL": "ERROR"})
    process.crawl(_Spider, port=port)
    process.start()
    server.shutdown()

    observed.sort()
    if len(observed) != REQUESTS:
        print(f"FAIL: expected {REQUESTS} dispatches, got {len(observed)}")
        return 1

    gaps = [b - a for a, b in zip(observed, observed[1:], strict=False)]
    print(f"delay setting : {DELAY}s")
    print("gaps          : " + ", ".join(f"{g:.3f}s" for g in gaps))

    bad = [g for g in gaps if g < DELAY - TOLERANCE]
    if bad:
        print(f"FAIL: {len(bad)} gap(s) under the configured delay")
        return 1

    print("PASS: every dispatch gap respects the configured delay")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
