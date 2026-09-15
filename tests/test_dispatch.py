#!/usr/bin/env python3
"""Prove the dispatch timer measures and enforces real request spacing.

Run directly: uv run python tests/test_dispatch.py

Uses a local HTTP server so the assertion depends on Scrapy's scheduling only,
not on any external site. Four requests share one download slot, so their
transfer starts must be DOWNLOAD_DELAY apart.

The enforcement half matters because Scrapy's own spacing is not sufficient
under load: Slot._process_queue stamps lastseen when it schedules the download
coroutine, not when the coroutine runs, and at high concurrency the event loop
can be blocked in between. To prove the floor works independently, the second
test configures a DOWNLOAD_DELAY *below* the floor and checks the gaps still
come out above it.
"""

from __future__ import annotations

import subprocess
import sys
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from scrapy import Request, Spider  # noqa: E402
from scrapy.crawler import CrawlerProcess  # noqa: E402

from crawler.dispatch import DISPATCH_TIME, install  # noqa: E402

REQUESTS = 4
TOLERANCE = 0.05

# Scenario 1 measures Scrapy's own spacing: the floor is off, so the gaps can
# only come from DOWNLOAD_DELAY.
# Scenario 2 proves the floor stands on its own: DOWNLOAD_DELAY is set below
# it, so any gap at or above the floor must come from dispatch.py.
SCENARIOS = {
    "measure": {"delay": 2.0, "floor": 0.0, "expect": 2.0},
    "enforce": {"delay": 0.5, "floor": 2.0, "expect": 2.0},
}

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


def run_scenario(name: str) -> int:
    """Crawl once under one scenario and check the gaps. Runs in a subprocess.

    A subprocess per scenario is needed because install() is idempotent by
    design: the monkey-patch must not stack, so one process can only ever hold
    one floor value.
    """
    cfg = SCENARIOS[name]
    install(cfg["floor"])

    server = HTTPServer(("127.0.0.1", 0), _Handler)
    port = server.server_address[1]
    threading.Thread(target=server.serve_forever, daemon=True).start()

    process = CrawlerProcess(settings={"LOG_LEVEL": "ERROR", "DOWNLOAD_DELAY": cfg["delay"]})
    process.crawl(_Spider, port=port)
    process.start()
    server.shutdown()

    observed.sort()
    if len(observed) != REQUESTS:
        print(f"FAIL [{name}]: expected {REQUESTS} dispatches, got {len(observed)}")
        return 1

    gaps = [b - a for a, b in zip(observed, observed[1:], strict=False)]
    expect = cfg["expect"]
    print(
        f"[{name}] delay={cfg['delay']}s floor={cfg['floor']}s "
        f"gaps: {', '.join(f'{g:.3f}s' for g in gaps)}"
    )

    bad = [g for g in gaps if g < expect - TOLERANCE]
    if bad:
        print(f"FAIL [{name}]: {len(bad)} gap(s) under {expect}s")
        return 1

    print(f"PASS [{name}]: every gap held at or above {expect}s")
    return 0


def main() -> int:
    if len(sys.argv) > 1:
        return run_scenario(sys.argv[1])

    # Parent: run each scenario in its own interpreter.
    failures = 0
    for name in SCENARIOS:
        result = subprocess.run([sys.executable, __file__, name], check=False)
        failures += result.returncode != 0
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
