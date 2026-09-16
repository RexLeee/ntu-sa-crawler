#!/usr/bin/env python3
"""Prove the per-domain delay counts from the previous response's completion.

Run directly: uv run python tests/test_downloader.py

This is the invariant the whole politeness claim rests on. Scrapy spaces the
POPS from a slot's queue: _process_queue writes slot.lastseen when it pops,
then hands the download to the event loop. Under a saturated reactor -- a
measured run held 95-98% CPU with a 0.5s timer firing 1.3-4.6s late -- the
pop-to-pop interval says almost nothing about the interval the server sees.

crawler/downloader.py advances slot.lastseen at the completion instead, so:

    pop(B) >= complete(A) + delay >= wire(A) + delay, and wire(B) >= pop(B)

The test distinguishes the two semantics by making the response slow. With a
2s delay and a 1.5s response:

    from the pop        gaps land at 2.0s
    from the completion gaps land at 3.5s

So a gap at or above delay + response_time proves the anchor moved. The
difference is the whole point: under the old semantics that 1.5s was the
margin a late reactor could eat into.

A local HTTP server, so the timing depends on Scrapy's scheduling only.
"""

from __future__ import annotations

import subprocess
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import scrapy  # noqa: E402
from scrapy import Request, Spider  # noqa: E402
from scrapy.crawler import CrawlerProcess  # noqa: E402

from crawler.dispatch import DISPATCH_TIME, configure  # noqa: E402

REQUESTS = 3
DELAY = 2.0
RESPONSE_TIME = 1.5
TOLERANCE = 0.1

# The version crawler/downloader.py copied Downloader._download from. An
# upgrade must re-compare the copy against the original: the parent's finally
# block is what calls _process_queue, so a change in its body silently changes
# what the override guarantees.
PINNED_SCRAPY = "2.19.0"

# (dispatch stamp, time the response body finished arriving)
dispatches: list[float] = []
completions: list[float] = []


class _SlowHandler(BaseHTTPRequestHandler):
    """Answer after RESPONSE_TIME, so a pop-anchored delay is distinguishable."""

    def do_GET(self):
        body = b"<html><body>ok</body></html>"
        self.send_response(200)
        self.send_header("Content-Type", "text/html")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        # Sleeps in the server's own thread, not the reactor's.
        time.sleep(RESPONSE_TIME)
        self.wfile.write(body)

    def log_message(self, *args):
        pass


class _FailHandler(BaseHTTPRequestHandler):
    """Close the connection after a delay, without answering.

    A failed request still occupied the domain for RESPONSE_TIME, so it must
    still cost the domain its full delay. Failures reach the override's
    finally block exactly as successes do; this proves it.
    """

    def do_GET(self):
        time.sleep(RESPONSE_TIME)
        self.close_connection = True

    def log_message(self, *args):
        pass


class _Spider(Spider):
    name = "downloader_probe"
    custom_settings = {
        "DOWNLOAD_DELAY_JITTER": 0,
        "CONCURRENT_REQUESTS_PER_DOMAIN": 1,
        "CONCURRENT_REQUESTS": 16,
        "ROBOTSTXT_OBEY": False,
        "LOG_LEVEL": "ERROR",
        "RETRY_ENABLED": False,
        "DOWNLOAD_TIMEOUT": 30,
    }

    def __init__(self, port: int, **kw):
        super().__init__(**kw)
        self.port = int(port)

    async def start(self):
        for i in range(REQUESTS):
            yield Request(
                f"http://127.0.0.1:{self.port}/?i={i}",
                dont_filter=True,
                callback=self.parse,
                errback=self.failed,
            )

    def parse(self, response):
        dispatches.append(float(response.meta[DISPATCH_TIME]))
        completions.append(time.time())

    def failed(self, failure):
        stamp = failure.request.meta.get(DISPATCH_TIME)
        if stamp is not None:
            dispatches.append(float(stamp))
            completions.append(time.time())


def run_case(handler_name: str) -> int:
    """Crawl REQUESTS URLs on one domain and check the dispatch spacing."""
    handler = _SlowHandler if handler_name == "success" else _FailHandler
    configure(0.0, None, None)  # measure only; the guard is tested elsewhere

    server = HTTPServer(("127.0.0.1", 0), handler)
    port = server.server_address[1]
    threading.Thread(target=server.serve_forever, daemon=True).start()

    process = CrawlerProcess(
        settings={
            "LOG_LEVEL": "ERROR",
            "DOWNLOAD_DELAY": DELAY,
            "DOWNLOADER": "crawler.downloader.DomainSlotDownloader",
        }
    )
    process.crawl(_Spider, port=port)
    process.start()
    server.shutdown()

    if len(dispatches) != REQUESTS:
        print(
            f"FAIL [{handler_name}]: expected {REQUESTS} dispatches, "
            f"got {len(dispatches)}"
        )
        return 1

    order = sorted(range(len(dispatches)), key=lambda i: dispatches[i])
    stamps = [dispatches[i] for i in order]
    dones = [completions[i] for i in order]

    gaps = [b - a for a, b in zip(stamps, stamps[1:], strict=False)]
    # What the override guarantees: each dispatch is at least DELAY after the
    # PREVIOUS response completed, not merely after the previous dispatch.
    from_completion = [
        stamp - done for stamp, done in zip(stamps[1:], dones, strict=False)
    ]

    print(
        f"[{handler_name}] delay={DELAY}s response={RESPONSE_TIME}s\n"
        f"  dispatch-to-dispatch : {', '.join(f'{g:.3f}s' for g in gaps)}\n"
        f"  completion-to-dispatch: "
        f"{', '.join(f'{g:.3f}s' for g in from_completion)}"
    )

    failures = 0

    short = [g for g in from_completion if g < DELAY - TOLERANCE]
    if short:
        print(
            f"FAIL [{handler_name}]: {len(short)} dispatch(es) landed under "
            f"{DELAY}s after the previous completion, e.g. {short[0]:.3f}s. "
            f"The delay is still anchored to the pop."
        )
        failures += 1
    else:
        print(
            f"PASS [{handler_name}]: every dispatch was >= {DELAY}s after the "
            f"previous response completed"
        )

    # The distinguishing assertion. A pop-anchored delay produces gaps at
    # DELAY; a completion-anchored one produces DELAY + RESPONSE_TIME. If this
    # fails while the check above passes, the responses were not actually slow
    # and the test proves nothing.
    expected = DELAY + RESPONSE_TIME
    too_close = [g for g in gaps if g < expected - TOLERANCE]
    if too_close:
        print(
            f"FAIL [{handler_name}]: gap {too_close[0]:.3f}s is below "
            f"delay + response_time ({expected}s), so the anchor did not move"
        )
        failures += 1
    else:
        print(f"PASS [{handler_name}]: gaps reached {expected}s, the anchor moved")

    return 1 if failures else 0


def test_scrapy_version_is_pinned() -> int:
    """crawler/downloader.py copies Downloader._download from one version."""
    if scrapy.__version__ != PINNED_SCRAPY:
        print(
            f"FAIL [version]: scrapy is {scrapy.__version__}, the _download "
            f"copy in crawler/downloader.py was taken from {PINNED_SCRAPY}. "
            f"Re-compare it against the original, then update PINNED_SCRAPY."
        )
        return 1
    print(f"PASS [version]: scrapy {PINNED_SCRAPY}, the copy is current")
    return 0


def test_spider_builds_against_a_real_crawler() -> int:
    """BroadSpider.from_crawler must not touch anything set later than it.

    Crawler.stats raises RuntimeError until the crawl starts. An earlier
    version of the dispatch wiring read it from from_crawler, which every
    unit test passed because none of them builds a real Crawler; the first
    real run died at startup. This constructs the spider the way Scrapy does.
    """
    from scrapy.crawler import Crawler
    from scrapy.utils.project import get_project_settings

    from crawler.spiders.broad import BroadSpider

    settings = get_project_settings()
    settings.set("LOG_LEVEL", "ERROR")
    crawler = Crawler(BroadSpider, settings)
    try:
        spider = BroadSpider.from_crawler(crawler, seeds="tests/seeds_smoke.txt")
    except Exception as exc:
        print(f"FAIL [startup]: from_crawler raised {type(exc).__name__}: {exc}")
        return 1
    if spider is None:
        print("FAIL [startup]: from_crawler returned None")
        return 1
    print("PASS [startup]: the spider builds against a real, unstarted Crawler")
    return 0


def main() -> int:
    if len(sys.argv) == 2 and sys.argv[1] == "version":
        return test_scrapy_version_is_pinned()
    if len(sys.argv) == 2 and sys.argv[1] == "startup":
        return test_spider_builds_against_a_real_crawler()
    if len(sys.argv) > 1:
        return run_case(sys.argv[1])

    failures = 0
    for name in ("version", "startup", "success", "failure"):
        result = subprocess.run([sys.executable, __file__, name], check=False)
        failures += result.returncode != 0
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
