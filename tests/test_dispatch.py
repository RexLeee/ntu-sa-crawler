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

The fourth scenario covers the other way a gap can collapse: the requests are
correctly spaced, but grouped under the wrong domain. A request built by
MetaRefreshMiddleware carries a copy of the *source* page's meta, including
its download_slot, so it queues on the source domain's timer and ignores the
target's. Sending four requests to one host under four different foreign slot
keys reproduces that.
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
# Scenario 3 reproduces a real violation from a two hour run. Scrapy's
# Downloader._slot_gc destroys any slot idle for 60s. The floor used to be
# keyed by id(slot), and CPython reuses a freed object's address immediately
# (measured: 1,999 reuses in 2,000 allocations), so a domain that went quiet
# and came back lost its gap entirely: two requests to panasonic.jp dispatched
# 0.001s apart. Here the slot is collected between every request.
# Scenario 4 reproduces the meta refresh violation. See _foreign_meta below.
SCENARIOS = {
    "measure": {"delay": 2.0, "floor": 0.0, "expect": 2.0},
    "enforce": {"delay": 0.5, "floor": 2.0, "expect": 2.0},
    "slot_gc": {"delay": 0.0, "floor": 2.0, "expect": 2.0, "gc_slots": True},
    "foreign_meta": {"delay": 0.0, "floor": 2.0, "expect": 2.0, "foreign_meta": True},
}

observed: list[float] = []
# The slot each request actually landed on, as the Downloader resolved it.
resolved_slots: list[str] = []


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

    # Chained: each response yields the next request. The slot_gc scenario
    # needs the slot to be empty between requests, which cannot happen while
    # three siblings sit queued on it. This mirrors the production case, a
    # domain whose next URL is discovered only after the previous page parses.
    chained = False

    # Give every request a different, wrong download_slot, as a request
    # manufactured by a meta refresh from another site arrives with.
    foreign_meta = False

    def _probe(self, i: int) -> Request:
        slot = f"foreign{i}.example" if self.foreign_meta else "probe"
        return Request(
            f"http://127.0.0.1:{self.port}/?i={i}",
            meta={"download_slot": slot, "i": i},
            dont_filter=True,
            callback=self.parse,
        )

    async def start(self):
        if self.chained:
            yield self._probe(0)
        else:
            for i in range(REQUESTS):
                yield self._probe(i)

    def parse(self, response):
        observed.append(float(response.meta[DISPATCH_TIME]))
        # _enqueue_request writes the key the Downloader chose back into meta,
        # so this is what the rate limiter actually grouped the request under.
        resolved_slots.append(response.meta.get("download_slot", ""))
        if self.chained:
            nxt = response.meta["i"] + 1
            if nxt < REQUESTS:
                yield self._probe(nxt)


def _force_slot_gc_between_requests() -> None:
    """Drop every idle download slot after each dispatch.

    This is what Downloader._slot_gc does on its own 60s timer for any slot
    idle that long. A 2 hour run is full of domains that go quiet and come
    back, so the real crawl hits this constantly; a short test never would.
    Forcing it turns a rare production race into a deterministic check.
    """
    import scrapy.core.downloader as dl

    patched = dl.Downloader._enqueue_request

    async def _enqueue_then_gc(self, request):
        try:
            return await patched(self, request)
        finally:
            # Collect here, not inside _download. _enqueue_request holds the
            # request in slot.active for the whole download, and _slot_gc
            # skips any slot with active requests, so a collection attempted
            # from inside _download is silently a no-op. This finally block
            # runs after slot.active.remove.
            #
            # Drop the slot outright rather than calling _slot_gc, which also
            # refuses while any sibling request is queued on the same slot.
            # The production case is a domain whose last request finished and
            # whose next one arrives minutes later, so the slot is genuinely
            # gone; forcing that here is the point of the scenario.
            key = request.meta.get(self.DOWNLOAD_SLOT)
            slot = self.slots.get(key) if key else None
            if slot is not None and not slot.active:
                self.slots.pop(key).close()

    dl.Downloader._enqueue_request = _enqueue_then_gc


def run_scenario(name: str) -> int:
    """Crawl once under one scenario and check the gaps. Runs in a subprocess.

    A subprocess per scenario is needed because install() is idempotent by
    design: the monkey-patch must not stack, so one process can only ever hold
    one floor value.
    """
    cfg = SCENARIOS[name]
    install(cfg["floor"])

    if cfg.get("gc_slots"):
        _force_slot_gc_between_requests()

    server = HTTPServer(("127.0.0.1", 0), _Handler)
    port = server.server_address[1]
    threading.Thread(target=server.serve_forever, daemon=True).start()

    settings = {"LOG_LEVEL": "ERROR", "DOWNLOAD_DELAY": cfg["delay"]}
    if cfg.get("foreign_meta"):
        # The floor alone is not enough. A request carrying a foreign slot key
        # would still occupy the wrong Scrapy slot, so both halves of the fix
        # have to be in place for the gaps to hold.
        settings["DOWNLOADER"] = "crawler.downloader.DomainSlotDownloader"

    process = CrawlerProcess(settings=settings)
    _Spider.chained = bool(cfg.get("gc_slots"))
    _Spider.foreign_meta = bool(cfg.get("foreign_meta"))
    process.crawl(_Spider, port=port)
    process.start()
    server.shutdown()

    observed.sort()
    if len(observed) != REQUESTS:
        print(f"FAIL [{name}]: expected {REQUESTS} dispatches, got {len(observed)}")
        return 1

    if cfg.get("foreign_meta"):
        # Every probe is the same host, so one key must cover all four. The
        # gap check below would also pass if the timer simply got lucky, so
        # assert the grouping directly.
        wrong = [s for s in resolved_slots if s != "127.0.0.1"]
        if wrong:
            print(
                f"FAIL [{name}]: {len(wrong)} request(s) kept a foreign slot key, "
                f"e.g. {wrong[0]!r}; expected '127.0.0.1'"
            )
            return 1
        print(f"[{name}] all {len(resolved_slots)} requests resolved to '127.0.0.1'")

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


def test_reservation_never_moves_backwards() -> int:
    """A dispatch must not overwrite a sibling's later reserved turn.

    Each coroutine claims its turn before awaiting, writing turn + gap into
    the shared dict. When it finally dispatches it writes again, from the real
    dispatch time. That second write used to be unconditional, so a coroutine
    that dispatched while a sibling held a later reservation would replace the
    sibling's value with a smaller one. The next arrival then claimed the turn
    the sibling was already sleeping on, and the two went out together.

    Checked on the dict itself rather than through a crawl, because the losing
    interleaving depends on event loop timing that a test cannot force
    reliably. What it can assert is the invariant: the stored value never
    decreases.
    """
    import asyncio

    import scrapy.core.downloader as dl

    async def noop(self, slot, request):
        return None

    dl.Downloader._download = noop
    install(5.0)
    patched = dl.Downloader._download

    # The claim dict is the only empty dict in the closure.
    reservations = next(
        cell.cell_contents
        for cell in patched.__closure__
        if isinstance(cell.cell_contents, dict) and not cell.cell_contents
    )

    class _Slot:
        pass

    class _Req:
        def __init__(self, url):
            self.url = url
            self.meta = {"download_slot": "d.example"}

    seen: list[float] = []

    async def watch():
        # Sample the reservation while three requests claim and dispatch.
        for _ in range(60):
            value = reservations.get("d.example")
            if value is not None:
                seen.append(value)
            await asyncio.sleep(0.05)

    async def drive():
        await asyncio.gather(
            *(patched(None, _Slot(), _Req(f"https://d.example/{i}")) for i in range(3)),
            watch(),
        )

    asyncio.run(drive())

    drops = [
        (a, b) for a, b in zip(seen, seen[1:], strict=False) if b < a - 1e-9
    ]
    if drops:
        a, b = drops[0]
        print(
            f"FAIL [reservation]: value moved backwards, {a:.6f} -> {b:.6f} "
            f"({len(drops)} times)"
        )
        return 1
    print(f"PASS [reservation]: never decreased across {len(seen)} samples")
    return 0


def main() -> int:
    if len(sys.argv) == 2 and sys.argv[1] == "reservation":
        return test_reservation_never_moves_backwards()
    if len(sys.argv) > 1:
        return run_scenario(sys.argv[1])

    # Parent: run each scenario in its own interpreter.
    failures = 0
    for name in [*SCENARIOS, "reservation"]:
        result = subprocess.run([sys.executable, __file__, name], check=False)
        failures += result.returncode != 0
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
