#!/usr/bin/env python3
"""Prove the dispatch record measures real spacing, and the guard refuses.

Run directly: uv run python tests/test_dispatch.py

crawler/dispatch.py no longer throttles. The throttle is
crawler/downloader.py, covered by tests/test_downloader.py; this file covers
what dispatch.py still owns:

  * the stamp and the dispatch log, on a live crawl against a local server
  * the grouping key, which must come from the URL and never from meta
  * the guard, which refuses a dispatch that lands under the floor

Scenario 1 measures spacing on the real Downloader subclass.
Scenario 2 reproduces the slot-GC violation from a two hour run. Scrapy's
Downloader._slot_gc destroys any slot idle for 60s, and slot.lastseen lives
on the Slot, so a rebuilt slot starts with no history and dispatches at once.
Two requests to panasonic.jp went out 0.001s apart that way. This scenario
collects the slot between every request, and passes only because
DomainSlotDownloader keeps the completion time outside the slot and seeds a
rebuilt one from it. Written after the guard caught exactly this during the
rewrite.
Scenario 3 reproduces the meta refresh violation. MetaRefreshMiddleware
builds its target with source.replace(), copying the source page's
download_slot, so the target queued on the wrong domain's timer. Sending four
requests to one host under four different foreign slot keys reproduces it.
"""

from __future__ import annotations

import subprocess
import sys
import tempfile
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from scrapy import Request, Spider  # noqa: E402
from scrapy.crawler import CrawlerProcess  # noqa: E402

from crawler.dispatch import DISPATCH_TIME, close_dispatch_log, configure  # noqa: E402
from crawler.logwriter import read_lines  # noqa: E402

REQUESTS = 4
TOLERANCE = 0.05

SCENARIOS = {
    "measure": {"delay": 2.0, "expect": 2.0},
    "slot_gc": {"delay": 2.0, "expect": 2.0, "gc_slots": True},
    "foreign_meta": {"delay": 2.0, "expect": 2.0, "foreign_meta": True},
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

    A subprocess per scenario because configure() holds module state: one
    process can only ever hold one floor and one set of log files.
    """
    cfg = SCENARIOS[name]
    # Every scenario writes the dispatch log, so its coverage is checked
    # alongside the gaps rather than in a test of its own. The production log
    # must hold one line per dispatch including robots.txt and failures, and a
    # silent regression there would remove the compliance evidence.
    log_dir = Path(tempfile.mkdtemp(prefix="dispatchlog-"))
    log_path = log_dir / "dispatched.log.gz"
    trace_path = log_dir / "violation-trace.log"
    # The floor sits just under the delay. The guard must not fire on gaps the
    # throttle produced correctly, and a floor equal to the delay would make
    # every scheduling rounding error a refusal.
    configure(cfg["delay"] - 0.5, str(trace_path), str(log_path))

    if cfg.get("gc_slots"):
        _force_slot_gc_between_requests()

    server = HTTPServer(("127.0.0.1", 0), _Handler)
    port = server.server_address[1]
    threading.Thread(target=server.serve_forever, daemon=True).start()

    settings = {
        "LOG_LEVEL": "ERROR",
        "DOWNLOAD_DELAY": cfg["delay"],
        # The subclass is what throttles and what derives the slot key, so
        # every scenario needs it.
        "DOWNLOADER": "crawler.downloader.DomainSlotDownloader",
    }

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
        f"[{name}] delay={cfg['delay']}s "
        f"gaps: {', '.join(f'{g:.3f}s' for g in gaps)}"
    )

    bad = [g for g in gaps if g < expect - TOLERANCE]
    if bad:
        print(f"FAIL [{name}]: {len(bad)} gap(s) under {expect}s")
        return 1

    print(f"PASS [{name}]: every gap held at or above {expect}s")

    close_dispatch_log()

    # The guard must not have fired. These gaps were produced correctly, so an
    # entry here would mean the guard and the throttle disagree about what a
    # gap is, which would cost real pages in production.
    if trace_path.exists() and trace_path.stat().st_size:
        print(f"FAIL [{name}]: the guard traced a short gap on a compliant run")
        print(trace_path.read_text(encoding="utf-8")[:400])
        return 1
    print(f"PASS [{name}]: the guard stayed silent")

    logged = [ln for ln in read_lines(log_path) if ln.strip()]
    if len(logged) < REQUESTS:
        print(
            f"FAIL [{name}]: dispatch log holds {len(logged)} lines, "
            f"expected at least {REQUESTS}"
        )
        return 1
    # Column 2 is the slot the timer grouped by, which is what
    # ops/verify_politeness.py --source dispatched reads.
    keys = {ln.split("\t")[1] for ln in logged}
    if keys != {"127.0.0.1"}:
        print(f"FAIL [{name}]: dispatch log grouped under {keys}, expected 127.0.0.1")
        return 1
    print(f"PASS [{name}]: dispatch log holds {len(logged)} lines, all one slot")
    return 0


def test_guard_refuses_a_short_gap() -> int:
    """A dispatch under the floor must raise, and be traced and counted.

    Under the completion-anchored throttle this cannot happen, so the guard is
    a backstop for a defect. It has fired for real three times on this crawl
    under earlier designs, which is why it stays: dropping one page is much
    cheaper than one violation.
    """
    from scrapy.exceptions import IgnoreRequest
    from scrapy.settings import Settings
    from scrapy.statscollectors import StatsCollector

    from crawler import dispatch as mod

    class _FakeCrawler:
        settings = Settings()
        spider = None

    tmp = Path(tempfile.mkdtemp(prefix="guard-"))
    trace = tmp / "violation-trace.log"
    stats = StatsCollector(_FakeCrawler())
    mod.configure(5.0, str(trace), None)
    mod.set_stats(stats)

    class _Req:
        def __init__(self, url):
            self.url = url
            self.meta = {}

    failures = 0

    first = _Req("https://guard.test/a")
    mod.record_dispatch(first)
    if DISPATCH_TIME not in first.meta:
        print("FAIL [guard]: the first dispatch was not stamped")
        failures += 1

    # Immediately after, so the gap is ~0s against a 5s floor.
    second = _Req("https://guard.test/b")
    try:
        mod.record_dispatch(second)
    except IgnoreRequest:
        print("PASS [guard]: a 0s gap raised IgnoreRequest")
    else:
        print("FAIL [guard]: a 0s gap was allowed through")
        failures += 1

    refused = stats.get_value("dispatch/guard_refused")
    if refused != 1:
        print(f"FAIL [guard]: dispatch/guard_refused is {refused}, expected 1")
        failures += 1
    else:
        print("PASS [guard]: the refusal reached stats")

    mod.close_dispatch_log()
    text = trace.read_text(encoding="utf-8") if trace.exists() else ""
    if "SHORT GAP" not in text:
        print(f"FAIL [guard]: nothing traced:\n{text[:300]}")
        failures += 1
    else:
        print("PASS [guard]: the refusal was traced")

    # A different domain must be unaffected: the history is per domain, and
    # grouping everything together would refuse legitimate traffic.
    other = _Req("https://other.test/a")
    try:
        mod.record_dispatch(other)
    except IgnoreRequest:
        print("FAIL [guard]: a different domain was refused")
        failures += 1
    else:
        print("PASS [guard]: a different domain dispatched freely")

    return 1 if failures else 0


def test_a_wall_clock_step_does_not_fake_a_violation() -> int:
    """The guard must measure in monotonic time, not wall clock.

    The limiter in crawler/downloader.py spaces requests in monotonic time. If
    the guard checks the wall clock, the two disagree whenever the clocks do,
    and over 48 hours chronyd will step the wall clock at least once.

    Both directions are wrong and both matter:

      * A backward step would make the guard see a short gap that never
        happened. It would drop the page, count dispatch/guard_refused and
        write to violation-trace, which are the three things the acceptance
        list reads as proof of a violation. The run would fail its own
        evidence check with no violation having occurred.
      * A forward step would hide a real short gap.

    The dispatch log and request.meta keep the wall-clock stamp, because that
    is what ops/verify_politeness.py reads offline.
    """
    import time as time_mod
    import types

    from scrapy.exceptions import IgnoreRequest

    from crawler import dispatch as mod

    failures = 0
    mod.configure(5.0, None, None)

    class _Req:
        def __init__(self, url):
            self.url = url
            self.meta = {}

    # A dispatch, then the wall clock jumps back an hour while monotonic time
    # advances the full delay. The gap is real; only the wall clock disagrees.
    first = _Req("https://step.test/a")
    mod.record_dispatch(first)

    base_wall = time_mod.time()
    base_mono = time_mod.monotonic()

    def _fake_clock(wall: float, mono: float):
        """A stand-in for the time module, swapped into dispatch only.

        Patching time.time globally would change the clock for every other
        module in the interpreter, including the test runner.
        """
        fake = types.SimpleNamespace()
        fake.time = lambda: wall
        fake.monotonic = lambda: mono
        return fake

    real_module = mod.time
    try:
        mod.time = _fake_clock(base_wall - 3600.0, base_mono + 6.0)
        second = _Req("https://step.test/b")
        try:
            mod.record_dispatch(second)
        except IgnoreRequest:
            print("FAIL [clock]: a backward wall-clock step faked a violation")
            failures += 1
        else:
            print("PASS [clock]: a backward wall-clock step did not fake a violation")

        # The log and meta must still carry wall clock, or the offline check
        # cannot read them.
        stamp = second.meta.get(DISPATCH_TIME)
        if stamp is None or abs(stamp - (base_wall - 3600.0)) > 1.0:
            print(f"FAIL [clock]: meta stamp is {stamp}, expected the wall clock")
            failures += 1
        else:
            print("PASS [clock]: the recorded stamp is still wall clock")

        # And a forward wall-clock step must not hide a real short gap:
        # monotonic barely moves, so the guard must still refuse.
        mod.time = _fake_clock(base_wall + 7200.0, base_mono + 6.1)
        third = _Req("https://step.test/c")
        try:
            mod.record_dispatch(third)
        except IgnoreRequest:
            print("PASS [clock]: a forward step did not hide a short gap")
        else:
            print("FAIL [clock]: a forward wall-clock step hid a short gap")
            failures += 1
    finally:
        mod.time = real_module

    return 1 if failures else 0


def test_key_comes_from_the_url_not_meta() -> int:
    """The guard must group by the URL's domain, never by meta.

    A redirect or a meta refresh builds its target with source.replace(),
    which copies meta wholesale. A guard reading meta['download_slot'] would
    therefore check a target request against the source domain's history. A
    10 minute run produced six violations that way, all invisible to the
    check for exactly this reason.
    """
    from scrapy.exceptions import IgnoreRequest

    from crawler import dispatch as mod

    mod.configure(5.0, None, None)

    class _Req:
        def __init__(self, url, slot):
            self.url = url
            self.meta = {"download_slot": slot}

    # Two requests to ONE domain, carrying two different foreign slot keys.
    # Grouping by meta would see two domains and allow both.
    mod.record_dispatch(_Req("https://same.test/a", "foreign1.example"))
    try:
        mod.record_dispatch(_Req("https://same.test/b", "foreign2.example"))
    except IgnoreRequest:
        print("PASS [key]: two foreign meta keys still grouped by the URL")
        return 0
    print("FAIL [key]: meta['download_slot'] split one domain into two")
    return 1


def main() -> int:
    if len(sys.argv) == 2 and sys.argv[1] == "guard":
        return test_guard_refuses_a_short_gap()
    if len(sys.argv) == 2 and sys.argv[1] == "key":
        return test_key_comes_from_the_url_not_meta()
    if len(sys.argv) == 2 and sys.argv[1] == "clock":
        return test_a_wall_clock_step_does_not_fake_a_violation()
    if len(sys.argv) > 1:
        return run_scenario(sys.argv[1])

    # Parent: run each scenario in its own interpreter.
    failures = 0
    for name in [*SCENARIOS, "guard", "key", "clock"]:
        result = subprocess.run([sys.executable, __file__, name], check=False)
        failures += result.returncode != 0
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
