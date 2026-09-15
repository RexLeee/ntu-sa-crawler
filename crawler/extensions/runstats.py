"""Periodic internal measurements, written to data/runstats.tsv.

The external sampler in ops/run_hour.sh can see RSS, threads and file
descriptors. It cannot see the numbers that decide whether this design lasts
48 hours:

  main thread CPU   Scrapy parses responses on the reactor thread, alongside
                    TLS handshakes, socket reads and every timer. If that one
                    thread saturates, connect() completions are not serviced
                    and the 10s download timeout fires on hosts that are in
                    fact responding. A 28 minute run produced 132,878 download
                    timeouts against 83,341 responses, and 5,111 of those
                    timeouts were on hosts that also answered in the same run.
                    This column is what confirms or refutes that explanation.

  reactor lag       The same saturation seen directly: how late a 0.5s timer
                    actually fires.

  scraper queue     Responses waiting to be parsed. A measured p50 of 13.65s
                    from dispatch to parse, against a 10s download timeout,
                    means responses sit here for seconds.

  frontier shape    len(pqueues) is the per-domain queue count.
                    DownloaderAwarePriorityQueue.pop() is O(that), so it is a
                    ceiling that only appears hours in.

Every value is read from objects this process already holds, so the cost is a
few microseconds per sample.
"""

from __future__ import annotations

import gc
import logging
import os
import threading
import time
from collections import Counter
from pathlib import Path

from scrapy import signals

from crawler.config import data_dir

logger = logging.getLogger(__name__)

COLUMNS = (
    "ts",
    "elapsed",
    "proc_cpu_pct",
    "main_cpu_pct",
    "lag_max_ms",
    "lag_p50_ms",
    "dl_active",
    "dl_slots",
    "dl_transferring",
    "scraper_queued",
    "scraper_active_kb",
    "frontier",
    "pqueues",
    "robots_cached",
    "robots_inflight",
    "dns_cached",
    "crawled",
    "discovered",
    "dupe_skipped",
    "rss_mb",
)


def _read_cpu_ticks(path: str) -> float:
    """Return utime+stime in clock ticks from a /proc stat file."""
    try:
        with open(path) as fh:
            fields = fh.read().rsplit(") ", 1)[-1].split()
        # After the comm field, utime is index 11 and stime is index 12.
        return float(fields[11]) + float(fields[12])
    except (OSError, IndexError, ValueError):
        return 0.0


def _read_rss_kb() -> float:
    try:
        with open("/proc/self/status") as fh:
            for line in fh:
                if line.startswith("VmRSS:"):
                    return float(line.split()[1])
    except OSError:
        pass
    return 0.0


class RunStats:
    """Samples the crawler's own internals on a fixed interval."""

    def __init__(self, crawler, interval: float, objects_every: float):
        self.crawler = crawler
        self.interval = interval
        self.objects_every = objects_every

        self._ticks_per_sec = float(os.sysconf("SC_CLK_TCK"))
        self._proc_stat = "/proc/self/stat"
        # The reactor runs on whichever thread started it, which is this one.
        self._main_stat = f"/proc/self/task/{threading.get_native_id()}/stat"

        self._start = time.monotonic()
        self._last_sample = self._start
        self._last_proc_ticks = _read_cpu_ticks(self._proc_stat)
        self._last_main_ticks = _read_cpu_ticks(self._main_stat)
        self._last_objects = self._start

        # Reactor lag: a 0.5s timer records how late it actually fired. This
        # measures the same saturation as main_cpu_pct, from the other side.
        self._lag_interval = 0.5
        self._lag_expected = time.monotonic() + self._lag_interval
        self._lags: list[float] = []

        d = data_dir()
        self._path = d / "runstats.tsv"
        self._objects_path = d / "objects.log"
        self._fh = self._path.open("a", encoding="utf-8")
        if self._path.stat().st_size == 0:
            self._fh.write("\t".join(COLUMNS) + "\n")
            self._fh.flush()

        self._stats_loop = None
        self._lag_loop = None

    @classmethod
    def from_crawler(cls, crawler):
        cfg_interval = crawler.settings.getfloat("RUNSTATS_INTERVAL", 30.0)
        cfg_objects = crawler.settings.getfloat("RUNSTATS_OBJECTS_INTERVAL", 600.0)
        ext = cls(crawler, cfg_interval, cfg_objects)
        crawler.signals.connect(ext.spider_opened, signal=signals.spider_opened)
        crawler.signals.connect(ext.spider_closed, signal=signals.spider_closed)
        return ext

    # --- lifecycle ----------------------------------------------------------

    def spider_opened(self, spider) -> None:
        # Deferred so the reactor is installed before this module touches it,
        # the same way scrapy.core.downloader imports it.
        from scrapy.utils.asyncio import create_looping_call

        self._lag_loop = create_looping_call(self._tick_lag)
        self._lag_loop.start(self._lag_interval, now=False)
        self._stats_loop = create_looping_call(self._sample)
        self._stats_loop.start(self.interval, now=False)
        logger.info("runstats writing to %s every %.0fs", self._path, self.interval)

    def spider_closed(self, spider) -> None:
        for loop in (self._stats_loop, self._lag_loop):
            if loop is not None and getattr(loop, "running", False):
                loop.stop()
        self._sample()
        self._fh.close()

    # --- measurement --------------------------------------------------------

    def _tick_lag(self) -> None:
        """Record how late this 0.5s timer fired. Saturation shows up here."""
        now = time.monotonic()
        self._lags.append(max(0.0, now - self._lag_expected))
        self._lag_expected = now + self._lag_interval

    def _cpu_percentages(self, now: float) -> tuple[float, float]:
        span = now - self._last_sample
        proc = _read_cpu_ticks(self._proc_stat)
        main = _read_cpu_ticks(self._main_stat)
        if span <= 0:
            return 0.0, 0.0
        proc_pct = (proc - self._last_proc_ticks) / self._ticks_per_sec / span * 100.0
        main_pct = (main - self._last_main_ticks) / self._ticks_per_sec / span * 100.0
        self._last_proc_ticks = proc
        self._last_main_ticks = main
        return proc_pct, main_pct

    def _downloader_numbers(self) -> tuple[int, int, int]:
        engine = getattr(self.crawler, "engine", None)
        dl = getattr(engine, "downloader", None)
        if dl is None:
            return 0, 0, 0
        transferring = sum(len(s.transferring) for s in dl.slots.values())
        return len(dl.active), len(dl.slots), transferring

    def _scraper_numbers(self) -> tuple[int, float]:
        engine = getattr(self.crawler, "engine", None)
        slot = getattr(getattr(engine, "scraper", None), "slot", None)
        if slot is None:
            return 0, 0.0
        return len(slot.queue), slot.active_size / 1024.0

    def _frontier_numbers(self) -> tuple[int, int]:
        """Frontier size and per-domain queue count.

        CappedScheduler publishes itself on the crawler so this never has to
        reach into engine internals. pqueues is the number the pop() cost
        scales with.
        """
        scheduler = getattr(self.crawler, "capped_scheduler", None)
        if scheduler is None:
            return 0, 0
        size = getattr(scheduler, "_size", 0)
        pqueues = 0
        for attr in ("dqs", "mqs"):
            queue = getattr(scheduler, attr, None)
            pq = getattr(queue, "pqueues", None)
            if pq is not None:
                pqueues += len(pq)
        return size, pqueues

    def _robots_numbers(self) -> tuple[int, int]:
        mw = getattr(self.crawler, "robots_middleware", None)
        if mw is None:
            return 0, 0
        return len(mw._parsers), len(mw._inflight)

    def _sample(self) -> None:
        now = time.monotonic()
        proc_pct, main_pct = self._cpu_percentages(now)

        lags = sorted(self._lags)
        self._lags = []
        lag_max = lags[-1] * 1000 if lags else 0.0
        lag_p50 = lags[len(lags) // 2] * 1000 if lags else 0.0

        dl_active, dl_slots, dl_transferring = self._downloader_numbers()
        scraper_queued, scraper_kb = self._scraper_numbers()
        frontier, pqueues = self._frontier_numbers()
        robots_cached, robots_inflight = self._robots_numbers()

        try:
            from scrapy.resolver import dnscache

            dns_cached = len(dnscache)
        except Exception:
            dns_cached = 0

        spider = self.crawler.spider
        crawled = getattr(getattr(spider, "crawled_log", None), "count", 0)
        discovered = getattr(getattr(spider, "discovered_log", None), "count", 0)
        dupe_skipped = getattr(spider, "dupe_skipped", 0)

        row = (
            f"{time.time():.0f}",
            f"{now - self._start:.0f}",
            f"{proc_pct:.1f}",
            f"{main_pct:.1f}",
            f"{lag_max:.0f}",
            f"{lag_p50:.0f}",
            str(dl_active),
            str(dl_slots),
            str(dl_transferring),
            str(scraper_queued),
            f"{scraper_kb:.0f}",
            str(frontier),
            str(pqueues),
            str(robots_cached),
            str(robots_inflight),
            str(dns_cached),
            str(crawled),
            str(discovered),
            str(dupe_skipped),
            f"{_read_rss_kb() / 1024:.0f}",
        )
        self._fh.write("\t".join(row) + "\n")
        self._fh.flush()
        self._last_sample = now

        if now - self._last_objects >= self.objects_every:
            self._last_objects = now
            self._dump_objects(now - self._start)

    def _dump_objects(self, elapsed: float) -> None:
        """Type histogram of live objects.

        RSS alone cannot tell a real working set from allocator fragmentation.
        If these counts are flat while RSS climbs, the memory is fragmentation
        and MALLOC_ARENA_MAX is the lever, not application code.
        """
        try:
            counts: Counter[str] = Counter()
            for obj in gc.get_objects():
                counts[type(obj).__name__] += 1
            with Path(self._objects_path).open("a", encoding="utf-8") as fh:
                fh.write(f"=== t={elapsed:.0f}s rss={_read_rss_kb() / 1024:.0f}MB ===\n")
                for name, count in counts.most_common(15):
                    fh.write(f"  {name:30s} {count:10d}\n")
                fh.flush()
        except Exception as exc:  # never let measurement kill the crawl
            logger.warning("object dump failed: %s", exc)
