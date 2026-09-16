"""Scrapy settings, derived entirely from config.toml."""

from __future__ import annotations

from crawler.config import load_config

_cfg = load_config()
_ident = _cfg["identity"]
_pol = _cfg["politeness"]
_crawl = _cfg["crawl"]
_mem = _cfg["memory"]
_state = _cfg["paths"]

BOT_NAME = _ident["bot_name"]
USER_AGENT = _ident["user_agent"]

SPIDER_MODULES = ["crawler.spiders"]
NEWSPIDER_MODULE = "crawler.spiders"

# --- politeness -------------------------------------------------------------
ROBOTSTXT_OBEY = _pol["obey_robotstxt"]
ROBOTSTXT_PARSER = "scrapy.robotstxt.ProtegoRobotParser"
DOWNLOAD_DELAY = _pol["download_delay"]
# 1 is load-bearing, not a throttle setting. It is what stops a second request
# for a domain from being popped while the first is still transferring, which
# is what makes the completion-anchored delay in crawler/downloader.py a
# guarantee rather than a hope.
CONCURRENT_REQUESTS_PER_DOMAIN = _pol["concurrent_requests_per_domain"]
# Two overrides, both about compliance:
#
# 1. Scrapy keys a download slot by meta['download_slot'], else by hostname.
#    Neither is the registered domain, and meta is inherited by any request
#    built from another one. The subclass derives the key from the URL, which
#    is the same definition ops/verify_politeness.py checks.
# 2. Scrapy spaces the POPS from the slot queue, not the requests. The
#    subclass advances slot.lastseen at the response's completion instead, so
#    DOWNLOAD_DELAY becomes a floor on the interval the server sees.
#
# See crawler/downloader.py.
DOWNLOADER = "crawler.downloader.DomainSlotDownloader"
# A meta refresh builds its target with source_request.replace(), which copies
# meta wholesale and inherits dont_filter. That gave the target the source
# domain's timer and let it skip the dupefilter: a 10 minute run fetched
# dozens of URLs twice. A broad crawl does not need to follow meta refresh,
# and DOWNLOADER above would now keep it compliant either way.
METAREFRESH_ENABLED = _crawl["follow_meta_refresh"]
# Scrapy randomizes the delay to 0.5x-1.5x by default. At a 5s delay that
# produces 2.5s gaps, which breaks the 0.2 qps limit. Must stay 0.
#
# RANDOMIZE_DOWNLOAD_DELAY is not set alongside it. Setting it at all makes
# Downloader._default_jitter emit a deprecation warning in 2.19, and this
# setting is the only one it reads when the deprecated name is left alone.
DOWNLOAD_DELAY_JITTER = _pol["download_delay_jitter"]

# --- throughput -------------------------------------------------------------
CONCURRENT_REQUESTS = _crawl["concurrent_requests"]
DOWNLOAD_TIMEOUT = _crawl["download_timeout"]
DOWNLOAD_MAXSIZE = _crawl["download_maxsize"]
DOWNLOAD_WARNSIZE = 0
REACTOR_THREADPOOL_MAXSIZE = _crawl["reactor_threadpool_maxsize"]
DNSCACHE_ENABLED = True
DNSCACHE_SIZE = _crawl["dns_cache_size"]
# CachingThreadedResolver runs lookups in the reactor thread pool and has no
# negative cache, so at the 60s default one dead domain holds a thread for a
# minute. A measured run lost 48% of the pool to 2,118 DNS failures.
DNS_TIMEOUT = _crawl["dns_timeout"]

# Response bodies queued for parsing. The 5 MB default is about 62 average
# pages, which starts throttling the engine above roughly 60 pages/s.
SCRAPER_SLOT_MAX_ACTIVE_SIZE = _crawl["scraper_slot_max_active_size"]

# --- crawl order ------------------------------------------------------------
# Positive DEPTH_PRIORITY with FIFO queues yields breadth-first order, which
# spreads load across domains.
DEPTH_PRIORITY = _crawl["depth_priority"]
SCHEDULER_DISK_QUEUE = "scrapy.squeues.PickleFifoDiskQueue"
SCHEDULER_MEMORY_QUEUE = "scrapy.squeues.FifoMemoryQueue"
# DownloaderAwarePriorityQueue picks the next domain by scanning every
# per-domain queue, which a profile put at 52% of all CPU. The subclass keeps
# the same selection rule but stops at the first idle domain. See
# crawler/pqueue.py.
SCHEDULER_PRIORITY_QUEUE = "crawler.pqueue.RingDownloaderAwarePriorityQueue"
PQUEUE_SCAN_LIMIT = _crawl["pqueue_scan_limit"]
# Required by DownloaderAwarePriorityQueue, which raises ValueError otherwise.
CONCURRENT_REQUESTS_PER_IP = 0

# --- memory ------------------------------------------------------------------
# Without JOBDIR the disk queue above is inert: Scheduler.open skips it and
# every queued request stays a live object in the memory queue. A measured
# hour put 1.4M requests there and RSS rose 1.6 MB/s with no sign of
# flattening. Setting JOBDIR is what actually moves the frontier to disk.
JOBDIR = _state["jobdir"]

# RFPDupeFilter holds every fingerprint in a set at roughly 131 bytes each,
# which is about 6.5 GB at 50M URLs. The Bloom filter costs 171 MB instead.
DUPEFILTER_CLASS = "crawler.dupefilter.BloomDupeFilter"
BLOOM_DUPEFILTER_CAPACITY = _mem["bloom_capacity"]
BLOOM_DUPEFILTER_ERROR_RATE = _mem["bloom_error_rate"]

# Nothing in Scrapy caps the frontier, so a breadth-first crawl fills it about
# 33x faster than it drains. This is the backstop.
SCHEDULER = "crawler.scheduler.CappedScheduler"
FRONTIER_MAX_SIZE = _mem["frontier_max_size"]

# Hard stop. Defaults to 0, meaning no limit at all.
MEMUSAGE_ENABLED = True
MEMUSAGE_LIMIT_MB = _mem["memusage_limit_mb"]
MEMUSAGE_WARNING_MB = _mem["memusage_warning_mb"]

# RobotsTxtMiddleware caches a parser per hostname and never evicts one. At
# 8 KB each that dict alone reaches several GB in a broad crawl, which was the
# main driver of memory growth. See crawler/middlewares/robots.py.
ROBOTS_CACHE_SIZE = _mem["robots_cache_size"]
# An unreadable robots.txt means "do not crawl", not "crawl anything". See
# crawler/middlewares/robots.py.
ROBOTS_STRICT_ON_FAILURE = _mem["robots_strict_on_failure"]
ROBOTS_FAILURE_TTL = _mem["robots_failure_ttl"]
MAX_CRAWL_DELAY = _pol["max_crawl_delay"]

# --- trimming ---------------------------------------------------------------
COOKIES_ENABLED = _crawl["cookies_enabled"]
RETRY_ENABLED = _crawl["retry_enabled"]
REDIRECT_ENABLED = _crawl["redirect_enabled"]
REDIRECT_MAX_TIMES = _crawl["redirect_max_times"]
AJAXCRAWL_ENABLED = False
LOG_LEVEL = _crawl["log_level"]
TELNETCONSOLE_ENABLED = _crawl["telnet_console"]

# Scrapy logs every download failure at ERROR with a traceback, which
# LOG_LEVEL=WARNING does not suppress. A broad crawl fails constantly: 94,658
# such records filled a 495 MB log in 28 minutes. The counts survive in the
# stats dump.
LOG_FORMATTER = "crawler.logformatter.QuietLogFormatter"

# --- measurement -------------------------------------------------------------
# The external sampler sees RSS and file descriptors. It cannot see main-thread
# CPU, reactor lag, the scraper queue or the per-domain queue count, which are
# the numbers that decide whether this survives 48 hours.
EXTENSIONS = {
    "crawler.extensions.runstats.RunStats": 500,
    # CLOSESPIDER_TIMEOUT cannot end this crawl: its close waits for every
    # request parked on a per-domain timer, which took over five minutes. This
    # cancels the parked ones first. See crawler/extensions/shutdown.py.
    "crawler.extensions.shutdown.TimedShutdown": 501,
}
RUNSTATS_INTERVAL = _crawl["runstats_interval"]
RUNSTATS_OBJECTS_INTERVAL = _crawl["runstats_objects_interval"]
# The cyclic collector walks 1.7M protego objects on every gen-2 pass, and it
# does it on the reactor thread. See config.toml [memory].
GC_THRESHOLD0 = _mem["gc_threshold0"]
GC_FREEZE_AT_START = _mem["gc_freeze_at_start"]
# Set per run by ops/run_hour.sh with -s RUN_DURATION=<seconds>. 0 disables it,
# which is what a manual `scrapy crawl` gets.
RUN_DURATION = 0

DOWNLOADER_MIDDLEWARES = {
    "scrapy.downloadermiddlewares.robotstxt.RobotsTxtMiddleware": None,
    "crawler.middlewares.robots.PoliteRobotsTxtMiddleware": 100,
    # A cross-domain redirect would otherwise keep the source domain's slot
    # and bypass the destination's rate limit.
    "scrapy.downloadermiddlewares.redirect.RedirectMiddleware": None,
    "crawler.middlewares.redirect.SlotAwareRedirectMiddleware": 600,
}

ITEM_PIPELINES = {}

# REQUEST_FINGERPRINTER_IMPLEMENTATION was removed in Scrapy 2.13. The 2.7
# scheme is now the only one, so setting it here did nothing.
FEED_EXPORT_ENCODING = "utf-8"
