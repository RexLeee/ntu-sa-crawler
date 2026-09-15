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
CONCURRENT_REQUESTS_PER_DOMAIN = _pol["concurrent_requests_per_domain"]
# Scrapy randomizes the delay to 0.5x-1.5x by default. At a 5s delay that
# produces 2.5s gaps, which breaks the 0.2 qps limit. Must stay 0.
DOWNLOAD_DELAY_JITTER = _pol["download_delay_jitter"]
RANDOMIZE_DOWNLOAD_DELAY = False  # pre-2.13 name for the same behaviour

# --- throughput -------------------------------------------------------------
CONCURRENT_REQUESTS = _crawl["concurrent_requests"]
DOWNLOAD_TIMEOUT = _crawl["download_timeout"]
DOWNLOAD_MAXSIZE = _crawl["download_maxsize"]
DOWNLOAD_WARNSIZE = 0
REACTOR_THREADPOOL_MAXSIZE = _crawl["reactor_threadpool_maxsize"]
DNSCACHE_ENABLED = True
DNSCACHE_SIZE = 200_000
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
SCHEDULER_PRIORITY_QUEUE = "scrapy.pqueues.DownloaderAwarePriorityQueue"
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

# --- trimming ---------------------------------------------------------------
COOKIES_ENABLED = _crawl["cookies_enabled"]
RETRY_ENABLED = _crawl["retry_enabled"]
REDIRECT_ENABLED = _crawl["redirect_enabled"]
REDIRECT_MAX_TIMES = _crawl["redirect_max_times"]
AJAXCRAWL_ENABLED = False
LOG_LEVEL = _crawl["log_level"]
TELNETCONSOLE_ENABLED = True

DOWNLOADER_MIDDLEWARES = {
    # Must run before robots so the robots fetch inherits the right slot.
    "crawler.middlewares.slot.SlotKeyMiddleware": 90,
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
