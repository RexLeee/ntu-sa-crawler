"""Scrapy settings, derived entirely from config.toml."""

from __future__ import annotations

from crawler.config import load_config

_cfg = load_config()
_ident = _cfg["identity"]
_pol = _cfg["politeness"]
_crawl = _cfg["crawl"]

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
REACTOR_THREADPOOL_MAXSIZE = 50
DNSCACHE_ENABLED = True
DNSCACHE_SIZE = 200_000

# --- crawl order ------------------------------------------------------------
# Positive DEPTH_PRIORITY with FIFO queues yields breadth-first order, which
# spreads load across domains and keeps memory bounded.
DEPTH_PRIORITY = _crawl["depth_priority"]
SCHEDULER_DISK_QUEUE = "scrapy.squeues.PickleFifoDiskQueue"
SCHEDULER_MEMORY_QUEUE = "scrapy.squeues.FifoMemoryQueue"
SCHEDULER_PRIORITY_QUEUE = "scrapy.pqueues.DownloaderAwarePriorityQueue"

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
}

ITEM_PIPELINES = {}

REQUEST_FINGERPRINTER_IMPLEMENTATION = "2.7"
FEED_EXPORT_ENCODING = "utf-8"
