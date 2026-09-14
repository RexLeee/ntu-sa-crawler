# ntu-sa-crawler

Polite broad crawler for the NTU IM system architecture starter project.

Two hard constraints drive every design decision:

- Honor `robots.txt`.
- At most 0.2 qps per domain, i.e. a 5 second gap between requests to one site.

## Quick start

```bash
uv sync

# 5 minute smoke run against tests/seeds_smoke.txt
uv run scrapy crawl broad -s CLOSESPIDER_TIMEOUT=300

# compliance proofs
uv run python ops/verify_politeness.py     # must print "VIOLATIONS : 0"
uv run python ops/verify_robots.py         # must print "VIOLATIONS : 0"

# measured metrics and projection
uv run python ops/report_metrics.py

# regression test for the dispatch timer
uv run python tests/test_dispatch.py
```

All tunable values live in `config.toml`. No parameter is hardcoded in the
Python sources.

## How politeness is enforced

### The domain is the eTLD+1

`crawler/slot.py` maps every URL to its registered domain and puts it in
`meta['download_slot']`. Scrapy keys download slots by hostname by default,
which would let `en.wikipedia.org` and `simple.wikipedia.org` each run their
own 5 second timer and double the real request rate against one site.

PSL private domains are enabled, so `foo.blogspot.com` and `bar.blogspot.com`
count as separate domains. They are separate sites on shared infrastructure.

### Jitter is off

Scrapy randomizes the delay to 0.5x–1.5x by default. At a 5 second setting
that produces 2.5 second gaps, which breaks the limit. `DOWNLOAD_DELAY_JITTER`
is pinned to 0, and the legacy `RANDOMIZE_DOWNLOAD_DELAY` is set to False.

### Crawl-delay is honored

Scrapy's `RobotsTxtMiddleware` parses `Crawl-delay` but never applies it
(scrapy/scrapy#892). `crawler/middlewares/robots.py` subclasses it to:

1. Build the `robots.txt` request with our `download_slot`, so the robots
   fetch shares the site's timer instead of getting a free one.
2. Raise the slot delay when a site asks for more than 5 seconds, capped at 60.

## Measuring compliance correctly

This took three attempts and is the most subtle part of the project.

`DOWNLOAD_DELAY` spaces apart the moments requests **start**, not the moments
responses arrive. Three plausible measurement points are all wrong:

| Source | Why it is wrong |
|---|---|
| Response arrival time | Includes variable network latency, so gaps look shorter than they are. Produced false violations at ~4.9s. |
| Downloader middleware `process_request` | Runs at enqueue time. `Downloader.fetch` wraps `_enqueue_request` in the middleware chain, so every queued request gets the same timestamp. Produced false violations at 0.000s. |
| `recv - meta['download_latency']` | `download_latency` covers only the transfer, excluding DNS and TCP/TLS setup, so the derived start lands late. Produced a false violation at 4.675s. |

The correct point is `Downloader._download`, the first thing to run after the
slot's delay timer releases a request. `crawler/dispatch.py` stamps
`meta['dispatch_time']` there.

`tests/test_dispatch.py` locks this in against a local HTTP server: four
requests on one slot with a 2 second delay must dispatch 2 seconds apart.

## Measured results (20 seeds)

| Metric | macOS, c=20 | macOS, c=200 | leepc, c=200 |
|---|---|---|---|
| Wall time | 302s | 536s | 660s |
| Requests | 465 | 1,657 | 3,610 |
| Domains touched | 442 | 1,442 | 3,081 |
| Domains seen | — | — | 12,269 |
| Unique discovered URLs | 25,297 | 81,481 | 175,903 |
| Unique new URLs per page | 54.4 | 49.2 | 48.7 |
| pages/sec | 1.54 | 3.09 | 5.53 |
| Peak memory | 249 MB | 511 MB | 1,078 MB |
| Politeness violations | **0** (4.999s) | **0** (4.996s) | **0** (4.990s) |

Compliance held unchanged at 10x concurrency and on a second machine, which is
the property that matters: the limit is enforced per slot, so it does not
degrade under load.

`unique new URLs per page` sits near 50 on every run and on both machines. It
is the most stable number here, which makes it the right basis for projecting
the discovered metric.

All three runs are still ramping up. leepc had touched 3,081 domains but
*seen* 12,269 when it stopped, so the frontier was nowhere near steady state
and none of these rates can be extrapolated to 48 hours yet.

### A cross-domain redirect bypassed the rate limit

leepc's first run reported two violations, both on `developers.google.com`,
one pair dispatched at an identical timestamp.

Scrapy's `RedirectMiddleware` builds the follow-up request with
`source_request.replace(url=...)`, which copies meta wholesale. The stale
`download_slot` travels with it, so a redirect from `youtube.com` to
`google.com` lands on youtube's timer and skips google's entirely. That run
followed 3,110 redirects, which was enough to collide.

`crawler/middlewares/redirect.py` re-keys the slot on the destination URL.
The re-run reported 0 violations.

The macOS runs never exposed this. They were too slow to accumulate enough
cross-domain redirects. The general lesson is that a single `DOWNLOAD_DELAY`
setting does not deliver politeness on its own: every internal path that
manufactures a new request has to be audited.

### The fd limit is the first ceiling, not memory

WSL's default `ulimit -n` is 1024. `CONCURRENT_REQUESTS=200` plus DNS sockets
and log handles approaches it, so raising concurrency hits
"too many open files" before memory or bandwidth. The hard limit is 1048576,
so `ops/run_hour.sh` simply raises the soft limit at startup.

## Throughput is bounded by domain count, not by the machine

One domain yields at most one page per 5 seconds, so over 48 hours it can
never exceed 34,560 pages. Three independent ceilings apply:

```
pages/sec <= active_domains / download_delay      (politeness)
pages/sec <= CONCURRENT_REQUESTS / mean_latency   (in-flight budget)
pages/sec <= bandwidth / mean_page_size           (network)
```

The smoke runs are bound by the **second** ceiling, not by bandwidth. At 20
concurrent requests and 0.29s latency the cap is about 4 pages/s regardless of
how fast the link is; measured 1.54. Raising to 200 moved it to 3.09.

Neither run approached the bandwidth ceiling, so `CONCURRENT_REQUESTS` and the
number of live domains are the levers that matter. Concurrency costs memory
roughly linearly: 249 MB at 20, 511 MB at 200.

## Calibrating the 48 hour projection

`unique new URLs per page` is stable near 50 across both runs, so the
discovered metric tracks crawled almost linearly:

```
discovered ≈ crawled × 50
```

The crawled figure depends on the steady-state front size, which a 5–9 minute
run cannot reach: the 200-concurrency run was still adding domains when it
stopped (1,442 touched and climbing). Projections from these samples are
therefore lower bounds, not estimates. A multi-hour run on the target machine
is needed to fix the steady-state rate.

## Design choices that follow from this

- **New domains get priority.** Each new domain opens another parallel 5 second
  pipeline, so its first URL outranks another page on a domain already held.
- **Per-domain caps.** A domain stops being queued after
  `max_crawled_per_domain`. Large sites generate URLs without limit but can
  only ever contribute one page per 5 seconds, so the frontier slot is worth
  more to a new domain.
- **Regex link extraction.** `crawler/extract.py` scans for `href` rather than
  building an lxml tree, roughly 5–8x faster. Some exotic markup is missed,
  which is an acceptable trade when the metric is volume.
- **Small pages preferred.** `download_maxsize` is 512 KB. Page size correlates
  weakly with link count, so capping it raises links per byte.

## Layout

```
config.toml                       all tunable parameters
crawler/config.py                 config loader
crawler/settings.py               Scrapy settings, derived from config.toml
crawler/slot.py                   eTLD+1 politeness key
crawler/dispatch.py               dispatch-time stamping (see above)
crawler/extract.py                regex href extraction
crawler/filters.py                canonicalization and trap filtering
crawler/logwriter.py              gzipped TSV writers
crawler/middlewares/slot.py       assigns download_slot
crawler/middlewares/robots.py     robots.txt + Crawl-delay
crawler/middlewares/redirect.py   re-keys the slot across redirects
crawler/spiders/broad.py          the crawler
ops/verify_politeness.py          proves the rate limit held
ops/verify_robots.py              proves robots.txt was respected
ops/report_metrics.py             Tier 1 metrics and projection
ops/run_hour.sh                   long run with resource sampling
ops/report_growth.py              fits the memory and domain growth curves
ops/LEEPC_SETUP.md                WSL2 host setup and tuning
tests/test_dispatch.py            dispatch timer regression test
tests/seeds_smoke.txt             20 seeds for the smoke run
```

## Log format

`data/crawled*.log.gz`, tab separated:

```
dispatch_ts  recv_ts  url  status  content_type  n_links
```

`data/discovered*.log.gz`: one URL per line. Deduplicate with
`sort -u` for the final count.

### Known limit in the analysis path

`ops/report_metrics.py` holds every discovered URL in a Python `set` to count
uniques. That is fine for the ~1M URLs of a one hour run. At the tens of
millions a 48 hour run produces, the script itself would exhaust memory, so
the final count has to come from `sort -u` on disk or from a HyperLogLog
estimate instead.
