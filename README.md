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

## Measured results (macOS dev machine, 20 seeds)

| Metric | `CONCURRENT_REQUESTS=20` | `CONCURRENT_REQUESTS=200` |
|---|---|---|
| Wall time | 302s | 536s |
| Requests | 465 | 1,657 |
| 2xx rate | 100% | 100% |
| Domains touched | 442 | 1,442 |
| Unique discovered URLs | 25,297 | 81,481 |
| Links per page | 81.0 | 79.4 |
| Unique new URLs per page | 54.4 | 49.2 |
| pages/sec | 1.54 | 3.09 |
| Latency p50 / p90 | 0.29s / 0.82s | 0.36s / 1.15s |
| Peak memory | 249 MB | 511 MB |
| Politeness violations | **0** (min gap 4.999s) | **0** (min gap 4.996s) |
| robots.txt violations | **0** | not re-run |

Compliance held unchanged at 10x concurrency, which is the property that
matters: the limit is enforced per slot, so it does not degrade under load.

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
crawler/spiders/broad.py          the crawler
ops/verify_politeness.py          proves the rate limit held
ops/verify_robots.py              proves robots.txt was respected
ops/report_metrics.py             Tier 1 metrics and projection
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
