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

## Measured results

| Metric | Before | After |
|---|---|---|
| Seeds | 20 | 1,000 |
| CONCURRENT_REQUESTS | 200 | 3,000 |
| Wall time | 5,241s | 573s |
| Pages crawled | 20,000 | 26,801 |
| **pages/sec** | **3.82** | **46.81** |
| Unique discovered URLs | 2.1M raw | 690,788 unique |
| Peak memory | 5,312 MB, still climbing | 980–1,056 MB, flat |
| Download failures | 9,684 | 0 in 26,801 |
| Politeness violations | **0** at the time | **0**, min gap 5.000s |
| robots violations | not collected | **0** over 786 re-checked URLs |

The "before" column is an 87 minute run on leepc; "after" is 10 minutes on
macOS, which is the slower machine. The rate difference is not machine speed.

Three ceilings were removed, and raising throughput exposed a fourth problem
that had been invisible at 3.8 pages/s. Each is described below.

> **The "0 download failures" figure above does not survive at full speed.**
> It comes from the 10 minute macOS run, which was too slow to reach the
> state where failures appear. A 28 minute run on leepc at 35 pages/s
> reported this instead:
>
> | | |
> |---|---|
> | Requests issued | 221,897 |
> | Responses received | 83,341 |
> | Exceptions | **140,331 (63%)** |
> | of which download timeouts | 132,878 |
> | robots.txt fetches | 45,825 |
> | of which timed out | **40,957 (89%)** |
>
> A robots.txt fetch that times out is treated as permission granted, so at
> that rate most domains were crawled without their rules ever being read.
> That is a compliance risk, not only a throughput one. See
> "The reactor thread is the fourth ceiling" below.

`unique new URLs per page` was a stable ~50 across early runs and remains the
right basis for projecting the discovered metric, though it falls as the crawl
revisits known link targets: 49.5 at 3,000 pages, 25.8 at 26,801.

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
never exceed 34,560 pages. The governing relation is:

```
pages/sec <= active_domains / download_delay
```

An 87 minute run managed 3.82 pages/s, which implies only about 19 domains
were ever in flight at once, against 18,344 discovered. Finding what held it
there took reading Scrapy's source and measuring the local costs, because
every plausible explanation was wrong:

| Suspected cause | Measured | Verdict |
|---|---|---|
| CPU parsing links | 0.91 ms/page, 1,096 pages/s on one core | not the limit |
| Bandwidth | never approached | not the limit |
| Frontier in memory | JOBDIR was already moving it to disk | not the limit |

Three real ceilings were found, one of them introduced by our own code.

### 1. The frontier cap was O(number of domains)

`CappedScheduler.enqueue_request` called `len(self)`. `Scheduler.__len__` sums
`len()` over every per-domain queue, so its cost grows with the crawl:

| Live domain queues | `len(self)` | Max enqueues/sec |
|---|---|---|
| 1,000 | 33 µs | 30,043 |
| 10,000 | 333 µs | 3,001 |
| 100,000 | 3,371 µs | 297 |

One page yields ~49 requests, so 100 pages/s needs 4,900 enqueues/s. The run
had reached 12,269 domains, which put this method's own ceiling near
61 pages/s. It now tracks the size incrementally, which `tests/test_scheduler.py`
locks in by asserting the cost stays flat as the domain count grows 10x.

### 2. CONCURRENT_REQUESTS is a domain budget, not a download budget

`scrapy/core/downloader/__init__.py` adds a request to `slot.active` when it is
*queued*, before the delay wait:

```python
async def _enqueue_request(self, request):
    key, slot = self._get_slot(request)
    slot.active.add(request)          # counted from here
    ...
    slot.queue.append((request, d))   # then waits up to 5s
```

`needs_backout()` compares `CONCURRENT_REQUESTS` against that set, so the
setting really caps how many requests may sit parked on domain timers, and
therefore how many domains can be in flight. At 200 the run held about 57
distinct domains.

A parked request holds no socket and costs roughly 3 KB, so raising this is
close to free: 3,000 costs about 9 MB.

### 3. DNS failures consumed half the thread pool

`CachingThreadedResolver` runs lookups in the reactor thread pool, and
`scrapy/resolver.py` still carries a `# TODO: cache misses` where a negative
cache would go. With the 60s default timeout, one dead domain holds a thread
for a full minute.

That run hit 2,118 DNS failures in 5,241s. Against a 50-thread pool
(262,050 thread-seconds available) those failures alone claimed 127,080, or
**48% of the entire pool**. `DNS_TIMEOUT` is now 5s and the pool is 200.

### Memory was the robots cache, not the frontier

`RobotsTxtMiddleware` keeps a parser per hostname in a plain dict and never
evicts one. Measured sizes: 0.9 KB for a one-rule file, 7.1 KB for a typical
one, 369 KB for a 500-rule one. At 500k hosts that dict alone reaches several
GB, which matches the 5,312 MB the long run reached.

It is now a bounded `LocalCache`. Eviction is safe because a miss re-fetches
`robots.txt` and waits for it; the check is never skipped. `tests/test_robots_cache.py`
asserts exactly that, along with the eviction not breaking in-flight fetches.

### The reactor thread is the fourth ceiling, but not for the reason below

> **Corrected 2026-09-15 14:00, by profiling the running crawl.** The section
> that follows reasons from latency to a conclusion about parsing. The
> conclusion about the *thread* is right and is now measured directly: main
> thread at 99% CPU for a full two hours, on a machine with 8 cores. The
> conclusion about the *cause* is wrong.
>
> `py-spy` over 6,035 samples, confirmed by a second independent capture:
>
> | Share of CPU | Where |
> |---|---|
> | **50.7%** | `_dqpop` → `DownloaderAwarePriorityQueue.pop` |
> | 31.4% | inside it, `pqueues.stats()` |
> | 11.8% | actually downloading |
> | **5.2%** | `parse`, the presumed culprit |
> | 1.1% | `canonicalize_url` |
>
> Parsing is 5% of the work. Picking the next domain is half of it. The
> scraper queue, which the argument below assumes is backed up, measured
> empty for almost the entire run.
>
> The inference failed in a specific way worth recording: a saturated event
> loop delays *everything*, so slow page delivery was a symptom of the
> saturation rather than its cause. Latency told us the loop was busy. Only
> the profile could say what it was busy with. The fix is the O(domains)
> `pop()` described in the next section, not a faster parser.

### The original reasoning, kept for the record

Removing the first three raised the rate to 35 pages/s and produced a 63%
exception rate. The cause is not the network. Both plausible network
explanations were measured and ruled out on the machine that runs the crawl:

| Test | Result |
|---|---|
| WSL DNS forwarder, 400 names at 100 concurrent | p50 88 ms, p99 600 ms, 0 failures |
| WSL NAT, 300 real hosts at 300 concurrent `curl` | 2 hard timeouts out of 300 |

The evidence points at CPU instead. Successful pages took a median of 13.65 s
from dispatch to `parse()`, against a 10 s download timeout. Time that a
download cannot have spent must have been spent queued for parsing, and the
scraper queue held 100 MB, roughly 770 responses, which at 45 pages/s is the
17 s that matches the p90 of 17.86 s.

Scrapy parses on the reactor thread, alongside every TLS handshake, socket
read and timer. When parsing saturates that thread, a completed `connect()`
is not serviced and the 10 s timer fires on a host that is in fact answering.
The signature is in the data: 5,111 timeouts landed on 1,091 hosts that also
returned pages in the same run, and the log shows 73,275 connect-stage
timeouts distinct from the download-stage ones.

The machine has 8 cores and the crawl uses one. `crawler/extensions/runstats.py`
now samples main-thread CPU and reactor lag directly, so the next run
confirms or refutes this rather than inferring it.

Three quarters of the work on that thread was avoidable. A measured run
filtered 3,319,826 duplicate requests against 1,106,885 scheduled, and each
duplicate was allocated as a `Request`, carried through the spider middleware
chain and handed to the engine before being dropped. The spider now tests the
URL against the Bloom filter before building anything, using a fingerprint
proven identical to Scrapy's in `tests/test_dupefilter.py`.

### pop() is O(domains in the frontier), and it is the real ceiling

`DownloaderAwarePriorityQueue.pop()` builds a list over every per-domain queue
and then scans it. Reproduced on the same `_next_slot` logic:

| Domains in frontier | One pop | Predicted cost at 100 pops/s |
|---|---|---|
| 3,430 (28 min) | 0.24 ms | 2% of a core |
| 10,000 | 0.86 ms | 9% |
| 35,000 (48 h, projected) | 2.94 ms | **29%** |
| 100,000 | 8.47 ms | **85%** |

**The measured cost is far worse than this table predicts.** At roughly 5,000
queues the profile puts `pop()` at 50.7% of all CPU, where the microbenchmark
predicted about 4%. The benchmark under-counted allocation: `stats()`
constructs a tuple per queue on every single call, and that allocation
dominates once the numbers are real.

So this is not a ceiling that arrives late in a long run. It is the ceiling
the crawl is already sitting against, and fixing it is worth more than
sharding across processes because it needs no new failure modes. Sharding
remains available afterwards; the profile should be re-taken first, since
removing half the CPU cost will move the bottleneck somewhere new.

#### The fix, and what it moved

`crawler/pqueue.py` keeps the domains in a deque and stops at the first one
with no active download, rotating what it skips to the back. The selection
rule is unchanged: fewest active downloads first, ties broken by rotation.
With `CONCURRENT_REQUESTS_PER_DOMAIN=1` almost every queue holds 0 or 1
active downloads, so the rule is really "find an idle domain", and finding one
does not require looking at all of them. The scan is capped by
`PQUEUE_SCAN_LIMIT`; on reaching it the least-active domain seen wins, which
is the parent's answer over a window.

Microseconds per `pop()`, measured:

| Domains | Parent | Ring | Speedup |
|---|---|---|---|
| 200 | 3.98 | 0.30 | 13x |
| 2,000 | 141.67 | 0.90 | 157x |
| 20,000 | 1,734.12 | 1.09 | 1,597x |
| 50,000 | 4,863.36 | 1.12 | 4,340x |

The parent is linear; the ring is flat. A second profile taken under the same
conditions:

| Component | Before | After |
|---|---|---|
| `pop()` total | 50.5% | 3.8% |
| `stats()` | 31.7% | gone |
| `_active_downloads` | 20.4% | gone |
| `_next_slot` | 10.6% | gone |
| `parse` | 5.1% | 21.7% |
| `_extract_links` | 4.9% | 20.9% |

And in the crawl itself:

| Metric | 2h run | 10 min run after the fix |
|---|---|---|
| pages/s | 23.0 | 37.6 |
| reactor lag p50 | 1,200-2,000 ms | 15-430 ms |

The reactor thread is still near saturation, but the expensive work is now
link extraction and parsing, which is work the crawl actually needs. Reactor
lag falling by an order of magnitude is the direct evidence that `pop()` was
what the loop had been spending its time on.

One caveat worth recording: `pop()` must never return `None` while queues hold
requests. `Scheduler.has_pending_requests()` answers from `__len__`, so a
`None` there would make the engine call `next_request()` on every heartbeat
and get nothing, forever. `tests/test_pqueue.py` checks that with every domain
busy and a scan limit of 2. Five of its six checks also pass against the
parent class, which is what makes them evidence that the semantics were
preserved rather than merely self-consistent.

`DEPTH_PRIORITY` was also removed here. `_next_slot` chooses purely by active
download count and never reads request priority, so it could not affect which
domain was visited next. Its only measurable effect was splitting each
domain's queue across priority buckets: 3,430 domains produced 7,569 queue
directories and 15,422 open descriptors, two per queue.

## Raising throughput exposed a real politeness violation

At 3.8 pages/s compliance was clean. At 40 pages/s the same code produced 50
violations, the worst at 4.694s. This was not a measurement artifact.

`Slot._process_queue` sets `slot.lastseen` when it *schedules* the download
coroutine:

```python
now = monotonic()
...
while slot.queue and slot.free_transfer_slots() > 0:
    slot.lastseen = now                                    # scheduled at T
    _schedule_coro(self._wait_for_download(slot, request, queue_dfd))
```

The socket write happens when the event loop reaches that coroutine, at T+d.
Scrapy spaces the schedule times by exactly 5s, so the gap the server sees is
`5 + (d₂ - d₁)`. Under load the loop was blocked for up to 1.4s at a time, so
d varied enough to push real gaps below the limit.

Margin alone could not fix it. Raising `DOWNLOAD_DELAY` to 5.5s cut 50
violations to 1, because the jitter is a long tail:

| percentile | jitter |
|---|---|
| p50 | 36 ms |
| p90 | 147 ms |
| p99 | 302 ms |
| max | 559 ms |

`crawler/dispatch.py` now enforces the floor at the last point before the
request goes out. Two details make it correct:

- **Claim the turn before awaiting.** Coroutines released together would
  otherwise read the same previous timestamp and wake at the same instant. The
  first attempt did exactly that and left two of three gaps at 0.000s.
- **Re-check the clock after every sleep, and advance from the real dispatch
  time.** `asyncio.sleep` guarantees a lower bound, not an exact wake time, so
  a single sleep still lands late by a varying amount.

Result: 0 violations over 26,801 requests, minimum gap 5.000s.

The general lesson is that a rate limit expressed as a scheduling delay is not
a rate limit. It becomes one only when enforced against the clock at the point
the request leaves.

## Seed selection

The assignment allows 1,000 seeds, and each one opens an independent 5 second
pipeline, so seed count and domain diversity both feed directly into
throughput.

`seeds/build_seeds.py` draws from [Tranco](https://tranco-list.eu/), a research
ranking that averages four commercial lists over 30 days to resist
manipulation. Rank order alone is a poor seed list:

1. **Infrastructure is dropped.** About 4% of the top 3,000 are names like
   `gstatic.com` or `akamai.net` that serve no followable HTML.
2. **One URL per registered domain.** Two hosts on one eTLD+1 share a timer and
   buy no extra parallelism.
3. **Sampled across TLDs.** The top 3,000 is 55% `.com`. A prefix would
   concentrate the crawl in one slice of the web, so each TLD gets a quota
   proportional to its share, capped at 35%.

`seeds/probe.py` then fetches each candidate once and keeps only those
returning HTML with at least 5 followable links. Of 3,000 candidates, 1,652
were rejected:

| Reason | Count |
|---|---|
| HTML but no usable links (JavaScript-rendered) | 733 |
| DNS does not resolve | 446 |
| Timeout | 173 |
| Certificate error | 89 |
| Redirect loop | 87 |

The final list is 1,000 URLs across 291 TLDs, each verified reachable, with a
mean of 115 links on the landing page.

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
