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

# a real run: sharding.shards processes, resources sampled throughout
ops/run_hour.sh 600

# compliance proofs, across every shard's log
# Default source is data/dispatched*.log.gz: every request the crawler sent,
# including robots.txt and the ones that failed.
uv run python ops/verify_politeness.py                    # "VIOLATIONS : 0"
uv run python ops/verify_politeness.py --source crawled   # "VIOLATIONS : 0"
uv run python ops/verify_robots.py                        # "VIOLATIONS : 0"
uv run python ops/verify_no_refetch.py     # ~1.4%, see the redirect note

# measured metrics and projection
uv run python ops/report_metrics.py --json data/run-<stamp>/metrics.json
uv run python ops/report_timeline.py data/run-<stamp>

# tests
for t in dispatch downloader handoff pqueue scheduler robots_cache dupefilter shutdown; do
    uv run python tests/test_$t.py
done
```

`data/violation-trace*.log` must be 0 bytes. An entry means the guard in
`crawler/dispatch.py` caught a dispatch closer than 5.0s to the previous one
on that domain, and refused it. Under the design below that cannot happen, so
an entry is a defect rather than a tuning problem. The refusal is also counted
as `dispatch/guard_refused` in `data/stats*.json`, which the acceptance list
requires to be 0 or absent.

### Honouring Crawl-delay

Scrapy's `RobotsTxtMiddleware` parses `Crawl-delay` but never applies it
(scrapy/scrapy#892). `crawler/middlewares/robots.py` raises `slot.delay` to
the requested value, capped at `max_crawl_delay = 60.0`. That is sufficient
on its own now, because `slot.delay` is the single throttle and it counts from
the previous response's completion.

The 60 second cap is also what keeps slot collection safe.
`Downloader._slot_gc` drops a slot after 60s idle, and a rebuilt slot's delay
would be back at the global 5.1s. Since no delay in play exceeds 60s, a
rebuilt slot's first request is already at least its own delay after the
domain's last one.

## Sitemaps are never fetched

The assignment excludes URLs found inside sitemaps from both Tier 1 metrics.
No such URL can enter this crawl's data, because the crawler has no code path
that reaches one:

- `sitemap.xml` is never requested. The only non-page fetch anywhere is
  `robots.txt`, in `crawler/middlewares/robots.py`.
- `robots.txt` bodies are handed to Protego for allow/deny rules and
  `Crawl-delay` only. `Sitemap:` lines are never read.
- Scrapy's `SitemapSpider` is not used; `BroadSpider` extends plain `Spider`.
- Link discovery is a regex over `<a href>` in `crawler/extract.py`. The
  `<urlset>` and `<loc>` elements a sitemap is made of are not matched, and
  `.xml` is in `filters.skip_extensions`.

`grep -rni sitemap crawler/` returns nothing, which is the check.

An `<a href="/sitemap.xml">` link on an ordinary HTML page is followed like
any other link, and the page it fetches counts as one crawled page. What never
happens is parsing that file for the URLs inside it.

All tunable values live in `config.toml`. No parameter is hardcoded in the
Python sources.

## How politeness is enforced

### The domain is the eTLD+1, and it is always derived from the URL

`crawler/slot.py` maps every URL to its registered domain. Scrapy keys download
slots by hostname by default, which would let `en.wikipedia.org` and
`simple.wikipedia.org` each run their own 5 second timer and double the real
request rate against one site.

`crawler/downloader.py` overrides `get_slot_key` so the key is computed from
the URL every time. The alternative, which this project used first, is to put
the key in `meta['download_slot']` and let Scrapy read it back. That is what
Scrapy's own API invites, and it is wrong for the same reason twice over, as
the section on meta refresh below describes: meta is state that gets copied
from one request to another, so a key placed there can end up describing a
domain the request is no longer going to.

Deriving instead of storing removes the whole class of failure. The key is a
pure function of the URL, so no code path can hand a request a stale identity,
and it is the same function `ops/verify_politeness.py` groups by. The timer and
the proof cannot disagree.

PSL private domains are enabled, so `foo.blogspot.com` and `bar.blogspot.com`
count as separate domains. They are separate sites on shared infrastructure.

### Jitter is off

Scrapy randomizes the delay to 0.5x–1.5x by default. At a 5 second setting
that produces 2.5 second gaps, which breaks the limit. `DOWNLOAD_DELAY_JITTER`
is pinned to 0. The legacy `RANDOMIZE_DOWNLOAD_DELAY` is deliberately not set
at all: in Scrapy 2.19 setting it emits a deprecation warning, and
`_default_jitter` reads the new name when the old one is left alone.

### The delay counts from the previous response's completion

This is the invariant everything else rests on, and it took four attempts to
get right.

Scrapy spaces the **pops** from a slot's queue, not the requests.
`Downloader._process_queue` writes `slot.lastseen` at the moment it pops, then
hands the download to the event loop, which runs it whenever it gets there. So
`DOWNLOAD_DELAY` bounds the interval between two scheduling decisions and
nothing bounds the interval between the two socket writes that follow.

On this crawl that gap is large. A 20 minute run held the reactor thread at
95–98% CPU for its whole length, with a 0.5s timer firing 1.3–4.6 seconds late
at the median and up to 9.7 seconds late at the maximum.

Three earlier designs tried to close it and each failed in a way worth
recording:

| Attempt | Why it failed |
|---|---|
| Raise `DOWNLOAD_DELAY` to 5.5s for margin | Cut 50 violations to 1. The lateness is a long tail (p50 36ms, p99 302ms, max 559ms), so no fixed margin covers it. |
| A second timer in `dispatch.py` that slept to a claimed turn | A sleeping coroutine is not in `slot.transferring`, so Scrapy saw a free transfer slot and popped the next request for that domain. Two coroutines then slept at once. |
| Bound that sleep by the last real dispatch as well | Correct as far as it went, but it still relied on two independent timers agreeing, and it could not constrain the interval between the stamp and the socket write. |

`crawler/downloader.py` overrides `_download` to advance `slot.lastseen` at the
response's **completion** instead. Every step then points the same way:

```
pop(B)   >= complete(A) + delay      the override, via slot.lastseen
stamp(B) >= pop(B)                   the stamp is taken in that coroutine
wire(A)  <= complete(A)              a socket write precedes its response
-------------------------------------------------------------------------
wire(B)  >= wire(A) + delay
```

Reactor lateness appears only on the right-hand side of a `>=`, so it can only
widen the gap. `CONCURRENT_REQUESTS_PER_DOMAIN = 1` is the other half: B
cannot be popped while A is still transferring, so two requests for one domain
are never in flight together and there is no ordering between them to get
wrong.

This is what Heritrix calls [`min-delay-ms`](https://github.com/internetarchive/heritrix3/wiki/Politeness-parameters),
"a minimum delay from the last request completion", and what Nutch's
`FetchItemQueue` implements by recording the time the last request finished.
Both are reference implementations for a politeness-critical broad crawl.

The cost is accepted: a domain yields one page per (delay + round trip) rather
than one per delay, about 20% fewer pages from any single domain at a 1.5s
median round trip. It costs no throughput overall, because the bottleneck is
the reactor thread at 97% CPU rather than the supply of ready domains. The
same run held 5,000–10,000 live domains, which at 5.1s allows 1,568 pages/s
against the 28 per shard actually achieved.

`slot.lastseen` lives on the `Slot`, and `_slot_gc` destroys any slot idle for
60s, so a rebuilt slot would start with no history and dispatch immediately. A
two hour run produced exactly that, two requests to `panasonic.jp` 0.001s
apart. `DomainSlotDownloader` therefore also keeps the completion time in a
dict keyed by registered domain, outside the slot, and seeds a newly built
slot's `lastseen` from it. `tests/test_dispatch.py slot_gc` collects the slot
between every request and fails without this.

`tests/test_downloader.py` is what pins the semantics. It serves responses
that take 1.5s against a 2.0s delay, so a pop-anchored delay produces 2.0s
gaps and a completion-anchored one produces 3.5s. Measured: 3.514s, with
completion-to-dispatch at exactly 2.000s. It runs the same assertion against a
handler that fails, because a timeout must also cost the domain its delay.

### The robots.txt fetch

It shares the site's timer with no work at all, because the slot key comes
from the URL and `robots.txt` is on the same registered domain as the pages it
governs.

## Measuring compliance correctly

`DOWNLOAD_DELAY` spaces apart the moments requests **start**, not the moments
responses arrive. Three plausible measurement points are all wrong:

| Source | Why it is wrong |
|---|---|
| Response arrival time | Includes variable network latency, so gaps look shorter than they are. Produced false violations at ~4.9s. |
| Downloader middleware `process_request` | Runs at enqueue time. `Downloader.fetch` wraps `_enqueue_request` in the middleware chain, so every queued request gets the same timestamp. Produced false violations at 0.000s. |
| `recv - meta['download_latency']` | `download_latency` covers only the transfer, excluding DNS and TCP/TLS setup, so the derived start lands late. Produced a false violation at 4.675s. |

The correct point is `Downloader._download`, the first thing to run after the
slot releases a request and the last before the handler opens a socket.
`crawler/dispatch.py` stamps `meta['dispatch_time']` there, writes
`data/dispatched*.log.gz`, and refuses anything under the floor.

`tests/test_dispatch.py` locks this in against a local HTTP server: four
requests on one slot with a 2 second delay must dispatch 2 seconds apart, the
guard must stay silent, and the dispatch log must hold one line per request
all grouped under one key.

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

The first fix re-keyed the slot inside `RedirectMiddleware`, on the destination
URL. The re-run reported 0 violations.

The macOS runs never exposed this. They were too slow to accumulate enough
cross-domain redirects. The general lesson is that a single `DOWNLOAD_DELAY`
setting does not deliver politeness on its own: every internal path that
manufactures a new request has to be audited.

### The same bug came back through meta refresh, and the fix was in the wrong place

That audit missed a path, and patching one middleware is what let it.

A later 10 minute run reported six violations while the in-process trace, which
records the gap at the moment of dispatch, recorded nothing at all. Those two
results are only compatible in one way. The timer really did space every pair
five seconds apart. It was spacing the wrong groups.

The violating rows share a shape:

```
1789457772.002  https://www.chinadaily.com.cn/china/xismoments
1789457772.052  https://subsites.chinadaily.com.cn/...     gap 0.050s
```

Both are `chinadaily.com.cn`, which is what `verify_politeness.py` groups by.
But `subsites...` never appears in `discovered.log.gz`, so the spider never
yielded it. It arrived another way. Fetching the pages that did get discovered
shows how:

```
av.jpn.support.panasonic.com/...  200  <meta http-equiv="Refresh" content="0;url=https://panasonic.jp/...">
www.home-assistant.io/blog/...    200  <meta http-equiv="refresh" content="0; url=https://www.openhomefoundation.org/...">
```

Scrapy enables `MetaRefreshMiddleware` by default, and it shares
`_build_redirect_request` with the redirect middleware that had already been
fixed. The target inherited the source page's `download_slot`, so it queued on
the source domain's timer and ignored the target's.

It also inherited `dont_filter`, which the spider sets on requests it has
already checked against the Bloom filter. Meta refresh targets therefore
skipped the duplicate filter: that run fetched dozens of URLs twice.

**The lesson is about where a fix belongs, not about meta refresh.** Correcting
`download_slot` inside one middleware treats a symptom of an inherited
identity, and leaves every other path that copies meta still broken. There is
no list of such paths that stays complete. The fix that holds is to stop
storing the identity: `crawler/downloader.py` derives the slot key from the URL
in `get_slot_key`, which is the single point every download passes through.
Nothing can inherit a value that is never stored.

`config.toml` also sets `follow_meta_refresh = false`. With the downloader fix
in place a meta refresh would now be compliant, so this is only about not
fetching the same page twice.

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

The first fix enforced the floor inside `dispatch.py`, with each coroutine
claiming a turn and sleeping to it. That reached 0 violations over 26,801
requests on a single process, and it was still wrong. See the next section.

The general lesson is that a rate limit expressed as a scheduling delay is not
a rate limit. It becomes one only when it is anchored to an event the
scheduler cannot be late for.

### The two short gaps that finished the argument

A 20 minute run at `shards = 2` produced **2 violations in 191,882
dispatches**: `poki.com` at 0.000520s and `wordpress.com` at 3.791s. Both were
inside a single process, confirmed by grepping each domain in both dispatch
logs and by `handoff/foreign_dropped = 0`.

`data/violation-trace-0.log` named the cause in two numbers:

```
=== SHORT GAP 0.000520s slot='poki.com' applicable_floor=5.0
  prev  dispatched=1789530254.160805 claimed_turn=1789530254.160802
  curr  dispatched=1789530254.161325 claimed_turn=1789530247.970933
```

`curr` claimed a turn **6.19 seconds earlier** than `prev`, and dispatched
second. The mechanism:

1. Coroutine A claims turn T, B claims T+5. The reservation order is correct.
2. Each sleeps to its own turn. `asyncio.sleep` is a lower bound only, and two
   sleeps overrun by different amounts.
3. A's sleep overran by ~6s, so B woke first and dispatched at `254.160805`.
4. A then woke to find its own turn six seconds in the past, so its
   `remaining <= 0` check passed and it dispatched at `254.161325`.

Claiming turns in order does not make them dispatch in order. A second dict
holding the last *real* dispatch closed that specific hole, and it also
explains the earlier unexplained 2.604s gap, whose "reactor stall" account
could not shorten a stamp-to-stamp gap at all.

But the shape of the design was still two independent timers that had to
agree, and a sleeping coroutine still left `slot.transferring` empty so Scrapy
kept popping. The throttle was moved into `crawler/downloader.py` instead, and
`dispatch.py` kept only the measurement and a guard. See "The delay counts
from the previous response's completion" above.

### Why the evidence had to change first

None of this was visible at the time. `dispatched.log.gz` covered 191,882
requests; `crawled.log.gz` covered 70,484. The old compliance claim was
computed on a log missing **63%** of what the crawler actually sent, because
`crawled.log.gz` is written in `parse()` and so omits every failed request and
every robots.txt fetch. In one run 40,957 of 45,825 robots fetches timed out.
One of the two violations was invisible to that log, and `--source crawled`
still reports 0 on the same run.

Two changes made the evidence complete:

1. **`data/dispatched*.log.gz` records every dispatch**, robots.txt and
   failures included. `verify_politeness.py --source dispatched` is the
   default and the claim rests on it.
2. **The silent fallback is gone.** `parse()` used to write the receive time
   when a response carried no dispatch stamp. The receive time is always later
   than the real dispatch, so it always widens the measured gap: the one case
   that could hide a violation was the one case the fallback covered. A
   missing stamp now writes `NA`, and the verifier counts those and fails.

## Sharding: more cores without shared state

After the `pop()` fix the reactor thread was still at 99%, but the profile no
longer showed anything wasteful. `parse` and `_extract_links` together were
42%, and that is the work the crawl exists to do. Scrapy runs all of it on one
thread, so the only remaining lever is more processes.

### Why the partition is by domain

`crawler/slot.shard_of()` assigns each registered domain to exactly one
process. That single rule is what makes the whole thing safe: a domain's 5
second timer lives inside one process, so no shared clock, no lock and no
coordination is needed to enforce it. Two processes crawling one domain would
each hold their own timer and silently double the real rate against that site,
and nothing in either process could detect it.

The rule only holds if every path that can produce a request respects it:

| path | how it is bound to the owner |
|---|---|
| seeds | every process reads the seed file and keeps only its own |
| page links | `_maybe_follow` checks the owner before admitting |
| redirects | target checked in `SlotAwareRedirectMiddleware`, handed over |
| robots.txt | same registered domain as the page, so the same shard |
| meta refresh | off |

Redirects are the interesting case. Seeds and links are checked before a
request exists, but a redirect picks its destination after the fetch, so it is
the one path that can move a request onto a domain this process does not own.

The hash covers the whole key rather than its first 8 bytes. Domains share
prefixes heavily, so truncating skews the split: measured over 5,994 domains,
2.012x max/min at 8 shards against 1.124x for the full digest. Over the 2,229
domains a 10 minute run actually reached, the full digest gives 1.077x at 4
shards.

### Cross-shard URLs have to be handed over, not dropped

The obvious implementation drops a URL another shard owns. That is wrong, and
the reason is the throughput bound:

```
pages/sec <= active domains / delay
```

A broad crawl finds nearly every new domain through a cross-domain link, so a
process that drops them keeps only the domains its own seeds reached. N
processes would then crawl little more than one. The links have to reach
whoever owns them.

`crawler/handoff.py` is an append-only text file per (sender, recipient) pair.
That is enough because of what the traffic is: one-directional, unordered, and
survivable to lose, since a dropped URL is almost certainly linked from
another page too. Delivery guarantees would cost more than the thing they
protect.

Ordering is what removes the need for locking. Each sender appends to its own
file, so there is exactly one writer per file. Readers track a byte offset and
stop before any line that has no newline yet, so a reader arriving mid-write
sees the line on its next poll rather than half a URL.

Received URLs go through the same `_admit()` as local links, so they face the
same Bloom filter and the same per-domain limits. They are deliberately not
counted into `discovered_raw` again: the shard that found the link already
counted it, and recounting would inflate the Tier 1 metric once per process
boundary crossed.

### The shard count is set by memory, not by cores

The machine has 8 cores, but WSL2 is given 9.9 GB and one process measured
1,468 MB at 10 minutes. Every value in `[memory]` is therefore per process and
documented as such, sized for one shard's share of the work rather than the
whole crawl's.

### Measured: sharding did not raise throughput, because the bottleneck is the network

This is the result the work was for, and it is negative.

| | 1 process | 4 shards |
|---|---|---|
| pages/s | **54.54** | 52.29 |
| requests issued @570s | 94,305 | **162,721** |
| responses @570s | 44,986 | 46,144 |
| response rate | 48% | **28%** |
| established TCP | ~1,700 | ~1,600 |
| DNS names cached | 25,567 | ~24,700 |
| CPU | 99% on one thread | 23–41% each |

Four processes issued 1.7x the requests and got the same number of responses
back. The TCP connection count and the DNS cache are the same in both, which
is the tell: the crawl is limited by how many connections this link and
resolver will carry, not by how much CPU is available to start them. Adding
processes only added requests that time out.

**Read the pages/s column with care.** A later single-process run under
identical code and config reached 68.16 pages/s, 25% above the 54.54 baseline,
purely because only 390 of the 1,000 seed domains came up that time against
622 in the baseline. Fewer live seeds means fewer links, fewer domains, and a
budget spent going deeper into a narrow set: that run touched 1,031 domains
against 2,229. Ten minute samples vary by more than the differences being
measured here, so the 52.29-against-54.54 gap is inside the noise. The
conclusion above rests on the TCP and DNS columns, which are structural and
agree across every run, not on pages/s.

The CPU column is the confirmation. After sharding no process is near
saturation, so the reactor thread was never what capped the previous run
either. It looked like the cap because `pop()` really had been burning it, and
fixing that moved the limit somewhere else without anyone checking where.

The first sharded attempt was slower still, at 45.66 pages/s, because
`concurrent_requests` had been left at 3,000. That setting is a budget for
requests parked on a domain timer, so it bounds domains in flight; a shard
owning a quarter of the domains spends it stacking requests on the few it
holds. Raising it to 12,000 recovered 45.66 to 52.29, which is the sharding
overhead being paid back, not a gain.

**Sharding is now switched on, `sharding.shards = 2`.** The conclusion above
was drawn on the WSL host, whose Wi-Fi link the crawl had already filled to
72%: four processes were contending for a link that was already full. On
t2d-standard-2 the link is not the constraint, 12.0 Mbps of 1,570 available,
and two shards measured 53.09 pages/s against 25.51 for one process.

Compliance is structural rather than coordinated. A domain belongs to exactly
one shard, chosen by `shard_of`, so its 5 second timer never spans processes
and needs no shared state. `_drain_handoff` re-verifies ownership on every URL
it receives rather than trusting the sending shard, because a handoff inbox
left over from a run with a different shard count would otherwise redirect
work to the wrong process. See the short-gap note above for the residual risk
and the smoke run that tests for it.

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
crawler/downloader.py             keys every slot by eTLD+1 of the URL
crawler/handoff.py                passes URLs between sharded processes
crawler/pqueue.py                 O(1) next-domain selection
crawler/scheduler.py              frontier cap with O(1) size tracking
crawler/dupefilter.py             Bloom duplicate filter
crawler/middlewares/robots.py     robots.txt + Crawl-delay
crawler/middlewares/redirect.py   restores filtering across redirects
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
