# Running on GCP Compute Engine

The WSL host needs a desktop that stays awake for 48 hours. A VM does not.
The measured reason to move is narrower than that, though: the crawl on leepc
used 13.3 Mbps of an 18.6 Mbps Wi-Fi link, 72% of it, and main-thread CPU fell
from 99% to 48-58% as a result. The link was the ceiling, not the machine.

The ten minute test settled the open question this document used to lead with.
Datacenter IP ranges are on many blocklists and the residential response rate
was only 48%, so migrating might have made things worse. Measured on t2d it
did not: **79.6% response rate at two shards**, against the 30% floor set as
the go/no-go criterion. The migration is decided.

## Machine type: t2d-standard-2

| | |
|---|---|
| Machine type | `t2d-standard-2` |
| vCPU | 2 dedicated |
| RAM | 7.9 GB usable |
| Zone | `us-central1-a` |
| Image | `ubuntu-2404-lts-amd64` |
| Boot disk | 60 GB `pd-balanced` |
| Provisioning | standard, **not Spot** |

**Not `e2-medium`, which this document specified until the measurements came
in.** Two reasons, and the second is disqualifying.

The e2 vCPUs are shared and guarantee 50% of CPU time each. That matched the
old belief that the crawl was network-bound and could not use more than one
core. On t2d the link is not the constraint: 12.0 Mbps used against 1,570
available, 0.8%. The reactor is, and two dedicated cores let two shards run at
53.09 pages/s against 25.51 for one, a 2.08x gain.

The disqualifying reason is memory. `memory.memusage_limit_mb` is 3000 **per
process**, so two shards need 6 GB before the guard fires. On a 4 GB e2-medium
the guard could never fire and the OOM killer would arrive first, which is the
exact failure `config.toml` sizes that limit to avoid: a SIGKILL loses the log
flush and the persisted dupefilter.

**Do not use a Spot instance.** Spot VMs terminate at 24 hours whatever
happens, so a 48 hour run is guaranteed to be cut in half. The saving is
$0.64.

### Cost

| | |
|---|---|
| Ten minute test | under $0.01 |
| 48 hour run | about $2.24 |

That is $1.61 of instance time, $0.39 of disk and $0.24 of external IP.
Ingress is free, which matters because a crawler is almost entirely ingress.
E2 machines do not qualify for sustained use discounts, so 48 hours is simply
the hourly rate times 48.

Confirm against the billing page before the long run. Third-party sources
disagree about the hourly rate, though even the highest quote puts 48 hours
at $5.40.

## Check the billing project first

`gcloud` may be pointed at a work project. This is coursework and belongs on a
personal one.

```bash
gcloud config get-value project    # what would be billed right now
gcloud projects list               # what is available
gcloud config set project <personal-project-id>
```

Do not run `instances create` until `get-value project` returns the personal
project.

## Provisioning

```bash
gcloud compute instances create crawler \
  --machine-type=t2d-standard-2 \
  --zone=us-central1-a \
  --image-family=ubuntu-2404-lts-amd64 \
  --image-project=ubuntu-os-cloud \
  --boot-disk-size=60GB \
  --boot-disk-type=pd-balanced
```

60 GB is sized from measurements, not guessed. **Every per-process figure is
counted twice, because `sharding.shards` is 2 and each shard has its own
JOBDIR.** Counting the frontier once was an error that hid 13 GB.

| | |
|---|---|
| Frontier ceiling, 2 x `state/job-N` | 13.2 GB |
| Logs, gzipped, at 53 pages/s | 7.3 GB |
| `dispatched.log.gz`, all requests | 1.0 GB |
| `collect()` copy into `data/run-<stamp>/` | 8.3 GB |
| OS, uv, venv | 8 GB |
| **Total** | **37.8 GB** |

The log figures come from a real run: `discovered.log.gz` held 4,075,529 lines
in 43,961,228 bytes, so a discovered URL costs 10.79 bytes compressed and a
crawled page costs 31.87. The frontier ceiling is `frontier_max_size` (10M per
process, halved from 20M for exactly this reason) times the 660 bytes per
request a measured run showed, times two shards.

`ops/run_hour.sh` samples free disk every 30s into `resources.tsv` and stops
the run below `MIN_DISK_FREE_MB` (3000). A full disk fails the gzip writers and
the JOBDIR write path at the same moment, which kills both shards at once.

The default 10 GB boot disk would fill during the run. Disk is $0.0027 per GB
over 48 hours, so the headroom is nearly free.

## Firewall

Nothing inbound is needed except SSH, which the default rules already allow.
Do not open anything else. Outbound is unrestricted by default, which is what
the crawl needs.

## Deploying

A fresh Ubuntu image has neither `uv` nor a suitable Python.

```bash
gcloud compute ssh crawler --zone=us-central1-a

sudo apt update && sudo apt install -y git
curl -LsSf https://astral.sh/uv/install.sh | sh
export PATH="$HOME/.local/bin:$PATH"

git clone https://github.com/RexLeee/ntu-sa-crawler.git
cd ntu-sa-crawler && uv sync
```

`uv` installs to `~/.local/bin`, which a non-interactive SSH session does not
have on its PATH. `ops/run_hour.sh` exports it itself, but any manual
`uv run ...` over `ssh --command` needs the export first.

## One-time warm-up

`tldextract` fetches the Public Suffix List on first use, and `crawler/slot.py`
calls it from the reactor thread on the first dispatch. That is a blocking HTTP
request inside the event loop, and it falls back silently to a bundled snapshot
if it fails. The snapshot and the live list can disagree about private
suffixes, and `slot_key()` is the unit the rate limit is enforced on, so the
disagreement would change which hosts share a five second timer.

Populate the cache once, before the first crawl:

```bash
cd ~/ntu-sa-crawler
export PATH="$HOME/.local/bin:$PATH"
export TLDEXTRACT_CACHE="$PWD/state/tldextract"
uv run python -c "from crawler.slot import slot_key; print(slot_key('https://foo.blogspot.com/x'))"
```

It must print `foo.blogspot.com`. If it prints `blogspot.com`, the private
section of the list did not load and the cache is not usable: every
`*.blogspot.com` host would share one timer instead of getting its own.

`run_hour.sh` sets the same `TLDEXTRACT_CACHE` default, and its startup cleanup
removes only `state/job*` and `state/handoff`, so the cache survives across
runs.

## File descriptor limits

Each (domain, priority) pair in the frontier is a queue holding two open files,
so descriptors track the frontier's shape rather than the number of live
connections. A measured run reached 7,569 queues and 15,422 descriptors.

`run_hour.sh` now raises the soft limit to whatever the hard limit is, rather
than to a fixed 1,000,000. Check what the host allows:

```bash
ulimit -Hn
```

GCP's Ubuntu images normally give 1,048,576. If it comes back below about
65,536, add to `/etc/security/limits.conf` and reconnect:

```
*  soft  nofile  1048576
*  hard  nofile  1048576
```

Every run echoes its effective `fd limit` at startup, so the value is on the
record without extra checking.

## Smoke test before the real run

Prove the new memory ceiling does not fire spuriously and that the suffix cache
is live, before committing hours:

```bash
cd ~/ntu-sa-crawler
export PATH="$HOME/.local/bin:$PATH"
for t in tests/test_*.py; do uv run python "$t" || break; done
```

## The ten minute test

This is the test the migration hinges on. Cost is not the question; it is under
one US cent.

```bash
tmux new -d -s crawl "export PATH=\$HOME/.local/bin:\$PATH; ops/run_hour.sh 600 > /tmp/run.log 2>&1"
tmux attach -t crawl      # detach with ctrl-b d
```

Then:

```bash
uv run python ops/verify_politeness.py    # must print VIOLATIONS : 0
uv run python ops/verify_robots.py        # must print VIOLATIONS : 0
uv run python ops/report_metrics.py
wc -c data/violation-trace.log            # must be 0
tail -1 data/runstats.tsv                 # main_cpu_pct, requests, responses
```

| What to read | Where | What it has to say |
|---|---|---|
| **Response rate** | `responses / requests` in `runstats.tsv` | **Below 30% means do not migrate** |
| Main thread CPU | `main_cpu_pct` | Near 99% means the link is no longer the ceiling |
| RSS | `resources.tsv` | Per shard under the 2,500 MB warning, and the total well under the machine's 7.9 GB |
| Compliance | both verifiers | 0 and 0 |

Response rate is the decision. Published figures put datacenter IP success at
20-40% against 85-99% for residential addresses, and the crawl already sits at
48% on a home link. Bandwidth is worthless if the doors do not open.

The other numbers are context. Run-to-run variance in pages/s on this project
has reached 25%, because each run reaches a different number of seed domains,
so pages/s alone does not settle anything over ten minutes.

## Long runs

Use `tmux` so a dropped SSH session does not take the crawl with it. The run
writes continuously, so nothing is lost by disconnecting.

### The 20 minute smoke, before any 48 hour commitment

Twenty minutes is chosen, not rounded: at ~53 pages/s two shards send roughly
60,000 requests, and the one short gap ever recorded appeared at a rate of
1 in 32,000. A shorter run cannot see it.

```bash
tmux new -d -s smoke "export PATH=\$HOME/.local/bin:\$PATH; \
  cd ~/ntu-sa-crawler && ops/run_hour.sh 1200 > /tmp/smoke.log 2>&1"
tmux attach -t smoke        # detach with ctrl-b d
```

Every one of these must pass. Any failure means do not start the 48 hour run.

```bash
cd ~/ntu-sa-crawler && export PATH=$HOME/.local/bin:$PATH
uv run python ops/verify_politeness.py                      # VIOLATIONS : 0
uv run python ops/verify_politeness.py --source crawled     # VIOLATIONS : 0
uv run python ops/verify_robots.py --hosts 100              # VIOLATIONS : 0
wc -c data/violation-trace-*.log                            # 0 bytes each
grep -ho 'guard_refused": [0-9]*' data/stats-*.json         # absent, or 0
grep -ho 'finish_reason": "[^"]*"' data/stats-*.json        # run_duration
ls state/job-*/requests.bloom                               # one per shard
cat data/run-*/supervisor.log                               # empty
tail -3 data/run-*/resources.tsv                            # rss per shard, disk free
```

`dispatch/guard_refused` and the trace are the same signal. The guard in
`crawler/dispatch.py` refuses any dispatch under 5.0s from the previous one on
that domain, and the throttle in `crawler/downloader.py` counts from the
previous response's completion, so the guard cannot fire by design. An entry
in either place is a defect to investigate before the 48 hour run starts.

Do not grep for "Dumping Scrapy stats". That line is INFO and `log_level` is
WARNING, so it never appears however cleanly the crawl closed. `finish_reason`
in `data/stats-N.json` is the real marker.

The default `--source dispatched` is the one that matters. It reads
`data/dispatched*.log.gz`, which holds every request the crawler sent,
including robots.txt and the ones that failed. `--source crawled` reads the
older log, which holds parseable responses only.

### The 48 hour run

```bash
tmux new -d -s crawl48 "export PATH=\$HOME/.local/bin:\$PATH; \
  cd ~/ntu-sa-crawler && ops/run_hour.sh 172800 > /tmp/crawl48.log 2>&1"
```

Check once a day:

```bash
tail -2 data/run-*/resources.tsv       # RSS per shard, free disk
cat data/run-*/supervisor.log          # restarts; should stay empty
wc -c data/violation-trace-*.log       # must stay 0
tail -5 /tmp/crawl48.log
```

If the supervisor itself dies but the machine is fine, restart without losing
the frontier:

```bash
RESUME=1 ops/run_hour.sh <remaining_seconds>
```

## Watching from outside

```bash
gcloud compute ssh crawler --zone=us-central1-a \
  --command='tail -3 ~/ntu-sa-crawler/data/run-*/resources.tsv'
```

Cloud Monitoring's agent is neither installed nor needed. `run_hour.sh` already
samples RSS, threads, descriptors and TCP connections every 30 seconds into the
run directory.

## Retrieving results, then deleting

Copy the results back **before** deleting anything:

```bash
gcloud compute scp --recurse \
  crawler:~/ntu-sa-crawler/data/run-* ./ --zone=us-central1-a
```

Then delete the whole instance:

```bash
gcloud compute instances delete crawler --zone=us-central1-a
```

`stop` is not enough. A stopped instance still bills for its disk. Deleting
releases the disk and the ephemeral IP together.

## What changed for GCP

| Where | Change | Why |
|---|---|---|
| `ops/run_hour.sh` | `ulimit -n` takes `ulimit -Hn` instead of 1,000,000 | A bare `ulimit` to a value above the host's ceiling returns non-zero and `set -e` kills the run before the first page |
| `ops/run_hour.sh` | Exports `TLDEXTRACT_CACHE` under `state/` | Keeps domain keying identical across runs; see the warm-up section |
| `config.toml` | `memusage_limit_mb` 7000 to 3000, warning 5500 to 2500 | The value is per process. At `shards = 2` on a 7.9 GB machine anything above 3000 puts the combined limit over physical RAM, so the OOM killer would act first and the guard would never fire |
| `config.toml` | `shards` 1 to 2 | The link is 0.8% used on this machine, so the condition the old note set for sharding is met. See "Why two shards" below |
| `crawler/dupefilter.py` | `url_seen()` no longer calls `canonicalize_url` | Its caller passes a `UrlFilter.normalize()` result, which is already canonical. The second call cost 10.77 us of the method's 12.79 us and could not change the value |
| `ops/run_hour.sh` | Shutdown grace 20s to 90s | Every run so far was SIGKILLed before Scrapy wrote `Dumping Scrapy stats`, which is the only source of `exception_type_count` |

## Why two shards

The migration notes above argued that larger machines buy nothing because the
crawl is network-bound. That was measured on the WSL host, whose Wi-Fi link the
crawl had saturated to 72%. It does not hold here.

| | Measured on t2d-standard-2 |
|---|---|
| Link used | 12.0 Mbps of 1,570 Mbps, **0.8%** |
| `proc_cpu_pct` | 100.5%, where two dedicated cores allow 200% |
| `main_cpu_pct` | 99.5% of that |
| Reactor lag, median | 2,631 ms against a 5.0s per-domain gap |
| `dl_slots` at 604s | 8,783 domains, a ceiling of 1,757 pages/s |
| Actual | 25.52 pages/s, **1.5% of that ceiling** |

The last two rows are the ones that settle it. Domain supply is not short: the
reactor cannot dispatch what is already available. Scrapy's own optimization
guide says splitting into processes is the only way to use a second core.

`concurrent_requests` stays at 3000 despite the note in `config.toml` saying to
raise it when sharding. That note describes a shard starved of domains, which
is not this machine: `dl_active` last touched the 3000 ceiling at 31 seconds
and sat below it for the rest of the run.

`bloom_capacity` and `frontier_max_size` were deliberately left alone. The
bloom filter's 171 MB is a fixed allocation and shrinking it would raise the
false positive rate, and a false positive is a URL never fetched. The frontier
lives on disk, so its size is answered by the 60 GB boot disk rather than by a
config change.

## Host facts

Fill in after the first boot. This is the record of what the run actually ran
on.

| | |
|---|---|
| Machine type | |
| vCPU / RAM | |
| Disk free | |
| OS / kernel | |
| Python | |
| `ulimit -Hn` | |
