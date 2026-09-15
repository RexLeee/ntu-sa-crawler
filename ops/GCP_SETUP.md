# Running on GCP Compute Engine

The WSL host needs a desktop that stays awake for 48 hours. A VM does not.
The measured reason to move is narrower than that, though: the crawl on leepc
used 13.3 Mbps of an 18.6 Mbps Wi-Fi link, 72% of it, and main-thread CPU fell
from 99% to 48-58% as a result. The link was the ceiling, not the machine.

**Whether this is worth doing at all is an open question until the ten minute
test answers it.** Datacenter IP ranges are on many blocklists, and the crawl's
response rate on a residential link is already only 48%. See "The ten minute
test" below, which exists to settle that before any 48 hour commitment.

## Machine type: e2-medium

| | |
|---|---|
| Machine type | `e2-medium` |
| vCPU | 2 shared, each guaranteed 50% CPU time |
| RAM | 4 GB |
| Zone | `us-central1-a` |
| Image | `ubuntu-2404-lts-amd64` |
| Boot disk | 60 GB `pd-balanced` |
| Provisioning | standard, **not Spot** |

The two shared vCPUs each sustain 50% of CPU time, so the guaranteed floor is
one full core. The crawl is a single-threaded reactor that measured 99% of one
thread, so the guarantee matches the need exactly.

Larger machines buy nothing. The crawl is network-bound: four sharded
processes measured 52.29 pages/s against 54.54 for a single process, with
identical TCP connection and DNS cache counts. `sharding.shards` is 1 for that
reason and should stay 1.

`e2-small` costs $0.80 less over 48 hours but has 2 GB against a measured
1.5 GB working set. Half a gigabyte of headroom is not enough for a 48 hour
run whose RSS trend nobody has measured to the end.

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
  --machine-type=e2-medium \
  --zone=us-central1-a \
  --image-family=ubuntu-2404-lts-amd64 \
  --image-project=ubuntu-os-cloud \
  --boot-disk-size=60GB \
  --boot-disk-type=pd-balanced
```

60 GB is sized from measurements, not guessed:

| | |
|---|---|
| Frontier ceiling under `state/job` | 12.3 GB |
| Logs, gzipped, at 54 pages/s | 7.3 GB |
| `collect()` copy into `data/run-<stamp>/` | 7.3 GB |
| OS, uv, venv | 8 GB |
| **Total** | **35 GB** |

The log figures come from a real run: `discovered.log.gz` held 4,075,529 lines
in 43,961,228 bytes, so a discovered URL costs 10.79 bytes compressed and a
crawled page costs 31.87. The frontier ceiling is `frontier_max_size` times the
660 bytes per request that a measured run showed.

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
uv run python tests/test_dispatch.py
uv run python tests/test_handoff.py
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
| RSS | `resources.tsv` | Must stay well under the 2,200 MB warning |
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
| `config.toml` | `memusage_limit_mb` 7000 to 2800, warning 5500 to 2200 | 7000 exceeds the 4 GB of physical RAM, so the OOM killer would act first and the guard would never fire |

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
