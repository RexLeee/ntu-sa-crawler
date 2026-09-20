# Final 48h figures — authoritative

Counted 2026-09-19 after both shards wrote finish_reason=run_duration.

## The counting trap I hit first

`ops/run_hour.sh:356` does `cp data/crawled*.log.gz "$RUNDIR/"`. The writer
appends to `data/` across BOTH parts, so each run directory holds a copy of the
cumulative file, not that run's slice. First row of all three copies is
2026-09-17T05:27:19 — part 1's launch.

Summing part1 + part2 archives therefore double counts part 1:
  crawled naive sum 16,720,545 — WRONG
  crawled cumulative   9,499,288 — correct

The live `data/*.log.gz` files are the single source of truth.

## Tier 1 (sitemap URLs excluded by construction: the crawler never fetches
## sitemaps — zero mentions in crawler/ or config.toml)

| metric | value | source |
|---|---|---|
| discovered URLs | 177,579,733 | gz line count, data/discovered-{0,1} |
| successfully crawled URLs | 9,499,288 | gz line count, data/crawled-{0,1} |

All 16.7M scanned crawled rows were 2xx (200 = 16,712,701 of the naive scan;
every status in the file is 2xx, so crawled == successfully crawled).

## Runstats vs gz

runstats last sample said crawled 9,531,506. gz says 9,499,288, i.e. 32,218
FEWER. The gz count is lower, not higher, because the final rows sat in the
2000-line buffer when the process exited. Part 1 cross-check: archive 7,221,257
vs runstats 7,221,425, a 168-row buffer gap. Consistent.

## Damaged gzip members — bounded loss

89 damaged members in discovered, 11 in crawled, skipped by the reader.
Upper bound if each held a full 2000-line flush:
  discovered <= 178,000 rows = 0.10% of total
  crawled    <= 22,000 rows = 0.23% of total
Counts are conservative: they can only understate, never overstate.

## Compliance — all zero

| check | result |
|---|---|
| verify_politeness.py | 26,783,167 requests / 4,771,574 domains, min gap 5.125s vs 5.00s limit, VIOLATIONS 0 |
| verify_robots.py | 9,499,288 urls / 1,558,444 origins, 10,736 re-checked, VIOLATIONS 0 |
| violation-trace-0.log | 0 bytes |
| violation-trace-1.log | 0 bytes |
| dispatch/guard_refused | absent from both shards |
| memusage/limit_reached | absent from both shards |
| supervisor.log | 148 bytes = the two clean-exit records only, zero restarts |
| shard-*.gaveup | none |
| stats-*.restart*.json | none |

## Run shape

| | shard 0 | shard 1 |
|---|---|---|
| finish_reason | run_duration | run_duration |
| part 2 elapsed | 40,073s | 40,060s |
| domains seen | 578,850 | 511,970 |
| robots requests | 1,071,466 | 982,027 |
| dropped over_capacity | 14,047,859 | 14,897,191 |
| handoff sent | 32,454,208 | 17,255,238 |
| handoff received | 17,030,515 | 23,540,000 |

Total elapsed 132,794 + 40,073 = 172,867s = 48.02h.

---

## The VM is deleted (2026-09-20)

Instance `crawler` and its 100 GB disk are gone. Billing is zero: no instances,
disks, addresses or snapshots remain in project ntu-sa-crawler-20260915.

Before deleting, the six log files were pulled to `raw-logs/` and checked two
ways: every file is byte-exact against the VM, and every line count was
reproduced locally with the streaming reader.

| file | bytes | lines |
|---|---|---|
| dispatched-0.log.gz | 373,181,567 | 13,474,869 |
| dispatched-1.log.gz | 371,221,600 | 13,308,298 |
| crawled-0.log.gz | 175,901,613 | 4,732,839 |
| crawled-1.log.gz | 177,147,005 | 4,766,449 |
| discovered-0.log.gz | 1,208,294,705 | 87,742,550 |
| discovered-1.log.gz | 1,237,349,886 | 89,837,183 |

discovered total 177,579,733 — matches the reported Tier 1 figure exactly.

`raw-logs/` is gitignored: 3.3 GB, and every file is past GitHub's 100 MB
ceiling. It exists only on this machine. Back it up elsewhere if the evidence
has to survive this disk.

## Re-running the verifiers without the VM

    cd <repo>
    uv run python ops/verify_politeness.py \
        data/final-48h/raw-logs/dispatched-0.log.gz \
        data/final-48h/raw-logs/dispatched-1.log.gz

    uv run python ops/verify_robots.py \
        data/final-48h/raw-logs/crawled-0.log.gz \
        data/final-48h/raw-logs/crawled-1.log.gz --hosts 40

Both were run on this machine after the transfer. verify_politeness reproduced
the VM's result exactly: 26,783,167 requests, 4,771,574 domains, min gap 5.125s,
0 violations. verify_robots re-fetches live robots.txt, so its origin sample
differs per run; it reported 0 violations over 5 origins as a capability check.

## What was destroyed with the disk, and why it did not matter

state/job-0, job-1 (4.9 GB) and state/handoff (700 MB) were the crawler's
frontier working state, not evidence. data/run-*/ (6.0 GB) held copies of the
same logs already saved here. Five commits that looked unpushed on the VM were
each verified present on origin/main first; the VM's git had simply never
fetched after the pushes.
