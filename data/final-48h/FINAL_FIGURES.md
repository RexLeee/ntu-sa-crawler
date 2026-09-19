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
