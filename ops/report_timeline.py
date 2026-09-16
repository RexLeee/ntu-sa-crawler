#!/usr/bin/env python3
"""Turn runstats samples into an hourly timeline.

runstats*.tsv is sampled every 30s for the whole run and holds the numbers
that decide whether the design survives 48 hours: main-thread CPU, reactor
lag, the scraper queue, the per-domain queue count and the frontier size.
Nothing read it. report_growth.py fits RSS and domain curves from
resources.tsv and never opens this file at all.

A 48 hour run produces 5,760 samples per shard, which is too many to read and
too few to plot from memory. This buckets them by hour and reports the rate
within each bucket, so the report can state how throughput and memory moved
over the run rather than quoting one whole-run average.

Rates are computed from the difference between bucket ends, not from the
cumulative totals, because the cumulative columns only say where the run got
to, not how fast it was going at the time.

Usage:
    uv run python ops/report_timeline.py data/run-20260916-120000
    uv run python ops/report_timeline.py data/run-... --bucket 600
"""

from __future__ import annotations

import argparse
import glob
import sys
from pathlib import Path

HEADER = (
    "hour\tshard\tpages\tpages_per_sec\tdiscovered\tdiscovered_per_sec\t"
    "rss_mb\tmain_cpu_pct\tlag_p50_ms\tlag_max_ms\tscraper_queued\t"
    "frontier\tpqueues\tdl_slots\tdomains_per_sec"
)


def _read(path: Path) -> tuple[list[str], list[list[str]]]:
    lines = [ln.rstrip("\n") for ln in path.read_text(encoding="utf-8").splitlines()]
    if not lines:
        return [], []
    header = lines[0].split("\t")
    rows = [ln.split("\t") for ln in lines[1:] if ln.strip()]
    # A shard killed mid-write leaves a short final row. Drop it rather than
    # index past the end.
    rows = [r for r in rows if len(r) == len(header)]
    return header, rows


def _num(value: str, default: float = 0.0) -> float:
    try:
        return float(value)
    except (ValueError, TypeError):
        return default


def summarize(path: Path, bucket: int) -> list[tuple]:
    header, rows = _read(path)
    if not rows:
        return []
    idx = {name: i for i, name in enumerate(header)}
    required = ("elapsed", "crawled", "discovered")
    missing = [c for c in required if c not in idx]
    if missing:
        print(f"{path}: missing column(s) {missing}, skipped", file=sys.stderr)
        return []

    shard = path.stem.replace("runstats", "").lstrip("-") or "0"

    def col(row: list[str], name: str, default: float = 0.0) -> float:
        return _num(row[idx[name]], default) if name in idx else default

    out: list[tuple] = []
    # The last sample in each bucket is the bucket's end state. Rates come from
    # the difference between consecutive bucket ends.
    ends: dict[int, list[str]] = {}
    for row in rows:
        ends[int(col(row, "elapsed") // bucket)] = row

    previous = None
    for key in sorted(ends):
        row = ends[key]
        if previous is None:
            span = max(col(row, "elapsed"), 1.0)
            d_pages = col(row, "crawled")
            d_disc = col(row, "discovered")
            d_slots = col(row, "dl_slots")
        else:
            span = max(col(row, "elapsed") - col(previous, "elapsed"), 1.0)
            d_pages = col(row, "crawled") - col(previous, "crawled")
            d_disc = col(row, "discovered") - col(previous, "discovered")
            d_slots = col(row, "dl_slots") - col(previous, "dl_slots")
        out.append(
            (
                f"{col(row, 'elapsed') / 3600:.2f}",
                shard,
                int(col(row, "crawled")),
                f"{d_pages / span:.2f}",
                int(col(row, "discovered")),
                f"{d_disc / span:.1f}",
                int(col(row, "rss_mb")),
                f"{col(row, 'main_cpu_pct'):.0f}",
                int(col(row, "lag_p50_ms")),
                int(col(row, "lag_max_ms")),
                int(col(row, "scraper_queued")),
                int(col(row, "frontier")),
                int(col(row, "pqueues")),
                int(col(row, "dl_slots")),
                f"{d_slots / span:.2f}",
            )
        )
        previous = row
    return out


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("rundir", help="a data/run-* directory, or data/")
    ap.add_argument(
        "--bucket", type=int, default=3600, help="bucket width in seconds"
    )
    args = ap.parse_args()

    root = Path(args.rundir)
    if not root.exists():
        print(f"{root} does not exist", file=sys.stderr)
        return 2

    paths = [Path(p) for p in sorted(glob.glob(str(root / "runstats*.tsv")))]
    if not paths:
        print(f"no runstats*.tsv under {root}", file=sys.stderr)
        return 2

    print(HEADER)
    totals: dict[str, float] = {}
    for path in paths:
        for row in summarize(path, args.bucket):
            print("\t".join(str(v) for v in row))
            totals[row[1]] = max(totals.get(row[1], 0.0), float(row[2]))

    combined = sum(totals.values())
    print(f"# shards: {len(totals)}  combined pages at last sample: {combined:,.0f}",
          file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
