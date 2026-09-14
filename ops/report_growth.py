#!/usr/bin/env python3
"""Analyse the growth curves from a long run's resources.tsv.

The question a long run answers is not "how many pages" but "does this hold
for 48 hours". Two curves decide that:

  memory   linear growth means the single-process design cannot finish;
           sub-linear means it can.
  domains  throughput is bounded by active_domains / delay, so the crawl
           rate cannot be extrapolated until this curve flattens.

Fits both against sqrt(t) and t, and reports which describes the data better.
"""

from __future__ import annotations

import argparse
import gzip
import math
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from crawler.slot import slot_key  # noqa: E402

HOURS_48 = 48 * 3600


def _read_samples(path: Path) -> list[dict[str, float]]:
    rows: list[dict[str, float]] = []
    with path.open(encoding="utf-8") as fh:
        header = fh.readline().rstrip("\n").split("\t")
        for line in fh:
            parts = line.rstrip("\n").split("\t")
            if len(parts) != len(header):
                continue
            try:
                rows.append({k: float(v) for k, v in zip(header, parts, strict=True)})
            except ValueError:
                continue
    return rows


def _fit_scale(xs: list[float], ys: list[float]) -> tuple[float, float]:
    """Least-squares fit of y = k*x through the origin, plus its R^2."""
    denom = sum(x * x for x in xs)
    if denom == 0:
        return 0.0, 0.0
    k = sum(x * y for x, y in zip(xs, ys, strict=True)) / denom
    mean = sum(ys) / len(ys)
    ss_tot = sum((y - mean) ** 2 for y in ys)
    ss_res = sum((y - k * x) ** 2 for x, y in zip(xs, ys, strict=True))
    r2 = 1.0 - ss_res / ss_tot if ss_tot > 0 else 0.0
    return k, r2


def _describe(name: str, ts: list[float], vs: list[float], unit: str) -> None:
    """Report whether a curve grows like t or like sqrt(t), and project 48h."""
    if len(ts) < 4:
        print(f"{name}: too few samples")
        return

    lin_k, lin_r2 = _fit_scale(ts, vs)
    sqrt_k, sqrt_r2 = _fit_scale([math.sqrt(t) for t in ts], vs)

    better = "linear" if lin_r2 >= sqrt_r2 else "sqrt"
    proj = lin_k * HOURS_48 if better == "linear" else sqrt_k * math.sqrt(HOURS_48)

    print(f"\n{name}")
    print(f"  final           : {vs[-1]:,.0f} {unit} at {ts[-1]:,.0f}s")
    print(f"  linear fit R^2  : {lin_r2:.4f}")
    print(f"  sqrt fit R^2    : {sqrt_r2:.4f}")
    print(f"  better model    : {better}")
    print(f"  48h projection  : {proj:,.0f} {unit}")

    # Compare the first and last thirds. A decelerating curve is the one that
    # survives 48 hours; a steady rate is the one that does not.
    third = len(ts) // 3
    early_span = ts[third] - ts[0]
    late_span = ts[-1] - ts[-third - 1]
    if early_span > 0 and late_span > 0:
        early_rate = (vs[third] - vs[0]) / early_span
        late_rate = (vs[-1] - vs[-third - 1]) / late_span
        print(f"  rate first 1/3  : {early_rate:.3f} {unit}/s")
        print(f"  rate last 1/3   : {late_rate:.3f} {unit}/s")
        if early_rate > 0:
            print(f"  deceleration    : {late_rate / early_rate:.2f}x")


def _domain_curve(crawled: Path, buckets: int = 40) -> tuple[list[float], list[float]]:
    """Cumulative distinct registered domains over time, from the crawl log."""
    stamps: list[tuple[float, str]] = []
    try:
        with gzip.open(crawled, "rt", encoding="utf-8") as fh:
            for line in fh:
                p = line.rstrip("\n").split("\t")
                if len(p) < 3:
                    continue
                try:
                    stamps.append((float(p[0]), slot_key(p[2])))
                except ValueError:
                    continue
    except EOFError:
        # The crawl is still writing, so the final gzip member has no
        # end-of-stream marker yet. Everything already decoded is still
        # valid, which is what matters for inspecting a run in progress.
        pass
    if not stamps:
        return [], []

    stamps.sort(key=lambda s: s[0])
    t0 = stamps[0][0]
    span = stamps[-1][0] - t0
    if span <= 0:
        return [], []

    step = span / buckets
    seen: set[str] = set()
    ts: list[float] = []
    vs: list[float] = []
    next_mark = step
    for ts_val, key in stamps:
        seen.add(key)
        if ts_val - t0 >= next_mark:
            ts.append(next_mark)
            vs.append(float(len(seen)))
            next_mark += step
    ts.append(span)
    vs.append(float(len(seen)))
    return ts, vs


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("rundir", help="a data/run-* directory")
    args = ap.parse_args()

    rundir = Path(args.rundir)
    res = rundir / "resources.tsv"
    if not res.exists():
        print(f"missing {res}", file=sys.stderr)
        return 2

    rows = _read_samples(res)
    if not rows:
        print("no samples", file=sys.stderr)
        return 2

    ts = [r["elapsed"] for r in rows]

    print("=== resource growth ===")
    print(f"samples         : {len(rows)}")
    print(f"duration        : {ts[-1]:,.0f}s")
    print(f"peak rss        : {max(r['rss_kb'] for r in rows) / 1024:,.0f} MB")
    print(f"peak fds        : {max(r['fds'] for r in rows):,.0f}")
    print(f"peak tcp est    : {max(r['tcp_est'] for r in rows):,.0f}")
    print(f"peak threads    : {max(r['threads'] for r in rows):,.0f}")

    _describe("memory (RSS MB)", ts, [r["rss_kb"] / 1024 for r in rows], "MB")

    crawled = rundir / "crawled.log.gz"
    if not crawled.exists():
        crawled = Path("data/crawled.log.gz")
    if crawled.exists():
        dts, dvs = _domain_curve(crawled)
        if dts:
            _describe("distinct domains", dts, dvs, "domains")
            # The politeness limit turns the domain count into a hard rate cap.
            print(f"  implied ceiling : {dvs[-1] / 5.0:,.1f} pages/s at 5s delay")

    print("\nNote: a linear memory fit means the single-process design cannot")
    print("run 48 hours. A sqrt fit means it can.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
