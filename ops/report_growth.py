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

    # Rates over the first and last thirds. These decide the verdict; the fits
    # only describe the shape of whatever growth is left.
    third = len(ts) // 3
    early_span = ts[third] - ts[0]
    late_span = ts[-1] - ts[-third - 1]
    early_rate = late_rate = None
    if early_span > 0 and late_span > 0:
        early_rate = (vs[third] - vs[0]) / early_span
        late_rate = (vs[-1] - vs[-third - 1]) / late_span

    print(f"\n{name}")
    print(f"  peak            : {max(vs):,.0f} {unit}")
    print(f"  final           : {vs[-1]:,.0f} {unit} at {ts[-1]:,.0f}s")
    print(f"  linear fit R^2  : {lin_r2:.4f}")
    print(f"  sqrt fit R^2    : {sqrt_r2:.4f}")

    if early_rate is not None:
        print(f"  rate first 1/3  : {early_rate:+.3f} {unit}/s")
        print(f"  rate last 1/3   : {late_rate:+.3f} {unit}/s")

    # A curve that has turned over is not a growth curve, and fitting a
    # through-origin model to it produces a projection with no meaning. Say so
    # rather than print a number the reader would trust.
    if late_rate is not None and late_rate <= 0:
        print("  verdict         : no longer growing; it peaked and is falling")
        print("  48h projection  : not applicable to a curve that has turned over")
        return

    better = "linear" if lin_r2 >= sqrt_r2 else "sqrt"
    proj = lin_k * HOURS_48 if better == "linear" else sqrt_k * math.sqrt(HOURS_48)
    print(f"  better model    : {better}")

    if max(lin_r2, sqrt_r2) < 0.5:
        # Neither shape describes the data, so the extrapolation is noise.
        print(f"  48h projection  : unreliable, best fit R^2 only {max(lin_r2, sqrt_r2):.2f}")
    else:
        print(f"  48h projection  : {proj:,.0f} {unit}")

    if early_rate is not None and early_rate > 0:
        print(f"  deceleration    : {late_rate / early_rate:.2f}x")


def _domain_curve(
    crawled: list[Path], buckets: int = 40
) -> tuple[list[float], list[float]]:
    """Cumulative distinct registered domains over time, from the crawl logs.

    Takes every shard's log. Each domain belongs to exactly one shard, so the
    union is the crawl's real domain count and no deduplication across files
    is needed beyond the set already used here.
    """
    stamps: list[tuple[float, str]] = []
    for path in crawled:
        _read_stamps(path, stamps)

    stamps.sort()
    return _bucket(stamps, buckets)


def _read_stamps(crawled: Path, stamps: list[tuple[float, str]]) -> None:
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


def _bucket(
    stamps: list[tuple[float, str]], buckets: int
) -> tuple[list[float], list[float]]:
    if not stamps:
        return [], []

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

    # Every shard writes its own log, and each domain belongs to one shard.
    crawled = sorted(rundir.glob("crawled*.log.gz"))
    if not crawled:
        crawled = sorted(Path("data").glob("crawled*.log.gz"))
    if crawled:
        dts, dvs = _domain_curve(crawled)
        if dts:
            _describe("distinct domains", dts, dvs, "domains")
            # The politeness limit turns the domain count into a hard rate cap.
            print(f"  implied ceiling : {dvs[-1] / 5.0:,.1f} pages/s at 5s delay")

    print("\nRead the last-third rate first, not the fit. Linear growth that")
    print("does not decelerate means the design cannot last 48 hours; a rate")
    print("at or below zero means memory has found its working set.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
