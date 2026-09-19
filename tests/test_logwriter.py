#!/usr/bin/env python3
"""Reproduce the reader stopping at a mid-file broken gzip member.

Shape of the real file after the 36.9h interruption and resume:

  member 0..N-1   complete, written before the kill
  member N        truncated mid-deflate-block by SIGHUP
  member N+1      complete, appended by the resumed process

read_lines must yield the lines from every complete member, including the
ones after the damaged member. Today it stops at the damage.
"""

from __future__ import annotations

import gzip
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))


def build_fixture(path: Path) -> tuple[int, int]:
    """Write good + truncated + good. Returns (recoverable, total_written)."""
    # member 0: 100 clean lines
    with gzip.open(path, "wt", encoding="utf-8", newline="\n") as fh:
        for i in range(100):
            fh.write(f"pre-{i}\n")

    # member 1: 500 lines, then chop it mid-block to imitate SIGHUP
    partial = path.with_suffix(".partial")
    with gzip.open(partial, "wt", encoding="utf-8", newline="\n") as fh:
        for i in range(500):
            fh.write(f"killed-{i}\n")
    blob = partial.read_bytes()
    partial.unlink()
    # Drop the last 60 bytes: past the header, inside the deflate stream, so
    # the member has real data but no end-of-stream marker.
    with path.open("ab") as out:
        out.write(blob[:-60])

    # member 2: 200 clean lines appended by the "resumed" writer
    with gzip.open(path, "at", encoding="utf-8", newline="\n") as fh:
        for i in range(200):
            fh.write(f"post-{i}\n")

    return 100 + 200, 800


def test_clean_files_are_unchanged() -> bool:
    """An undamaged file must read exactly as gzip.open would.

    This is the regression guard. Reading member by member is only worth
    doing if it changes nothing for the normal case, which is every file the
    writer closed cleanly.
    """
    import random

    from crawler.logwriter import read_lines

    random.seed(7)
    ok = True

    # Three append sessions, all closed cleanly: three intact members.
    multi = Path("/tmp/test_logwriter_clean_multi.log.gz")
    if multi.exists():
        multi.unlink()
    expected: list[str] = []
    for session in range(3):
        with gzip.open(multi, "at", encoding="utf-8", newline="\n") as fh:
            for i in range(1000):
                line = f"s{session}\tline{i}\t{random.random()}"
                fh.write(line + "\n")
                expected.append(line + "\n")
    got = list(read_lines(multi))
    if got != expected:
        print(f"FAIL: 3-member clean file differs ({len(got)} vs {len(expected)})")
        ok = False
    else:
        print(f"PASS: 3-member clean file identical ({len(got)} lines)")

    # The common case: one member, written once.
    single = Path("/tmp/test_logwriter_clean_single.log.gz")
    if single.exists():
        single.unlink()
    exp2 = []
    with gzip.open(single, "wt", encoding="utf-8", newline="\n") as fh:
        for i in range(5000):
            line = f"only\t{i}"
            fh.write(line + "\n")
            exp2.append(line + "\n")
    got2 = list(read_lines(single))
    if got2 != exp2:
        print(f"FAIL: single-member clean file differs ({len(got2)} vs {len(exp2)})")
        ok = False
    else:
        print(f"PASS: single-member clean file identical ({len(got2)} lines)")

    return ok


def main() -> int:
    from crawler.logwriter import read_lines

    fixture = Path(sys.argv[1] if len(sys.argv) > 1 else "/tmp/test_logwriter_fixture.log.gz")
    if fixture.exists():
        fixture.unlink()
    at_least, _ = build_fixture(fixture)

    lines = list(read_lines(fixture))
    pre = sum(1 for ln in lines if ln.startswith("pre-"))
    killed = sum(1 for ln in lines if ln.startswith("killed-"))
    post = sum(1 for ln in lines if ln.startswith("post-"))

    print(f"pre    (complete member before damage) : {pre} / 100")
    print(f"killed (truncated member, partial ok)  : {killed} / 500")
    print(f"post   (complete member after damage)  : {post} / 200")
    print(f"total yielded                          : {len(lines)}")

    # No line may mix two members: that would be a fabricated record.
    spliced = [
        ln for ln in lines
        if sum(ln.count(p) for p in ("pre-", "killed-", "post-")) > 1
    ]
    malformed = [
        ln for ln in lines
        if not (ln.startswith(("pre-", "killed-", "post-")))
    ]

    ok = True
    if pre != 100:
        print(f"FAIL: lost lines before the damage ({pre}/100)")
        ok = False
    if post != 200:
        print(f"FAIL: lost {200 - post} line(s) appended after the damage")
        ok = False
    if spliced:
        print(f"FAIL: {len(spliced)} spliced line(s), e.g. {spliced[0]!r}")
        ok = False
    if malformed:
        print(f"FAIL: {len(malformed)} malformed line(s), e.g. {malformed[0]!r}")
        ok = False
    if killed == 0:
        print("FAIL: recovered nothing from the truncated member")
        ok = False
    elif killed < 400:
        print(f"WARN: only {killed}/500 recovered from the truncated member")
    if ok:
        print(f"PASS: 100 before + {killed} partial + 200 after, no spliced lines")

    if not test_clean_files_are_unchanged():
        ok = False
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
