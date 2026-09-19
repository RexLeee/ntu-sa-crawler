"""Gzipped TSV logs for the two Tier 1 metrics.

crawled.log.gz   one line per fetched response
discovered.log.gz one line per URL first seen

Both are append-only and flushed periodically so a kill -9 loses at most the
last buffer, not the run.
"""

from __future__ import annotations

import gzip
import sys
import time
import zlib
from collections.abc import Iterator
from pathlib import Path
from typing import BinaryIO


class GzipLineWriter:
    def __init__(self, path: Path, flush_every: int = 2000):
        self._fh = gzip.open(path, "at", encoding="utf-8", newline="\n")
        self._flush_every = flush_every
        self._since_flush = 0
        self.count = 0

    def write(self, *fields: object) -> None:
        # Tabs and newlines inside a URL would corrupt the column layout.
        line = "\t".join(str(f).replace("\t", "%09").replace("\n", "%0A") for f in fields)
        self._fh.write(line + "\n")
        self.count += 1
        self._since_flush += 1
        if self._since_flush >= self._flush_every:
            self._fh.flush()
            self._since_flush = 0

    def close(self) -> None:
        try:
            self._fh.flush()
        finally:
            self._fh.close()


_GZIP_MAGIC = b"\x1f\x8b\x08"
_READ_CHUNK = 1 << 20


def _split_lines(buf: bytes) -> tuple[bytes, list[str]]:
    """Split on newlines, returning (incomplete tail, complete lines)."""
    if b"\n" not in buf:
        return buf, []
    head, _, tail = buf.rpartition(b"\n")
    lines = head.decode("utf-8", errors="replace").split("\n")
    return tail, [ln + "\n" for ln in lines]


def _find_next_member(raw: BinaryIO, pos: int, size: int) -> int:
    """Byte offset of the next gzip header after pos, or -1.

    Scans in windows rather than loading the file. A 1 GB log read whole cost
    3.4 GB of RSS once decompression buffers were added, and the OOM killer
    took the process on the 7.9 GB crawl host while two shards were running.
    """
    window = _READ_CHUNK
    start = pos + 3
    overlap = len(_GZIP_MAGIC) - 1
    while start < size:
        raw.seek(start)
        block = raw.read(window + overlap)
        if not block:
            return -1
        found = block.find(_GZIP_MAGIC)
        if found >= 0:
            return start + found
        start += window
    return -1


def _decode_bounded(blob: bytes) -> list[bytes]:
    """Decode one member's worth of bytes, keeping whatever survives."""
    decomp = zlib.decompressobj(31)
    chunks: list[bytes] = []
    try:
        pending = blob
        while pending and not decomp.eof:
            chunks.append(decomp.decompress(pending, _READ_CHUNK))
            pending = decomp.unconsumed_tail
    except (EOFError, zlib.error, gzip.BadGzipFile):
        pass
    return chunks


def read_lines(path: Path | str) -> Iterator[str]:
    """Yield log lines from every readable gzip member, skipping damage.

    The writer opens the file in append mode, so one file holds one member per
    process start. A kill damages only the member the dying process was
    writing; members before it are complete, and members a later process
    appends after it are complete too.

    Decoding member by member is what makes that recoverable. Handing the
    whole file to gzip.open stops at the first damaged member and yields
    nothing after it, which under-reports whenever a run was killed and then
    resumed. A measured case: the 48 hour run was SIGHUPed at 36.9 hours and
    resumed, and gzip.open returned 3,607,540 of 3,624,753 lines, losing every
    record the resumed process wrote.

    Partial data inside the damaged member is recovered as well. gzip.open
    discards it, so a truncated member's already-decoded records were lost
    even though they are real records that were flushed to disk.

    Three exceptions all mean "this member is not intact":

      EOFError          a member with no end-of-stream marker
      zlib.error        a corrupt deflate block, which is what a kill leaves
      gzip.BadGzipFile  a member header cut in half
    """
    path = Path(path)
    damaged = 0
    truncated_lines = 0
    size = path.stat().st_size

    with path.open("rb") as raw:
        pos = 0
        while pos < size:
            raw.seek(pos)
            decomp = zlib.decompressobj(31)  # 31 = gzip wrapper, not raw deflate
            carry = b""  # a record split across two read blocks
            produced = False
            failed = False
            while True:
                block = raw.read(_READ_CHUNK)
                if not block:
                    failed = not decomp.eof  # ran out with no end-of-stream mark
                    break
                try:
                    out = decomp.decompress(block)
                except (zlib.error, gzip.BadGzipFile):
                    failed = True
                    break
                if out:
                    produced = True
                    carry, whole = _split_lines(carry + out)
                    yield from whole
                if decomp.eof:
                    break

            if not failed:
                # unused_data is the decoder's own report of where this member
                # ended. Trust it rather than searching for the next header:
                # the three magic bytes occur by chance inside compressed data,
                # and an earlier version searched for them, cut intact members
                # in half, and read 425,483 of 3,607,540 lines from a 1 GB log.
                consumed = raw.tell() - pos - len(decomp.unused_data)
                if carry:
                    # An intact member ending without a newline is the live
                    # file being read mid-write. That record is real.
                    yield carry.decode("utf-8", errors="replace")
            else:
                damaged += 1
                # The decoder cannot say where a damaged member ends, so the
                # next header has to be found. A false positive costs nothing
                # here: this member is already unreadable.
                nxt = _find_next_member(raw, pos, size)
                consumed = (nxt - pos) if nxt > 0 else (size - pos)
                if not produced and nxt > 0:
                    # Unbounded, a truncated member reads the NEXT member's
                    # bytes as a corrupt continuation and delivers nothing.
                    # Bounded by the next header it fails cleanly instead,
                    # keeping the records it had already decoded.
                    raw.seek(pos)
                    for chunk in _decode_bounded(raw.read(nxt - pos)):
                        carry, whole = _split_lines(carry + chunk)
                        yield from whole
                # A fragment at a damaged member's end was cut mid-record.
                # Carrying it into the next member splices two records into a
                # line that never existed, such as "killed-47post-0".
                if carry:
                    truncated_lines += 1

            if consumed <= 0:
                break
            pos += consumed

    if truncated_lines:
        print(
            f"{path}: dropped {truncated_lines} line(s) cut in half by a kill",
            file=sys.stderr,
        )
    if damaged:
        print(
            f"{path}: {damaged} damaged gzip member(s) skipped; "
            "every intact member was read",
            file=sys.stderr,
        )


def now() -> float:
    return time.time()
