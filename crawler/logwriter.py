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
    with path.open("rb") as raw:
        data = raw.read()

    pos = 0
    size = len(data)
    while pos < size:
        decomp = zlib.decompressobj(31)  # 31 = gzip wrapper, not raw deflate
        chunks: list[bytes] = []
        try:
            pending = data[pos:]
            while True:
                chunks.append(decomp.decompress(pending, _READ_CHUNK))
                if decomp.eof:
                    break
                # unconsumed_tail is the input held back by max_length; feeding
                # it back is what advances the member. Passing b"" instead
                # stalls, and the member then looks truncated even when it is
                # intact.
                pending = decomp.unconsumed_tail
                if not pending:
                    # Input exhausted with no end-of-stream marker: the writer
                    # was killed partway through this member.
                    raise EOFError("member truncated")
            # unused_data is the decoder's own report of where this member
            # ended. Trust it rather than searching for the next header: the
            # three magic bytes occur by chance inside compressed data, and an
            # earlier version of this function searched for them and cut
            # intact members in half, reading 425,483 of 3,607,540 lines from
            # a real 1 GB log.
            consumed = size - pos - len(decomp.unused_data)
            intact = True
        except (EOFError, zlib.error, gzip.BadGzipFile):
            # Damaged: the decoder cannot say where this member ends, so the
            # next header has to be searched for. A false positive here costs
            # nothing extra, because the member is already unreadable.
            damaged += 1
            intact = False
            nxt = data.find(_GZIP_MAGIC, pos + 3)
            consumed = (nxt - pos) if nxt > 0 else (size - pos)
            if not chunks and nxt > 0:
                # Unbounded, a truncated member reads the NEXT member's bytes
                # as a corrupt continuation of itself and fails before
                # delivering anything. Bounded by the next header it fails
                # cleanly instead, keeping the records it had already decoded.
                chunks = _decode_bounded(data[pos:nxt])
        body = b"".join(chunks)

        text = body.decode("utf-8", errors="replace")
        lines = text.split("\n")
        # Each member is self-contained: the writer only ever appends whole
        # lines, so a member that ends mid-line was cut there by the kill.
        # Carrying that fragment into the next member splices two records into
        # one fake line. A measured case produced "killed-47post-0", a record
        # that never existed. Drop the fragment instead, and count it.
        remainder = lines.pop()
        if remainder:
            if intact:
                # An intact member ending without a newline is the live file
                # being read mid-write. The record is real and complete enough
                # for the tools, which parse by column.
                yield remainder
            else:
                truncated_lines += 1
        yield from (ln + "\n" for ln in lines)

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
