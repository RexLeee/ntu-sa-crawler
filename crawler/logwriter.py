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


def read_lines(path: Path | str) -> Iterator[str]:
    """Yield log lines, tolerating a file the writer never closed.

    The writer flushes periodically so a killed run keeps its records, but the
    final gzip member then has no end-of-stream marker and the stdlib raises
    EOFError at the end. Every line decoded before that point is a real record,
    so the analysis tools must report on them rather than refuse the file.
    """
    try:
        with gzip.open(path, "rt", encoding="utf-8") as fh:
            yield from fh
    except EOFError:
        print(f"{path}: truncated, using the decoded prefix", file=sys.stderr)


def now() -> float:
    return time.time()
