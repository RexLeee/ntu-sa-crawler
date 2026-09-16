"""A disk queue that survives a kill, and costs 0.6 KB instead of 9.1 KB.

Two defects in queuelib's FifoDiskQueue made a 48 hour run impossible on this
machine. Both are in the same class, so both are fixed in one subclass.

A kill makes the queue look empty
---------------------------------
`info.json` holds head, tail and size, and `_saveinfo` is called only from
`close()`. A SIGKILLed crawl therefore leaves chunk files full of real requests
and no metadata, so the next `_loadinfo` returns size 0. Worse,
ScrapyPriorityQueue.init_prios() closes any queue that reports empty, and
`close()` runs `_cleanup()` at size 0, which DELETES the chunks. Resuming
without this fix does not merely ignore the frontier; it destroys it.

A measured 2 hour run proved the cost: shard 0 was OOM-killed holding a 2.6 GB
frontier of 3,003,701 requests, and the process that replaced it crawled
nothing and exited in 1.66 seconds.

The fix is to reconstruct the metadata by scanning the chunks. Each record is
a 4 byte big-endian length followed by that many bytes, so a scan from the
last known tail to the end of the last chunk recovers head and size exactly.
`save_info()` checkpoints the metadata periodically, which bounds the scan.

Each queue wasted two 8 KB buffers
----------------------------------
FifoDiskQueue reads and writes through `os.read`/`os.write` on the raw file
descriptor. It never uses the buffer that `Path.open()` allocates, and it
holds two such files per queue, head and tail.

Measured on 5,000 pairs of file objects: 9.1 KB per pair buffered, 0.6 KB per
pair with buffering=0. An independent least-squares fit of RSS against the
per-domain queue count, over the 99 samples after the robots.txt cache
saturated, gave 9.1 KB per queue. The two agree, so the buffers were the
frontier's entire marginal memory cost.

At the 125,000 queues the run reached, that is 1.1 GB per shard of allocated
and unused buffer.
"""

from __future__ import annotations

import logging
import os
import pickle
import struct
from pathlib import Path
from typing import Any, BinaryIO, Literal

from queuelib.queue import FifoDiskQueue
from scrapy.squeues import (
    _pickle_serialize,
    _scrapy_serialization_queue,
    _serializable_queue,
    _with_mkdir,
)

logger = logging.getLogger(__name__)


class RecoverableFifoDiskQueue(FifoDiskQueue):
    """FifoDiskQueue that rebuilds its own metadata after an unclean kill."""

    def _openchunk(
        self, number: int, mode: Literal["rb", "ab+"] = "rb"
    ) -> BinaryIO:
        # buffering=0. Every read and write in this class goes through
        # os.read/os.write on the raw descriptor, so the buffer a buffered
        # object allocates is never touched. See the module docstring for the
        # measurement: 9.1 KB per queue against 0.6 KB.
        return Path(self.path, f"q{number:05d}").open(mode, buffering=0)  # type: ignore[return-value]

    def _loadinfo(self, chunksize: int) -> dict[str, Any]:
        info = super()._loadinfo(chunksize)
        # "clean" is written by close() only. Its absence means either that
        # this queue has never been closed, or that the process was killed
        # after the last checkpoint, and both need the same scan.
        if info.pop("clean", False):
            return info
        return self._recover(info)

    def _recover(self, info: dict[str, Any]) -> dict[str, Any]:
        """Rebuild head and size by scanning forward from the recorded tail.

        The tail is trusted: it only ever advances, and a checkpoint that
        recorded it means those records were already popped. Everything from
        there to the end of the last chunk is still pending, so counting it
        gives both the size and the head position.

        A record whose length header promises more bytes than the file holds
        was being written when the process died. It is truncated away, because
        pickle cannot deserialise a partial record and the alternative is a
        queue that raises on its last pop for the rest of the run.
        """
        chunksize = info["chunksize"]
        tnum, tcnt, toffset = info["tail"]

        chunks = sorted(int(p.name[1:]) for p in Path(self.path).glob("q[0-9]*"))
        if not chunks:
            # Nothing on disk. A fresh queue, or one whose chunks were removed.
            info["size"] = 0
            info["tail"] = [0, 0, 0]
            info["head"] = [0, 0]
            return info

        # A tail chunk that no longer exists means the recorded tail is older
        # than the files. Start from the oldest chunk that does exist.
        if tnum not in chunks:
            tnum = chunks[0]
            tcnt = toffset = 0

        pending = 0
        hnum, hpos = tnum, tcnt
        for num in [c for c in chunks if c >= tnum]:
            path = Path(self.path, f"q{num:05d}")
            start = toffset if num == tnum else 0
            count, good_end = self._scan_chunk(path, start)
            if good_end is not None:
                # A partial trailing record. Nothing after it can be read, so
                # this is the last chunk with any content.
                os.truncate(path, good_end)
                logger.warning(
                    "truncated a partial record at the end of %s", path
                )
                pending += count
                hnum, hpos = num, (tcnt + count) if num == tnum else count
                break
            pending += count
            hpos = (tcnt + count) if num == tnum else count
            hnum = num
            # push() rolls over to the next chunk once a chunk holds chunksize
            # records, so a full chunk means the head is at the start of the
            # next one.
            if hpos >= chunksize:
                hnum, hpos = num + 1, 0

        info["size"] = pending
        info["tail"] = [tnum, tcnt, toffset]
        info["head"] = [hnum, hpos]
        if pending:
            logger.info(
                "recovered %d pending request(s) in %s", pending, self.path
            )
        return info

    def _scan_chunk(self, path: Path, start: int) -> tuple[int, int | None]:
        """Count whole records in one chunk from byte *start*.

        Returns the count, and the offset to truncate to when the chunk ends
        with a partial record. The second value is None when the chunk is
        intact.
        """
        count = 0
        size = path.stat().st_size
        offset = start
        with path.open("rb", buffering=0) as fh:
            fh.seek(start)
            while True:
                if offset + self.szhdr_size > size:
                    # A stray fragment shorter than a length header.
                    return count, (offset if offset != size else None)
                szhdr = fh.read(self.szhdr_size)
                if len(szhdr) < self.szhdr_size:
                    return count, offset
                (length,) = struct.unpack(self.szhdr_format, szhdr)
                nxt = offset + self.szhdr_size + length
                if nxt > size:
                    return count, offset
                fh.seek(nxt)
                offset = nxt
                count += 1
                if offset == size:
                    return count, None

    def save_info(self) -> None:
        """Persist the metadata without closing the queue.

        This is what bounds the recovery scan. Without it a killed shard
        rescans every record ever pushed to each of its queues; with it the
        scan covers one checkpoint interval.
        """
        self._saveinfo(self.info)

    def close(self) -> None:
        # Mark the metadata as authoritative, so the next open trusts it and
        # skips the scan entirely.
        self.info["clean"] = True
        try:
            super().close()
        finally:
            self.info.pop("clean", None)


# The three wrappers are Scrapy's own, applied in the same order
# scrapy.squeues applies them to build PickleFifoDiskQueue: create the parent
# directory, serialise with pickle, then convert Requests to and from dicts.
# They are private names, and pinned deliberately: crawler/downloader.py
# already pins internals from the same version, and tests/test_downloader.py
# fails if Scrapy moves off 2.19.0.
RecoverablePickleFifoDiskQueue = _scrapy_serialization_queue(
    _serializable_queue(
        _with_mkdir(RecoverableFifoDiskQueue),
        _pickle_serialize,
        pickle.loads,
    )
)
