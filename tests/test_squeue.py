#!/usr/bin/env python3
"""Lock in the two properties a frontier queue needs to survive 48 hours.

A killed shard must be able to read its own frontier, and a queue must not
cost 9 KB of buffer it never uses. Both were real defects: a measured 2 hour
run OOM-killed a shard holding 3,003,701 queued requests, and the process that
replaced it read an empty frontier and exited in 1.66 seconds.

Run: uv run python tests/test_squeue.py
"""

from __future__ import annotations

import io
import json
import os
import shutil
import struct
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from crawler.squeue import RecoverableFifoDiskQueue  # noqa: E402

FAILED: list[str] = []


def check(label: str, ok: bool, detail: str = "") -> None:
    # The detail explains a failure, so it is only printed on one. Printing it
    # on a pass produced lines like "PASS  length recovered: 230 != 230".
    print(f"PASS  {label}" if ok else f"FAIL  {label}: {detail}")
    if not ok:
        FAILED.append(label)


def test_no_buffers_are_allocated() -> None:
    """queuelib reads and writes the raw fd, so a buffer is pure waste.

    Measured 9.1 KB per queue with buffered objects against 0.6 KB without,
    and the frontier reached 132,779 queues in one process.
    """
    d = tempfile.mkdtemp()
    try:
        q = RecoverableFifoDiskQueue(os.path.join(d, "q"))
        check(
            "head and tail are unbuffered FileIO",
            isinstance(q.headf, io.FileIO) and isinstance(q.tailf, io.FileIO),
            f"{type(q.headf).__name__}/{type(q.tailf).__name__}",
        )
        q.close()
    finally:
        shutil.rmtree(d, ignore_errors=True)


def test_a_checkpoint_survives_a_kill() -> None:
    """save_info() then no close(): the queue reopens with the right length."""
    d = tempfile.mkdtemp()
    path = os.path.join(d, "q")
    try:
        q = RecoverableFifoDiskQueue(path)
        for i in range(300):
            q.push(f"item-{i:04d}".encode())
        for _ in range(120):
            q.pop()
        q.save_info()
        for i in range(300, 350):
            q.push(f"item-{i:04d}".encode())
        del q  # a kill: nothing else is written

        q2 = RecoverableFifoDiskQueue(path)
        check("length recovered after a kill", len(q2) == 230, f"{len(q2)} != 230")
        check(
            "recovery resumes at the first unpopped record",
            q2.pop() == b"item-0120",
        )
        drained = []
        while (v := q2.pop()) is not None:
            drained.append(v)
        check(
            "every pushed record is recovered in order",
            drained == [f"item-{i:04d}".encode() for i in range(121, 350)],
            f"{len(drained)} records, last {drained[-1] if drained else None!r}",
        )
        q2.close()
    finally:
        shutil.rmtree(d, ignore_errors=True)


def test_a_queue_with_no_metadata_replays_everything() -> None:
    """No info.json at all must mean "replay", never "empty".

    This is the case that destroyed a frontier. An empty-looking queue gets
    closed by ScrapyPriorityQueue, and queuelib's close() deletes the chunks of
    an empty queue, so reporting 0 here loses the data permanently.
    """
    d = tempfile.mkdtemp()
    path = os.path.join(d, "q")
    try:
        q = RecoverableFifoDiskQueue(path)
        for i in range(350):
            q.push(f"x-{i:04d}".encode())
        for _ in range(100):
            q.pop()
        q.save_info()
        os.unlink(os.path.join(path, "info.json"))
        del q

        q2 = RecoverableFifoDiskQueue(path)
        check(
            "a queue with no metadata replays from the start",
            len(q2) == 350,
            f"{len(q2)} != 350",
        )
        check("the replay starts at the first record", q2.pop() == b"x-0000")
        q2.close()
    finally:
        shutil.rmtree(d, ignore_errors=True)


def test_a_partial_record_is_truncated() -> None:
    """A kill mid-write leaves a record shorter than its length header."""
    d = tempfile.mkdtemp()
    path = os.path.join(d, "q")
    try:
        q = RecoverableFifoDiskQueue(path)
        for i in range(10):
            q.push(f"y-{i}".encode())
        del q
        chunk = Path(path, "q00000")
        before = chunk.stat().st_size
        os.truncate(chunk, before - 3)

        q2 = RecoverableFifoDiskQueue(path)
        check(
            "the partial record is dropped, the rest survive",
            len(q2) == 9,
            f"{len(q2)} != 9",
        )
        check(
            "the file is truncated to the last whole record",
            chunk.stat().st_size < before - 3,
            f"{chunk.stat().st_size} bytes",
        )
        drained = []
        while (v := q2.pop()) is not None:
            drained.append(v)
        check(
            "no pop raises on the truncated tail",
            drained == [f"y-{i}".encode() for i in range(9)],
            f"{drained}",
        )
        q2.close()
    finally:
        shutil.rmtree(d, ignore_errors=True)


def test_a_stray_fragment_shorter_than_a_header() -> None:
    """Fewer than 4 bytes of trailing garbage must not be read as a length."""
    d = tempfile.mkdtemp()
    path = os.path.join(d, "q")
    try:
        q = RecoverableFifoDiskQueue(path)
        for i in range(5):
            q.push(f"z-{i}".encode())
        del q
        chunk = Path(path, "q00000")
        with chunk.open("ab") as fh:
            fh.write(b"\x00\x00")  # half a length header

        q2 = RecoverableFifoDiskQueue(path)
        check(
            "a sub-header fragment is ignored",
            len(q2) == 5,
            f"{len(q2)} != 5",
        )
        q2.close()
    finally:
        shutil.rmtree(d, ignore_errors=True)


def test_a_clean_close_skips_the_scan() -> None:
    """close() marks the metadata authoritative so reopening is O(1)."""
    d = tempfile.mkdtemp()
    path = os.path.join(d, "q")
    try:
        q = RecoverableFifoDiskQueue(path)
        for _ in range(5):
            q.push(b"payload")
        q.close()
        info = json.loads(Path(path, "info.json").read_text())
        check("close() records clean=true", info.get("clean") is True, str(info))

        q2 = RecoverableFifoDiskQueue(path)
        check("a clean queue reopens at the right length", len(q2) == 5, str(len(q2)))
        check(
            "the clean marker is not kept in memory",
            "clean" not in q2.info,
            str(q2.info),
        )
        q2.close()
    finally:
        shutil.rmtree(d, ignore_errors=True)


def test_recovery_across_a_chunk_rollover() -> None:
    """head and tail must land on the right chunk, not just the right count."""
    d = tempfile.mkdtemp()
    path = os.path.join(d, "q")
    try:
        q = RecoverableFifoDiskQueue(path, chunksize=4)
        for i in range(11):
            q.push(f"r-{i:02d}".encode())
        for _ in range(5):
            q.pop()
        q.save_info()
        del q

        q2 = RecoverableFifoDiskQueue(path, chunksize=4)
        check(
            "length is right after a rollover",
            len(q2) == 6,
            f"{len(q2)} != 6",
        )
        drained = []
        while (v := q2.pop()) is not None:
            drained.append(v)
        check(
            "pop order is right across chunks",
            drained == [f"r-{i:02d}".encode() for i in range(5, 11)],
            f"{drained}",
        )
        q2.close()
    finally:
        shutil.rmtree(d, ignore_errors=True)


def test_a_deleted_chunk_does_not_break_recovery() -> None:
    """pop() unlinks a drained chunk, so a stale tail can point at nothing."""
    d = tempfile.mkdtemp()
    path = os.path.join(d, "q")
    try:
        q = RecoverableFifoDiskQueue(path, chunksize=4)
        for i in range(11):
            q.push(f"s-{i:02d}".encode())
        for _ in range(5):
            q.pop()  # crosses a chunk boundary, unlinking q00000
        del q  # no checkpoint: info.json still names the deleted chunk

        q2 = RecoverableFifoDiskQueue(path, chunksize=4)
        drained = []
        while (v := q2.pop()) is not None:
            drained.append(v)
        check(
            "recovery falls back to the oldest surviving chunk",
            drained == [f"s-{i:02d}".encode() for i in range(4, 11)],
            f"{drained}",
        )
        q2.close()
    finally:
        shutil.rmtree(d, ignore_errors=True)


def test_records_hold_their_exact_bytes() -> None:
    """The scan must not corrupt payloads that contain header-like bytes."""
    d = tempfile.mkdtemp()
    path = os.path.join(d, "q")
    try:
        payloads = [
            b"\x00\x00\x00\x10" + b"A" * 16,  # looks like a length header
            struct.pack(">L", 999) + b"short",
            b"",  # a zero-length record is legal
            bytes(range(256)),
        ]
        q = RecoverableFifoDiskQueue(path)
        for p in payloads:
            q.push(p)
        del q

        q2 = RecoverableFifoDiskQueue(path)
        got = [q2.pop() for _ in payloads]
        check(
            "payloads round-trip byte for byte",
            got == payloads,
            f"{[len(x) if x is not None else None for x in got]}",
        )
        q2.close()
    finally:
        shutil.rmtree(d, ignore_errors=True)


if __name__ == "__main__":
    test_no_buffers_are_allocated()
    test_a_checkpoint_survives_a_kill()
    test_a_queue_with_no_metadata_replays_everything()
    test_a_partial_record_is_truncated()
    test_a_stray_fragment_shorter_than_a_header()
    test_a_clean_close_skips_the_scan()
    test_recovery_across_a_chunk_rollover()
    test_a_deleted_chunk_does_not_break_recovery()
    test_records_hold_their_exact_bytes()
    if FAILED:
        print(f"\n{len(FAILED)} FAILED: {FAILED}")
        sys.exit(1)
    print("\nall squeue checks passed")
