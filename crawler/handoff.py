"""Pass URLs between sharded crawler processes.

Each process owns a disjoint set of domains, chosen by shard_of(). That is
what makes the politeness timer safe to run per process. It also means a
process constantly finds URLs it must not fetch: every page links off its own
domains, and in a broad crawl most links leave the site.

Dropping those URLs is not an option. Throughput is bounded by the number of
domains a process holds:

    pages/sec <= active domains / delay

and nearly every new domain is discovered through a cross-domain link. A
process that discards them keeps only the domains its own seeds reached, so N
processes would crawl little more than one. The links have to reach whoever
owns them.

The transport is an append-only text file per (sender, recipient) pair,
rotated into numbered segments. That is enough because the traffic is
one-directional and unordered, and because losing a URL is survivable: it
will almost certainly be linked again from another page. A queue with
delivery guarantees would cost far more than the thing it protects.

Ordering is what makes the file safe without locking. Each sender appends to
its own segment, so there is exactly one writer per file and no interleaving
to coordinate. Readers track a byte offset and only ever read what is already
written. A line torn by a reader catching a partial write is dropped by the
offset rule below rather than being handed on as a corrupt URL.

Three properties this file adds beyond "append and seek", each because a 48
hour run needs it:

  * Segments. A single file grew without bound: a measured hour wrote 1.79M
    uncompressed URLs, about 143 MB, and nothing ever truncated it. A segment
    that has been read to the end and is not the sender's newest is deleted,
    so the inbox holds roughly one segment per sender rather than the whole
    run.
  * Bounded reads. poll() used to fh.read() to the end of the file, which
    materialises everything written since the last poll. It now reads a fixed
    number of bytes per sender per call, so the reactor thread cannot be held
    by a burst.
  * Persisted offsets. They lived only in memory, so a restarted shard reread
    its inbox from zero and replayed every URL ever handed to it. They are
    now written next to the inbox after every poll.
"""

from __future__ import annotations

import json
import logging
import os
import re
from pathlib import Path

logger = logging.getLogger(__name__)

# from-<sender>.<seq>.log
_SEGMENT_RE = re.compile(r"^from-(\d+)\.(\d+)\.log$")


class Handoff:
    """Send URLs to the shard that owns them, and receive the ones we own."""

    def __init__(
        self,
        shard: int,
        shards: int,
        root: Path,
        segment_bytes: int = 67_108_864,
        read_bytes: int = 4_194_304,
    ):
        self.shard = shard
        self.shards = shards
        self.root = Path(root)
        self._segment_bytes = int(segment_bytes)
        self._read_bytes = int(read_bytes)

        # One open segment per recipient, opened lazily: a shard that never
        # links to another never creates its file.
        self._outbox: dict[int, object] = {}
        self._seq: dict[int, int] = {}
        self._inbox_dir = self.root / f"to-{shard}"
        self._inbox_dir.mkdir(parents=True, exist_ok=True)
        self._offsets_path = self._inbox_dir / "offsets.json"
        # Byte offset consumed per segment file name.
        self._offsets: dict[str, int] = self._load_offsets()

        self.sent = 0
        self.received = 0

    # --- offsets ------------------------------------------------------------

    def _load_offsets(self) -> dict[str, int]:
        """Read the offsets a previous process left, if any.

        A restart that starts from zero replays every URL the inbox ever held.
        The Bloom filter absorbs most of that, but only if it too survived,
        and the work is wasted either way.
        """
        try:
            raw = self._offsets_path.read_text(encoding="utf-8")
        except FileNotFoundError:
            return {}
        except OSError as exc:
            logger.warning("cannot read handoff offsets, starting from 0: %s", exc)
            return {}
        try:
            data = json.loads(raw)
        except ValueError as exc:
            logger.warning("handoff offsets are corrupt, starting from 0: %s", exc)
            return {}
        if not isinstance(data, dict):
            return {}
        return {str(k): int(v) for k, v in data.items() if isinstance(v, int)}

    def _save_offsets(self) -> None:
        """Persist the offsets atomically.

        tmp plus os.replace, so a kill during the write leaves the previous
        file rather than a truncated one. A stale offsets file only costs a
        replay of one poll's worth of URLs.
        """
        tmp = self._offsets_path.with_suffix(".json.tmp")
        try:
            tmp.write_text(json.dumps(self._offsets), encoding="utf-8")
            os.replace(tmp, self._offsets_path)
        except OSError as exc:
            logger.warning("cannot persist handoff offsets: %s", exc)

    # --- sending ------------------------------------------------------------

    def _next_seq(self, owner: int) -> int:
        """Pick a segment number that does not collide with an existing file.

        A resumed run must not append to a segment the recipient has already
        read to the end and recorded an offset for.
        """
        directory = self.root / f"to-{owner}"
        highest = -1
        try:
            names = [p.name for p in directory.iterdir()]
        except OSError:
            names = []
        for name in names:
            match = _SEGMENT_RE.match(name)
            if match and int(match.group(1)) == self.shard:
                highest = max(highest, int(match.group(2)))
        return highest + 1

    def _writer(self, owner: int):
        fh = self._outbox.get(owner)
        if fh is not None:
            # Rotate once the segment is large enough. The recipient deletes a
            # segment it has finished with, which is what bounds the inbox.
            if fh.tell() >= self._segment_bytes:
                try:
                    fh.close()
                except OSError:
                    pass
                del self._outbox[owner]
                fh = None
        if fh is None:
            directory = self.root / f"to-{owner}"
            directory.mkdir(parents=True, exist_ok=True)
            seq = self._seq.get(owner)
            if seq is None:
                seq = self._next_seq(owner)
            else:
                seq += 1
            self._seq[owner] = seq
            # Line buffered. The recipient polls on a timer, so a URL sitting
            # in a 4 KB buffer would arrive minutes late or, if the run is
            # killed, never. One line per write is the point of the design.
            fh = open(
                directory / f"from-{self.shard}.{seq:06d}.log",
                "a",
                buffering=1,
                encoding="utf-8",
            )
            self._outbox[owner] = fh
        return fh

    def send(self, url: str, owner: int) -> None:
        """Hand a URL to the shard that owns its domain.

        The caller has already computed the owner, since it needed it to
        decide this URL was not ours.
        """
        try:
            self._writer(owner).write(url + "\n")
            self.sent += 1
        except OSError as exc:
            # A failed handoff costs one URL, which the crawl will almost
            # certainly rediscover. Taking the crawl down over it would not.
            logger.warning("handoff send failed: %s", exc)

    # --- receiving ----------------------------------------------------------

    def _segments(self) -> list[Path]:
        """Inbox segments in send order: by sender, then by sequence."""
        try:
            entries = list(self._inbox_dir.iterdir())
        except OSError:
            return []
        found: list[tuple[int, int, Path]] = []
        for path in entries:
            match = _SEGMENT_RE.match(path.name)
            if match:
                found.append((int(match.group(1)), int(match.group(2)), path))
        found.sort()
        return [path for _sender, _seq, path in found]

    def poll(self, limit: int) -> list[str]:
        """Return up to limit URLs sent to us since the last call.

        Reads whole lines only. A trailing fragment means the sender was
        mid-write, so the offset stops before it and the next poll picks the
        line up complete.

        Reads at most read_bytes per segment per call. This runs on the
        reactor thread, so the work per call has to be bounded by something
        other than how much the other shards happened to write.
        """
        out: list[str] = []
        segments = self._segments()
        # A sender's newest segment is the one it is still appending to, so
        # reaching its end does not mean it is finished.
        newest: dict[int, str] = {}
        for path in segments:
            match = _SEGMENT_RE.match(path.name)
            if match:
                newest[int(match.group(1))] = path.name

        dirty = False
        for path in segments:
            if len(out) >= limit:
                break
            name = path.name
            offset = self._offsets.get(name, 0)
            try:
                with open(path, "rb") as fh:
                    fh.seek(offset)
                    chunk = fh.read(self._read_bytes)
                    at_end = fh.read(1) == b""
            except OSError:
                continue

            if chunk:
                consumed = 0
                truncated = len(chunk) == self._read_bytes or not at_end
                for raw in chunk.split(b"\n"):
                    if len(out) >= limit:
                        break
                    # split() leaves the trailing fragment (or an empty string
                    # after a final newline) as the last element. Only advance
                    # past bytes that ended in a newline.
                    if consumed + len(raw) >= len(chunk):
                        break
                    consumed += len(raw) + 1
                    url = raw.decode("utf-8", "replace").strip()
                    if url:
                        out.append(url)
                if consumed:
                    self._offsets[name] = offset + consumed
                    dirty = True
                if truncated:
                    # More to read in this segment; leave it for the next poll
                    # rather than deleting it below.
                    continue

            # Nothing left in this segment. Delete it if its sender has moved
            # on, which is what keeps the inbox from holding the whole run.
            match = _SEGMENT_RE.match(name)
            sender = int(match.group(1)) if match else None
            if at_end and sender is not None and newest.get(sender) != name:
                try:
                    path.unlink()
                except OSError:
                    continue
                self._offsets.pop(name, None)
                dirty = True

        if dirty:
            self._save_offsets()
        self.received += len(out)
        return out

    def close(self) -> None:
        for fh in self._outbox.values():
            try:
                fh.close()
            except OSError:
                pass
        self._outbox.clear()
        self._save_offsets()
