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

The transport is an append-only text file per (sender, recipient) pair. That
is enough because the traffic is one-directional and unordered, and because
losing a URL is survivable: it will almost certainly be linked again from
another page. A queue with delivery guarantees would cost far more than the
thing it protects.

Ordering is what makes the file safe without locking. Each sender appends to
its own file, so there is exactly one writer per file and no interleaving to
coordinate. Readers track a byte offset and only ever read what is already
written. A line torn by a reader catching a partial write is dropped by the
offset rule below rather than being handed on as a corrupt URL.
"""

from __future__ import annotations

import logging
from pathlib import Path

logger = logging.getLogger(__name__)


class Handoff:
    """Send URLs to the shard that owns them, and receive the ones we own."""

    def __init__(self, shard: int, shards: int, root: Path):
        self.shard = shard
        self.shards = shards
        self.root = Path(root)

        # One outbox file per recipient, opened lazily: a shard that never
        # links to another never creates its file.
        self._outbox: dict[int, object] = {}
        self._inbox_dir = self.root / f"to-{shard}"
        self._inbox_dir.mkdir(parents=True, exist_ok=True)
        # Byte offset consumed per sender file.
        self._offsets: dict[Path, int] = {}

        self.sent = 0
        self.received = 0

    def _writer(self, owner: int):
        fh = self._outbox.get(owner)
        if fh is None:
            directory = self.root / f"to-{owner}"
            directory.mkdir(parents=True, exist_ok=True)
            # Line buffered. The recipient polls on a timer, so a URL sitting
            # in a 4 KB buffer would arrive minutes late or, if the run is
            # killed, never. One line per write is the point of the design.
            fh = open(
                directory / f"from-{self.shard}.log", "a", buffering=1, encoding="utf-8"
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

    def poll(self, limit: int) -> list[str]:
        """Return up to limit URLs sent to us since the last call.

        Reads whole lines only. A trailing fragment means the sender was
        mid-write, so the offset stops before it and the next poll picks the
        line up complete.
        """
        out: list[str] = []
        try:
            senders = sorted(self._inbox_dir.glob("from-*.log"))
        except OSError:
            return out

        for path in senders:
            if len(out) >= limit:
                break
            offset = self._offsets.get(path, 0)
            try:
                with open(path, "rb") as fh:
                    fh.seek(offset)
                    chunk = fh.read()
            except OSError:
                continue
            if not chunk:
                continue

            consumed = 0
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
            self._offsets[path] = offset + consumed

        self.received += len(out)
        return out

    def close(self) -> None:
        for fh in self._outbox.values():
            try:
                fh.close()
            except OSError:
                pass
        self._outbox.clear()
