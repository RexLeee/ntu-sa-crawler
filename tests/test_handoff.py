#!/usr/bin/env python3
"""Prove sharding keeps every domain on exactly one process.

That property is what makes the politeness timer safe to run per process. If
two processes could both issue requests for one domain, they would each hold
their own 5 second timer and the real rate against that site would double,
with no shared state able to notice.

Five checks:

  * shard_of is stable across processes, which hash() is not
  * the split is even enough that no core sits idle
  * a URL is admitted by exactly one shard, and handed to that one by the rest
  * handoff round-trips, and does not redeliver what it already delivered
  * a partially written line is never handed on as a URL

Run: uv run python tests/test_handoff.py
"""

from __future__ import annotations

import subprocess
import sys
import tempfile
from collections import Counter
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from crawler.handoff import Handoff  # noqa: E402
from crawler.slot import shard_of, slot_key  # noqa: E402

SHARDS = 4


def _domains(limit: int = 3000) -> list[str]:
    """Real domains from the seed list, which is what the split has to handle.

    Synthetic names like d0, d1, d2 share no prefixes and would make any hash
    look even. Real domains cluster heavily.
    """
    seeds = Path(__file__).resolve().parent.parent / "seeds" / "seeds_1000.txt"
    if not seeds.exists():
        return [f"site{i}.example" for i in range(limit)]
    out: list[str] = []
    for line in seeds.read_text(encoding="utf-8").splitlines():
        url = line.strip()
        if url and not url.startswith("#"):
            out.append(slot_key(url))
    return out[:limit]


def test_stable_across_processes() -> bool:
    """A domain must land on the same shard in every process.

    Python's hash() is salted per process by PYTHONHASHSEED, so a split built
    on it would disagree between workers and two of them would crawl the same
    domain. This runs a fresh interpreter to prove shard_of does not.
    """
    domains = _domains(200)
    mine = [shard_of(d, SHARDS) for d in domains]

    root = Path(__file__).resolve().parent.parent
    script = (
        f"import sys; sys.path.insert(0, {str(root)!r})\n"
        "from crawler.slot import shard_of\n"
        f"print(','.join(str(shard_of(d, {SHARDS})) for d in sys.argv[1:]))\n"
    )
    result = subprocess.run(
        [sys.executable, "-c", script, *domains],
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        print(f"FAIL: subprocess failed: {result.stderr.strip()}")
        return False

    theirs = [int(x) for x in result.stdout.strip().split(",")]
    if theirs != mine:
        differing = sum(1 for a, b in zip(mine, theirs, strict=False) if a != b)
        print(f"FAIL: {differing} of {len(mine)} domains landed on a different shard")
        return False
    print(f"PASS: {len(domains)} domains shard identically in a separate process")
    return True


def test_split_is_even() -> bool:
    """No shard may carry much more than its share, or a core idles.

    The threshold is loose because the seed list is a small sample and the
    ratio converges as domains accumulate. Measured max/min at 4 shards:

        1,000 seed domains          1.233x
        2,229 domains a real run reached   1.077x

    The steady state is what matters, and a 48 hour run reaches tens of
    thousands. A ratio above 1.4 here would mean the hash itself is skewed
    rather than the sample being small, which is the failure worth catching:
    hashing only the first 8 bytes of the key gave 2.012x at 8 shards because
    domains share prefixes heavily.
    """
    domains = _domains()
    counts = Counter(shard_of(d, SHARDS) for d in domains)
    if len(counts) != SHARDS:
        print(f"FAIL: only {len(counts)} of {SHARDS} shards received any domain")
        return False
    ratio = max(counts.values()) / min(counts.values())
    if ratio > 1.4:
        print(f"FAIL: load ratio {ratio:.3f}x across {SHARDS} shards: {dict(counts)}")
        return False
    print(
        f"PASS: {len(domains)} domains split {ratio:.3f}x max/min over {SHARDS} shards"
    )
    return True


def test_exactly_one_owner() -> bool:
    """Every URL belongs to one shard and is handed to it by every other."""
    domains = _domains(500)
    urls = [f"https://{d}/page" for d in domains]

    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        nodes = [Handoff(i, SHARDS, root) for i in range(SHARDS)]

        owners: dict[str, list[int]] = {}
        for url in urls:
            owner = shard_of(slot_key(url), SHARDS)
            for node in nodes:
                if owner == node.shard:
                    owners.setdefault(url, []).append(node.shard)
                else:
                    node.send(url, owner)

        for node in nodes:
            node.close()

        wrong = {u: o for u, o in owners.items() if len(o) != 1}
        if wrong:
            print(f"FAIL: {len(wrong)} url(s) claimed by more than one shard")
            return False
        if len(owners) != len(urls):
            print(f"FAIL: {len(urls) - len(owners)} url(s) claimed by no shard")
            return False

        # Every shard must receive exactly the URLs it owns, once each, even
        # though SHARDS-1 senders each sent them.
        for node in nodes:
            got = node.poll(limit=10_000)
            expected = [u for u in urls if shard_of(slot_key(u), SHARDS) == node.shard]
            if sorted(got) != sorted(expected * (SHARDS - 1)):
                print(
                    f"FAIL: shard {node.shard} received {len(got)}, "
                    f"expected {len(expected) * (SHARDS - 1)}"
                )
                return False

    print(f"PASS: all {len(urls)} urls owned by exactly one shard and delivered to it")
    return True


def test_poll_does_not_redeliver() -> bool:
    """A second poll must return only what arrived after the first.

    The offset is what bounds the cost of polling: without it every poll would
    re-read a file that grows for 48 hours, and every URL would be re-injected
    on every tick.
    """
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        sender, receiver = Handoff(0, 2, root), Handoff(1, 2, root)

        sender.send("https://a.example/1", 1)
        sender.send("https://a.example/2", 1)
        first = receiver.poll(limit=100)
        if first != ["https://a.example/1", "https://a.example/2"]:
            print(f"FAIL: first poll returned {first}")
            return False

        if receiver.poll(limit=100) != []:
            print("FAIL: second poll redelivered urls")
            return False

        sender.send("https://a.example/3", 1)
        third = receiver.poll(limit=100)
        if third != ["https://a.example/3"]:
            print(f"FAIL: poll after a new send returned {third}")
            return False

        # The limit must hold, or one tick could inject an unbounded batch and
        # block the reactor.
        for i in range(10):
            sender.send(f"https://a.example/limit{i}", 1)
        if len(receiver.poll(limit=4)) != 4:
            print("FAIL: poll ignored its limit")
            return False
        if len(receiver.poll(limit=100)) != 6:
            print("FAIL: urls held back by the limit were lost")
            return False

        sender.close()
    print("PASS: poll delivers each url once and honours its limit")
    return True


def test_partial_line_is_not_delivered() -> bool:
    """A line the sender has not finished writing must not be handed on.

    The sender appends without locking, so a reader can arrive mid-write. Half
    a URL is not a URL, and delivering it would put a request to the wrong
    host in the frontier.
    """
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        receiver = Handoff(1, 2, root)
        inbox = root / "to-1"
        inbox.mkdir(parents=True, exist_ok=True)
        path = inbox / "from-0.log"

        path.write_bytes(b"https://whole.example/1\nhttps://torn.example/pa")
        got = receiver.poll(limit=100)
        if got != ["https://whole.example/1"]:
            print(f"FAIL: poll returned {got}, expected only the complete line")
            return False

        # Completing the line must deliver it whole, not just its tail.
        with open(path, "ab") as fh:
            fh.write(b"rtial\n")
        got = receiver.poll(limit=100)
        if got != ["https://torn.example/partial"]:
            print(f"FAIL: completed line came back as {got}")
            return False

    print("PASS: a partial line waits for its newline and arrives intact")
    return True


def test_spider_partitions_and_hands_over() -> bool:
    """Two real spiders must split a link set, losing nothing.

    This is the property the whole design rests on: every URL is fetched by
    exactly one process, so exactly one 5 second timer governs each domain.
    The previous checks test shard_of and Handoff separately; this runs the
    spider's own _maybe_follow, which is what production calls.
    """
    from scrapy.utils.test import get_crawler

    from crawler.spiders.broad import BroadSpider

    urls = [f"https://{d}/page" for d in _domains(300)]

    with tempfile.TemporaryDirectory() as tmp:
        spiders = []
        for i in range(2):
            crawler = get_crawler(BroadSpider, {"DUPEFILTER_CLASS": None})
            crawler._apply_settings()
            spider = BroadSpider(shard=i, shards=2)
            spider.handoff = Handoff(i, 2, Path(tmp))
            spiders.append(spider)

        fetched: dict[str, list[int]] = {}
        for spider in spiders:
            for url in urls:
                for request in spider._maybe_follow(url, None) or ():
                    fetched.setdefault(request.url, []).append(spider.shard)

        twice = {u: s for u, s in fetched.items() if len(s) > 1}
        if twice:
            print(f"FAIL: {len(twice)} url(s) would be fetched by both shards")
            return False
        if len(fetched) != len(urls):
            print(f"FAIL: {len(urls) - len(fetched)} url(s) fetched by neither shard")
            return False

        # What one shard refused, it must have handed to the other.
        for spider in spiders:
            other = spiders[1 - spider.shard]
            handed = other.handoff.poll(limit=10_000)
            mine = [u for u in urls if fetched[u] == [other.shard]]
            if sorted(handed) != sorted(mine):
                print(
                    f"FAIL: shard {other.shard} received {len(handed)} urls, "
                    f"owns {len(mine)}"
                )
                return False

            # A handed-over URL goes straight to _admit, which calls
            # url_seen, which requires an already-canonical URL. The inbox
            # must therefore preserve the sender's canonical form exactly:
            # any rewriting in transit would make the two shards fingerprint
            # the same page differently and the dupefilter would stop working
            # across the boundary.
            from w3lib.url import canonicalize_url

            mangled = [u for u in handed if canonicalize_url(u) != u]
            if mangled:
                print(
                    f"FAIL: {len(mangled)} handed url(s) are not canonical, "
                    f"e.g. {mangled[0]}"
                )
                return False
            spider.handoff.close()

        # discovered_raw must be counted before the shard check, so a URL
        # handed away still counts as discovered. Here each spider was fed
        # every URL, which production never does: a link is found by whichever
        # shard holds the page carrying it. So the check is that each spider
        # counted everything it saw, not that the total equals len(urls).
        for spider in spiders:
            if spider.discovered_raw != len(urls):
                print(
                    f"FAIL: shard {spider.shard} counted {spider.discovered_raw} "
                    f"of {len(urls)} urls as discovered"
                )
                return False

    print(f"PASS: {len(urls)} urls partitioned across 2 spiders with no loss")
    return True


def main() -> int:
    results = [
        test_stable_across_processes(),
        test_split_is_even(),
        test_exactly_one_owner(),
        test_poll_does_not_redeliver(),
        test_partial_line_is_not_delivered(),
        test_spider_partitions_and_hands_over(),
    ]
    return 0 if all(results) else 1


if __name__ == "__main__":
    raise SystemExit(main())
