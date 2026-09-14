"""Link extraction.

Scrapy's LinkExtractor builds a full lxml tree. We only need href values, and
bandwidth is our bottleneck rather than accuracy, so a precompiled regex is the
better trade: roughly 5-8x faster at the cost of missing exotic markup.
"""

from __future__ import annotations

import re

# Capture href values inside <a> tags. Handles single quotes, double quotes and
# bare attribute values.
_HREF_RE = re.compile(
    rb"""<a\s[^>]*?href\s*=\s*(?:"([^"]{1,2048})"|'([^']{1,2048})'|([^\s"'<>`]{1,2048}))""",
    re.IGNORECASE | re.DOTALL,
)

_BASE_RE = re.compile(
    rb"""<base\s[^>]*?href\s*=\s*(?:"([^"]{1,2048})"|'([^']{1,2048})')""",
    re.IGNORECASE,
)

_CHARSET_RE = re.compile(rb"""charset\s*=\s*["']?([A-Za-z0-9_\-]{2,40})""", re.IGNORECASE)


def find_base_href(body: bytes) -> str | None:
    """Return the document's <base href> when present."""
    m = _BASE_RE.search(body[:8192])
    if not m:
        return None
    raw = m.group(1) or m.group(2)
    return _decode(raw)


def _decode(raw: bytes) -> str:
    return raw.decode("utf-8", errors="replace").strip()


def iter_hrefs(body: bytes) -> list[str]:
    """Extract raw href strings from an HTML body, in document order."""
    out = []
    for m in _HREF_RE.finditer(body):
        raw = m.group(1) or m.group(2) or m.group(3)
        if not raw:
            continue
        href = _decode(raw)
        if href:
            out.append(href)
    return out


def declared_charset(body: bytes) -> str | None:
    """Best-effort charset sniff from the first chunk of the document."""
    m = _CHARSET_RE.search(body[:4096])
    return m.group(1).decode("ascii", errors="replace") if m else None
