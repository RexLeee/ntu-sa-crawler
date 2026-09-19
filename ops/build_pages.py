#!/usr/bin/env python3
"""Wrap the report fragment into a standalone page for GitHub Pages.

The Artifact runtime supplies the document shell: doctype, charset and
viewport meta, and a small reset. A page served from Pages has to carry all
of that itself, so this script adds it around the same source file.

Usage:
    uv run python ops/build_pages.py <source.html>
    uv run python ops/build_pages.py            # uses the default source

Writes docs/index.html, which is what Pages serves.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DEST = ROOT / "docs" / "index.html"

DEFAULT_SRC = Path(
    "/private/tmp/claude-501/-Users-rex-ntu------------------/"
    "3873137f-4fe6-4d2f-84e5-dd4394667992/scratchpad/crawler_report.html"
)

# Spider-web emoji as an inline SVG, so the page needs no image file.
FAVICON = (
    "data:image/svg+xml,"
    "<svg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 100 100'>"
    "<text y='.9em' font-size='90'>%F0%9F%95%B8%EF%B8%8F</text></svg>"
)

DESC = (
    "NTU IM 系統架構師的鍛造與培育 Starter Project：禮貌性優先的分散式廣度爬蟲，"
    "48 小時執行結果、架構設計與實作問題。"
)

SHELL = """<!doctype html>
<html lang="zh-Hant">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<meta name="description" content="{desc}">
<meta name="color-scheme" content="light dark">
<meta property="og:title" content="{title}">
<meta property="og:description" content="{desc}">
<meta property="og:type" content="article">
<link rel="icon" href="{favicon}">
<style>
/* The reset the Artifact runtime normally injects. No background here: the
   page's own token-based body rule owns the ground, so a literal colour
   would fight its dark theme. */
:root{{color-scheme:light dark}}
body{{margin:0;font:14px system-ui,-apple-system,'Segoe UI',sans-serif}}
img{{max-width:100%}}
[hidden]{{display:none!important}}
</style>
"""


def build(src: Path) -> None:
    fragment = src.read_text(encoding="utf-8")

    match = re.search(r"<title>(.*?)</title>", fragment)
    title = match.group(1) if match else "Crawler Report"

    # Everything up to the end of the first </style> block is head material:
    # the title, the font stylesheet link, and the page's own CSS.
    end = fragment.index("</style>") + len("</style>")
    head, body = fragment[:end], fragment[end:]

    page = (
        SHELL.format(desc=DESC, title=title, favicon=FAVICON)
        + head
        + "\n</head>\n<body>\n"
        + body.strip()
        + "\n</body>\n</html>\n"
    )

    DEST.parent.mkdir(parents=True, exist_ok=True)
    DEST.write_text(page, encoding="utf-8")
    # Pages runs Jekyll by default, which skips files starting with _ and can
    # rewrite others. This opts out.
    (DEST.parent / ".nojekyll").touch()

    print(f"wrote {DEST.relative_to(ROOT)}  "
          f"({len(page.encode('utf-8')):,} bytes)")
    print(f"title: {title}")


def main() -> int:
    src = Path(sys.argv[1]) if len(sys.argv) > 1 else DEFAULT_SRC
    if not src.exists():
        print(f"source not found: {src}", file=sys.stderr)
        return 1
    build(src)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
