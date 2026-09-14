"""URL normalization and trap filtering.

Frontier quality decides how much of our scarce bandwidth reaches pages that
actually carry new links. Two jobs here: collapse URLs that differ only
cosmetically, and drop the shapes that are known traps.
"""

from __future__ import annotations

from urllib.parse import parse_qsl, urljoin, urlsplit

from w3lib.url import canonicalize_url

_ALLOWED_SCHEMES = frozenset({"http", "https"})


class UrlFilter:
    """Applies config-driven rules to candidate URLs."""

    def __init__(self, cfg: dict):
        f = cfg["filters"]
        self.max_url_length: int = f["max_url_length"]
        self.max_query_params: int = f["max_query_params"]
        self.max_path_depth: int = f["max_path_depth"]
        self.skip_extensions = frozenset(e.lower() for e in f["skip_extensions"])
        self.skip_query_keys = frozenset(k.lower() for k in f["skip_query_keys"])

    def normalize(self, href: str, base_url: str) -> str | None:
        """Resolve a raw href against its page and canonicalize it.

        Returns None when the URL is unusable or fails a filter rule.
        """
        href = href.strip()
        if not href or href.startswith("#"):
            return None

        # Reject javascript:, mailto:, tel:, data: and friends before joining.
        lowered = href.lower()
        if ":" in lowered[:12]:
            scheme = lowered.split(":", 1)[0]
            if scheme and scheme not in _ALLOWED_SCHEMES and "/" not in scheme:
                return None

        try:
            absolute = urljoin(base_url, href)
        except ValueError:
            return None

        parts = urlsplit(absolute)
        if parts.scheme not in _ALLOWED_SCHEMES or not parts.hostname:
            return None

        # Drop the fragment; it never changes what the server returns.
        absolute = absolute.split("#", 1)[0]

        try:
            canonical = canonicalize_url(absolute)
        except (ValueError, UnicodeError):
            return None

        return canonical if self.accepts(canonical) else None

    def accepts(self, url: str) -> bool:
        """Return True when the URL is worth fetching."""
        if len(url) > self.max_url_length:
            return False

        parts = urlsplit(url)
        path = parts.path

        if self._has_skipped_extension(path):
            return False

        if path.count("/") > self.max_path_depth:
            return False

        if parts.query:
            try:
                params = parse_qsl(parts.query, keep_blank_values=True)
            except ValueError:
                return False
            if len(params) > self.max_query_params:
                return False
            for key, _ in params:
                if key.lower() in self.skip_query_keys:
                    return False

        return True

    def _has_skipped_extension(self, path: str) -> bool:
        last = path.rsplit("/", 1)[-1]
        if "." not in last:
            return False
        ext = last.rsplit(".", 1)[-1].lower()
        return ext in self.skip_extensions
