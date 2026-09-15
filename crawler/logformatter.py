"""Log formatter that keeps download failures out of the log file.

A broad crawl fails constantly and that is normal: dead domains, hosts that
never answer, TLS that never completes. Scrapy logs each one at ERROR with a
full traceback, which LOG_LEVEL=WARNING does not suppress.

Measured over 28 minutes: 94,658 ERROR records inside a 495 MB scrapy.log,
which extrapolates to roughly 50 GB across 48 hours. The information is not
lost. Every failure is already counted in the stats dump under
downloader/exception_type_count/*, which is the form the report needs anyway.
"""

from __future__ import annotations

import logging

from scrapy.logformatter import LogFormatter


class QuietLogFormatter(LogFormatter):
    """Demotes per-request download failures from ERROR to DEBUG."""

    def download_error(self, failure, request, spider, errmsg=None):
        result = super().download_error(failure, request, spider, errmsg)
        result["level"] = logging.DEBUG
        return result
