"""Downloader middlewares — anti-fingerprint hygiene.

Ships a single middleware, ``RotatingUserAgentMiddleware``, which stamps
every outgoing request with a random ``User-Agent`` drawn from a pool.
The static default UA that Scrapy sends out of the box
(``Scrapy/2.x (+https://scrapy.org)``) is trivial for a WAF or bot-scoring
model to fingerprint; internal load testing against WRC showed that
rotating a small pool of realistic desktop-browser UAs correlated with
noticeably fewer sustained-crawl blocks.

Pool is sourced from ``ScraperSettings.user_agent_pool`` — a built-in
tuple of six UAs with the ``SCRAPER_USER_AGENT_POOL`` env var as override
(pipe-separated). An empty pool is a startup error, not a silent
fallback: we want the anti-fingerprint layer either fully installed or
plainly absent, never half-broken.
"""

from __future__ import annotations

import random

from wrc_pipeline.config.settings import settings


class RotatingUserAgentMiddleware:
    """Assign a random ``User-Agent`` header per outgoing request."""

    def __init__(self, pool: tuple[str, ...]) -> None:
        if not pool:
            raise ValueError(
                "RotatingUserAgentMiddleware requires a non-empty user_agent_pool"
            )
        self._pool = pool

    @classmethod
    def from_crawler(cls, crawler):
        return cls(pool=settings.scraper.user_agent_pool)

    def process_request(self, request, spider):
        # ``random.choice`` is intentional — cryptographic randomness would
        # buy nothing here, and per-request choice keeps the distribution
        # roughly uniform across the pool even for short crawls.
        request.headers[b"User-Agent"] = random.choice(self._pool)
        return None
