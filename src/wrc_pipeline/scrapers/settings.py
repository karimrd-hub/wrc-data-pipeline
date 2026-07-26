# Scrapy settings — thin adapter over `wrc_pipeline.config.settings`.
# Nothing here should be a magic number; if you need to tune a knob, do it in .env.

from wrc_pipeline.config.settings import settings as _cfg
from wrc_pipeline.logging_setup import install_json_root_logging

# Install our JSON formatter on the root logger *before* Scrapy touches
# logging. Combined with LOG_ENABLED=False below, this makes every log record
# from Scrapy, twisted, MinIO, PyMongo and our own code come out as JSON
# (task req 10).
install_json_root_logging(_cfg.scraper.log_level)

BOT_NAME = "wrc_pipeline"

SPIDER_MODULES = ["wrc_pipeline.scrapers.spiders"]
NEWSPIDER_MODULE = "wrc_pipeline.scrapers.spiders"

ADDONS = {}

# Obey robots.txt (recon §7.6 confirmed our targets are not disallowed).
ROBOTSTXT_OBEY = True

# Concurrency + throttling (all env-driven).
CONCURRENT_REQUESTS = _cfg.scraper.concurrent_requests
CONCURRENT_REQUESTS_PER_DOMAIN = _cfg.scraper.concurrent_requests_per_domain
DOWNLOAD_DELAY = _cfg.scraper.download_delay
AUTOTHROTTLE_ENABLED = _cfg.scraper.autothrottle_enabled
AUTOTHROTTLE_START_DELAY = _cfg.scraper.autothrottle_start_delay
AUTOTHROTTLE_MAX_DELAY = _cfg.scraper.autothrottle_max_delay
AUTOTHROTTLE_TARGET_CONCURRENCY = _cfg.scraper.autothrottle_target_concurrency

# Retry idempotent failures a few times; site is IIS/ASP.NET, occasional 5xx expected.
RETRY_ENABLED = True
RETRY_TIMES = _cfg.scraper.retry_times
# Scrapy's default retry list omits 403 and the 520–524 range. Our own
# load testing against WRC showed that when the server pushes back it
# tends to do so with 403 (WAF-style challenge) or one of the
# 520–524 origin/edge errors — treating those as terminal record_failed
# events would strand recoverable records. Retrying lets AutoThrottle
# back off and the request succeed on a subsequent attempt.
RETRY_HTTP_CODES = [403, 408, 429, 500, 502, 503, 504, 520, 521, 522, 523, 524]

# Do NOT let Scrapy reconfigure logging — we own the root handler above.
LOG_ENABLED = False

# --- Anti-fingerprint hygiene ------------------------------------------------
# Internal load testing showed the WRC server is more prone to blocking
# static-fingerprint clients under sustained load. Three layers work
# together so requests look like a real browser session at the WAF layer:
#
#   1. Random User-Agent per request — see ``RotatingUserAgentMiddleware``
#      (wired below; pool lives in ``config.settings``).
#   2. Realistic default headers — ``DEFAULT_REQUEST_HEADERS`` below.
#   3. Retry on the Cloudflare block/edge-error family — ``RETRY_HTTP_CODES``
#      above.

DOWNLOADER_MIDDLEWARES = {
    # Disable Scrapy's static UA middleware — the rotator replaces it.
    "scrapy.downloadermiddlewares.useragent.UserAgentMiddleware": None,
    "wrc_pipeline.scrapers.middlewares.RotatingUserAgentMiddleware": 543,
}

# Header shape a real browser sends on a top-level navigation. Values are
# intentionally not per-request-randomised — a browser doesn't randomise
# them either, and stability on these headers plus rotation on User-Agent
# is what a genuine "different user, different session" fingerprint looks
# like. ``Accept-Language`` is Ireland-first to match the target audience.
DEFAULT_REQUEST_HEADERS = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "en-IE,en-GB;q=0.9,en;q=0.8",
    "Upgrade-Insecure-Requests": "1",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
    "Sec-Fetch-User": "?1",
    "DNT": "1",
}

# Item pipelines. Single StoragePipeline owns hash + MinIO upload + Mongo upsert;
# see wrc_pipeline.scrapers.pipelines for the idempotency contract.
ITEM_PIPELINES = {
    "wrc_pipeline.scrapers.pipelines.StoragePipeline": 300,
}

# Set settings whose default value is deprecated to a future-proof value.
FEED_EXPORT_ENCODING = "utf-8"
