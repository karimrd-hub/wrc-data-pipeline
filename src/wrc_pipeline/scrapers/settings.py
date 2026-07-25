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
AUTOTHROTTLE_START_DELAY = 1.0
AUTOTHROTTLE_MAX_DELAY = 10.0
AUTOTHROTTLE_TARGET_CONCURRENCY = _cfg.scraper.autothrottle_target_concurrency

# Retry idempotent failures a few times; site is IIS/ASP.NET, occasional 5xx expected.
RETRY_ENABLED = True
RETRY_TIMES = _cfg.scraper.retry_times

# Do NOT let Scrapy reconfigure logging — we own the root handler above.
LOG_ENABLED = False

# Item pipelines. Single StoragePipeline owns hash + MinIO upload + Mongo upsert;
# see wrc_pipeline.scrapers.pipelines for the idempotency contract.
ITEM_PIPELINES = {
    "wrc_pipeline.scrapers.pipelines.StoragePipeline": 300,
}

# Set settings whose default value is deprecated to a future-proof value.
FEED_EXPORT_ENCODING = "utf-8"
