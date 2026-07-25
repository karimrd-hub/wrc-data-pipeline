"""Typed configuration loader for the WRC pipeline.

Every knob has a sensible default so the pipeline runs without a `.env`.
A `.env` at repo root (or anywhere in the CWD lookup chain) overrides defaults.
Import the module-level `settings` singleton anywhere in the code base — do NOT
call `Settings.from_env()` again elsewhere, so all callers see the same values.
"""

from __future__ import annotations

import os
from dataclasses import dataclass

from dotenv import load_dotenv

from wrc_pipeline.scrapers.utils.dates import PARTITION_SIZES  # single source of truth

# Load .env once at import time. `override=False` means real env vars win over
# .env values — useful for docker-compose runs where creds come from the shell.
load_dotenv(override=False)


def _str(name: str, default: str) -> str:
    v = os.getenv(name)
    return v if v is not None and v != "" else default


def _int(name: str, default: int) -> int:
    v = os.getenv(name)
    return int(v) if v not in (None, "") else default


def _float(name: str, default: float) -> float:
    v = os.getenv(name)
    return float(v) if v not in (None, "") else default


def _bool(name: str, default: bool) -> bool:
    v = os.getenv(name)
    if v is None or v == "":
        return default
    return v.strip().lower() in ("1", "true", "yes", "on")


@dataclass(frozen=True)
class MongoSettings:
    uri: str
    db: str
    landing_collection: str
    processed_collection: str

    @classmethod
    def from_env(cls) -> "MongoSettings":
        return cls(
            uri=_str("MONGO_URI", "mongodb://localhost:27017"),
            db=_str("MONGO_DB", "wrc"),
            landing_collection=_str("MONGO_LANDING_COLLECTION", "landing_metadata"),
            processed_collection=_str("MONGO_PROCESSED_COLLECTION", "processed_metadata"),
        )


@dataclass(frozen=True)
class MinioSettings:
    endpoint: str        # host:port, no scheme (minio-py convention)
    access_key: str
    secret_key: str
    secure: bool
    landing_bucket: str
    processed_bucket: str

    @classmethod
    def from_env(cls) -> "MinioSettings":
        return cls(
            endpoint=_str("MINIO_ENDPOINT", "localhost:9000"),
            access_key=_str("MINIO_ROOT_USER", "minioadmin"),
            secret_key=_str("MINIO_ROOT_PASSWORD", "minioadmin"),
            secure=_bool("MINIO_SECURE", False),
            landing_bucket=_str("MINIO_LANDING_BUCKET", "landing-zone"),
            processed_bucket=_str("MINIO_PROCESSED_BUCKET", "processed"),
        )


@dataclass(frozen=True)
class ScraperSettings:
    concurrent_requests: int
    concurrent_requests_per_domain: int
    download_delay: float
    autothrottle_enabled: bool
    autothrottle_target_concurrency: float
    retry_times: int
    partition_size: str
    log_level: str

    @classmethod
    def from_env(cls) -> "ScraperSettings":
        partition_size = _str("SCRAPER_PARTITION_SIZE", "monthly").lower()
        if partition_size not in PARTITION_SIZES:
            raise ValueError(
                f"SCRAPER_PARTITION_SIZE must be one of {list(PARTITION_SIZES)}, "
                f"got {partition_size!r}"
            )
        return cls(
            concurrent_requests=_int("SCRAPER_CONCURRENT_REQUESTS", 32),
            concurrent_requests_per_domain=_int("SCRAPER_CONCURRENT_REQUESTS_PER_DOMAIN", 16),
            download_delay=_float("SCRAPER_DOWNLOAD_DELAY", 0.0),
            autothrottle_enabled=_bool("SCRAPER_AUTOTHROTTLE_ENABLED", True),
            autothrottle_target_concurrency=_float("SCRAPER_AUTOTHROTTLE_TARGET_CONCURRENCY", 8.0),
            retry_times=_int("SCRAPER_RETRY_TIMES", 3),
            partition_size=partition_size,
            log_level=_str("SCRAPER_LOG_LEVEL", "INFO").upper(),
        )


@dataclass(frozen=True)
class Settings:
    mongo: MongoSettings
    minio: MinioSettings
    scraper: ScraperSettings

    @classmethod
    def from_env(cls) -> "Settings":
        return cls(
            mongo=MongoSettings.from_env(),
            minio=MinioSettings.from_env(),
            scraper=ScraperSettings.from_env(),
        )


settings: Settings = Settings.from_env()
