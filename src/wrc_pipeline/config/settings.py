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


def _bodies(name: str, default: dict[int, str]) -> dict[int, str]:
    """Parse a ``SCRAPER_BODIES``-style value: comma-separated ``id:Name`` pairs.

    Colons inside body names would be ambiguous but no WRC body contains one,
    so we split on the *first* colon only — the name may contain any other
    character.
    """
    v = os.getenv(name)
    if v is None or v == "":
        return default
    result: dict[int, str] = {}
    for entry in v.split(","):
        entry = entry.strip()
        if not entry:
            continue
        if ":" not in entry:
            raise ValueError(
                f"{name} entries must be 'id:Name' pairs, got {entry!r}"
            )
        raw_id, raw_name = entry.split(":", 1)
        result[int(raw_id.strip())] = raw_name.strip()
    if not result:
        raise ValueError(f"{name} parsed to an empty mapping")
    return result


_DEFAULT_BODIES: dict[int, str] = {
    1: "Equality Tribunal",
    2: "Employment Appeals Tribunal",
    3: "Labour Court",
    15376: "Workplace Relations Commission",
}


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
    autothrottle_start_delay: float
    autothrottle_max_delay: float
    autothrottle_target_concurrency: float
    retry_times: int
    partition_size: str
    log_level: str
    bodies: dict[int, str]

    @classmethod
    def from_env(cls) -> "ScraperSettings":
        partition_size = _str("SCRAPER_PARTITION_SIZE", "monthly").lower()
        if partition_size not in PARTITION_SIZES:
            raise ValueError(
                f"SCRAPER_PARTITION_SIZE must be one of {list(PARTITION_SIZES)}, "
                f"got {partition_size!r}"
            )
        return cls(
            # Defaults aligned with the reference repo; see
            # docs/performance-baseline.md for the shelved aggressive numbers.
            concurrent_requests=_int("SCRAPER_CONCURRENT_REQUESTS", 16),
            concurrent_requests_per_domain=_int("SCRAPER_CONCURRENT_REQUESTS_PER_DOMAIN", 16),
            download_delay=_float("SCRAPER_DOWNLOAD_DELAY", 0.0),
            autothrottle_enabled=_bool("SCRAPER_AUTOTHROTTLE_ENABLED", True),
            autothrottle_start_delay=_float("SCRAPER_AUTOTHROTTLE_START_DELAY", 0.25),
            autothrottle_max_delay=_float("SCRAPER_AUTOTHROTTLE_MAX_DELAY", 10.0),
            autothrottle_target_concurrency=_float("SCRAPER_AUTOTHROTTLE_TARGET_CONCURRENCY", 4.0),
            retry_times=_int("SCRAPER_RETRY_TIMES", 5),
            partition_size=partition_size,
            log_level=_str("SCRAPER_LOG_LEVEL", "INFO").upper(),
            bodies=_bodies("SCRAPER_BODIES", _DEFAULT_BODIES),
        )


@dataclass(frozen=True)
class DagsterSettings:
    # ISO YYYY-MM-DD — earliest partition offered in Dagit. Runs before this
    # date are not backfillable without a code change; pick the earliest date
    # you care about (the site itself has decisions back to ~2005).
    partition_start_date: str

    @classmethod
    def from_env(cls) -> "DagsterSettings":
        value = _str("DAGSTER_PARTITION_START_DATE", "2020-01-01")
        # Fail-fast on malformed dates rather than deferring to Dagster's
        # opaque partition-key error at asset materialization time.
        try:
            from datetime import date as _date
            _date.fromisoformat(value)
        except ValueError as exc:
            raise ValueError(
                f"DAGSTER_PARTITION_START_DATE must be YYYY-MM-DD, got {value!r}"
            ) from exc
        return cls(partition_start_date=value)


@dataclass(frozen=True)
class Settings:
    mongo: MongoSettings
    minio: MinioSettings
    scraper: ScraperSettings
    dagster: DagsterSettings

    @classmethod
    def from_env(cls) -> "Settings":
        return cls(
            mongo=MongoSettings.from_env(),
            minio=MinioSettings.from_env(),
            scraper=ScraperSettings.from_env(),
            dagster=DagsterSettings.from_env(),
        )


settings: Settings = Settings.from_env()
