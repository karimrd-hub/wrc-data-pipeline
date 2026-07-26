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


def _pipe_list(name: str, default: tuple[str, ...]) -> tuple[str, ...]:
    """Parse a pipe-separated list from the env, falling back to ``default``.

    Pipe was chosen as the separator because real User-Agent strings
    contain commas, semicolons and parentheses but never pipes, so no
    escaping is needed. Empty entries are dropped.
    """
    v = os.getenv(name)
    if v is None or v == "":
        return default
    items = tuple(a.strip() for a in v.split("|") if a.strip())
    if not items:
        raise ValueError(f"{name} parsed to an empty list")
    return items


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


# Built-in User-Agent pool. Every outgoing request is stamped with a random
# choice from this tuple by ``RotatingUserAgentMiddleware`` — internal load
# testing showed that a static UA correlated with server-side blocking on
# sustained crawls. Six-agent pool spans current desktop Chrome / Edge /
# Safari / Firefox on Windows / macOS / Linux; extend or replace via the
# ``SCRAPER_USER_AGENT_POOL`` env var (pipe-separated).
_DEFAULT_USER_AGENT_POOL: tuple[str, ...] = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36 Edg/128.0.2739.79",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_5) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.5 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:126.0) Gecko/20100101 Firefox/126.0",
)


@dataclass(frozen=True)
class MongoSettings:
    uri: str
    db: str
    landing_collection: str
    processed_collection: str
    quarantine_collection: str
    server_selection_timeout_ms: int
    connect_timeout_ms: int
    socket_timeout_ms: int

    @classmethod
    def from_env(cls) -> "MongoSettings":
        return cls(
            uri=_str("MONGO_URI", "mongodb://localhost:27017"),
            db=_str("MONGO_DB", "wrc"),
            landing_collection=_str("MONGO_LANDING_COLLECTION", "landing_metadata"),
            processed_collection=_str("MONGO_PROCESSED_COLLECTION", "processed_metadata"),
            quarantine_collection=_str("MONGO_QUARANTINE_COLLECTION", "quarantine_metadata"),
            server_selection_timeout_ms=_int("MONGO_SERVER_SELECTION_TIMEOUT_MS", 5000),
            connect_timeout_ms=_int("MONGO_CONNECT_TIMEOUT_MS", 5000),
            socket_timeout_ms=_int("MONGO_SOCKET_TIMEOUT_MS", 30000),
        )


@dataclass(frozen=True)
class MinioSettings:
    endpoint: str        # host:port, no scheme (minio-py convention)
    access_key: str
    secret_key: str
    secure: bool
    landing_bucket: str
    processed_bucket: str
    connect_timeout_sec: float
    read_timeout_sec: float

    @classmethod
    def from_env(cls) -> "MinioSettings":
        return cls(
            endpoint=_str("MINIO_ENDPOINT", "localhost:9000"),
            access_key=_str("MINIO_ROOT_USER", "minioadmin"),
            secret_key=_str("MINIO_ROOT_PASSWORD", "minioadmin"),
            secure=_bool("MINIO_SECURE", False),
            landing_bucket=_str("MINIO_LANDING_BUCKET", "landing-zone"),
            processed_bucket=_str("MINIO_PROCESSED_BUCKET", "processed"),
            connect_timeout_sec=_float("MINIO_CONNECT_TIMEOUT_SEC", 5.0),
            read_timeout_sec=_float("MINIO_READ_TIMEOUT_SEC", 30.0),
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
    user_agent_pool: tuple[str, ...]

    @classmethod
    def from_env(cls) -> "ScraperSettings":
        partition_size = _str("SCRAPER_PARTITION_SIZE", "monthly").lower()
        if partition_size not in PARTITION_SIZES:
            raise ValueError(
                f"SCRAPER_PARTITION_SIZE must be one of {list(PARTITION_SIZES)}, "
                f"got {partition_size!r}"
            )
        return cls(
            concurrent_requests=_int("SCRAPER_CONCURRENT_REQUESTS", 8),
            concurrent_requests_per_domain=_int("SCRAPER_CONCURRENT_REQUESTS_PER_DOMAIN", 8),
            download_delay=_float("SCRAPER_DOWNLOAD_DELAY", 0.0),
            autothrottle_enabled=_bool("SCRAPER_AUTOTHROTTLE_ENABLED", True),
            autothrottle_start_delay=_float("SCRAPER_AUTOTHROTTLE_START_DELAY", 0.25),
            autothrottle_max_delay=_float("SCRAPER_AUTOTHROTTLE_MAX_DELAY", 10.0),
            autothrottle_target_concurrency=_float("SCRAPER_AUTOTHROTTLE_TARGET_CONCURRENCY", 4.0),
            retry_times=_int("SCRAPER_RETRY_TIMES", 5),
            partition_size=partition_size,
            log_level=_str("SCRAPER_LOG_LEVEL", "INFO").upper(),
            bodies=_bodies("SCRAPER_BODIES", _DEFAULT_BODIES),
            user_agent_pool=_pipe_list("SCRAPER_USER_AGENT_POOL", _DEFAULT_USER_AGENT_POOL),
        )


@dataclass(frozen=True)
class TransformSettings:
    bulk_batch_size: int
    # Minimum size in bytes of the CLEANED payload before we consider a record
    # legitimate. Below this we route to quarantine — WRC's error pages and
    # session-timeout stubs are well under 1 KB, real decisions are 5 KB+.
    min_content_bytes: int

    @classmethod
    def from_env(cls) -> "TransformSettings":
        return cls(
            bulk_batch_size=_int("TRANSFORM_BULK_BATCH_SIZE", 200),
            min_content_bytes=_int("TRANSFORM_MIN_CONTENT_BYTES", 1024),
        )


@dataclass(frozen=True)
class DagsterSettings:
    # ISO YYYY-MM-DD — earliest partition offered in Dagit. Runs before this
    # date are not backfillable without a code change; pick the earliest date
    # you care about (the site itself has decisions back to ~2005).
    partition_start_date: str
    # Wall-clock ceiling for the Scrapy subprocess spawned by landing_records.
    # Safety net against a wedged spider (DNS stall, hung TCP), not an
    # operational knob — one hour is well beyond any observed partition runtime.
    subprocess_timeout_sec: int
    # Per-asset retry policy: how many times Dagster re-runs the asset after
    # the first failure, and how long it waits between attempts.
    landing_max_retries: int
    landing_retry_delay_sec: int
    processed_max_retries: int
    processed_retry_delay_sec: int

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
        return cls(
            partition_start_date=value,
            subprocess_timeout_sec=_int("DAGSTER_SUBPROCESS_TIMEOUT_SEC", 3600),
            landing_max_retries=_int("DAGSTER_LANDING_MAX_RETRIES", 2),
            landing_retry_delay_sec=_int("DAGSTER_LANDING_RETRY_DELAY_SEC", 30),
            processed_max_retries=_int("DAGSTER_PROCESSED_MAX_RETRIES", 2),
            processed_retry_delay_sec=_int("DAGSTER_PROCESSED_RETRY_DELAY_SEC", 15),
        )


@dataclass(frozen=True)
class Settings:
    mongo: MongoSettings
    minio: MinioSettings
    scraper: ScraperSettings
    transform: TransformSettings
    dagster: DagsterSettings

    @classmethod
    def from_env(cls) -> "Settings":
        return cls(
            mongo=MongoSettings.from_env(),
            minio=MinioSettings.from_env(),
            scraper=ScraperSettings.from_env(),
            transform=TransformSettings.from_env(),
            dagster=DagsterSettings.from_env(),
        )


settings: Settings = Settings.from_env()
