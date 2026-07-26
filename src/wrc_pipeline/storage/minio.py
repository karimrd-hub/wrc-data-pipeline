"""MinIO client + bucket bootstrap.

Framework-agnostic (see storage/mongo.py header). docker-compose already
creates the two buckets via the ``minio-init`` one-shot container, so
``ensure_bucket`` is a defensive belt-and-braces for local dev runs where the
scraper points at an existing MinIO without the init container having run.
"""

from __future__ import annotations

import urllib3
from minio import Minio

from wrc_pipeline.config.settings import settings


# Hardcoded rather than env-driven: safety nets against a half-open connection,
# not operational tuning knobs. Defaults are generous enough for large PDFs
# on slow uplinks; tighten if you actually observe hangs.
_CONNECT_TIMEOUT_SEC = 5.0
_READ_TIMEOUT_SEC = 30.0


def get_minio_client() -> Minio:
    http_client = urllib3.PoolManager(
        timeout=urllib3.Timeout(connect=_CONNECT_TIMEOUT_SEC, read=_READ_TIMEOUT_SEC),
    )
    return Minio(
        settings.minio.endpoint,
        access_key=settings.minio.access_key,
        secret_key=settings.minio.secret_key,
        secure=settings.minio.secure,
        http_client=http_client,
    )


def ensure_bucket(client: Minio, bucket: str) -> None:
    if not client.bucket_exists(bucket):
        client.make_bucket(bucket)
