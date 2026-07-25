"""MinIO client + bucket bootstrap.

Framework-agnostic (see storage/mongo.py header). docker-compose already
creates the two buckets via the ``minio-init`` one-shot container, so
``ensure_bucket`` is a defensive belt-and-braces for local dev runs where the
scraper points at an existing MinIO without the init container having run.
"""

from __future__ import annotations

from minio import Minio

from wrc_pipeline.config.settings import settings


def get_minio_client() -> Minio:
    return Minio(
        settings.minio.endpoint,
        access_key=settings.minio.access_key,
        secret_key=settings.minio.secret_key,
        secure=settings.minio.secure,
    )


def ensure_bucket(client: Minio, bucket: str) -> None:
    if not client.bucket_exists(bucket):
        client.make_bucket(bucket)
