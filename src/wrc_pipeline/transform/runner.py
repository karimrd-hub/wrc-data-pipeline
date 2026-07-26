"""Transform runner: landing zone -> processed bucket + processed collection.

Task requirements this module owns (docs/task.md, transformation script):

* (a) Fetch metadata from Mongo by ``start_date`` / ``end_date``.
* (b) Pull the raw bytes from the landing bucket.
* (c) Iterate: HTML -> BeautifulSoup cleaner (``transform/cleaner.py``);
      PDF/DOC -> passthrough (task rule "don't apply any transformation").
* (c/2) Recompute ``file_hash`` on the cleaned bytes.
* (c/iii) Rename to ``{identifier}.{ext}`` in the processed bucket.
* (c/iv) Write to the processed bucket.
* (c/v) Upsert into the processed collection with the new path + new hash.

Idempotency (task req 9 is a whole-pipeline requirement, not just scraper):
we compare the *newly-computed* ``file_hash`` of the cleaned payload against
whatever is already in the processed collection for this identifier. If they
match we skip the MinIO put and just bump ``last_transformed_at`` — so
re-running against the same landing state is free. This also handles the
"cleaner algo changed" case: a code change alters the cleaned bytes -> hash
differs -> the record is re-materialized on the next run.

Everything is streamed one record at a time — Mongo cursor + MinIO
``get_object`` + BS4 parse + MinIO ``put_object``. At the reference volume
(500-1000 records) memory is bounded to a single decoded document at a
time. Batching hooks (Mongo ``bulk_write``, parallel MinIO puts) are the
1000x-scale levers called out in ``ARCHITECTURE.md`` — deliberately
un-shipped here to keep the three-outcome branching readable.
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from typing import Iterable

from minio import Minio
from minio.error import S3Error
from pymongo.collection import Collection
from pymongo.errors import PyMongoError

from wrc_pipeline.config.settings import settings
from wrc_pipeline.logging_setup import get_json_logger
from wrc_pipeline.scrapers.utils.bodies import BODIES, body_slug
from wrc_pipeline.storage.hashing import sha256_hash
from wrc_pipeline.storage.minio import ensure_bucket, get_minio_client
from wrc_pipeline.storage.mongo import ensure_indexes, get_collection, get_mongo_client
from wrc_pipeline.transform.cleaner import ContentNotFoundError, clean_html


# Same mapping as the scraper pipeline — extension -> media type. Duplicated
# rather than imported from ``scrapers.pipelines`` to keep the transform
# module free of Scrapy imports (decisions.md §4.5).
EXT_MIME = {
    "html": "text/html",
    "pdf": "application/pdf",
    "doc": "application/msword",
    "docx": "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
}


@dataclass
class TransformStats:
    transformed: int = 0
    unchanged: int = 0
    passthrough: int = 0
    failed: int = 0
    per_partition: dict[tuple[str, str], dict[str, int]] = field(
        default_factory=lambda: defaultdict(_new_partition_counter)
    )

    def bump(self, body: str, partition_date: str, outcome: str) -> None:
        setattr(self, outcome, getattr(self, outcome) + 1)
        self.per_partition[(body, partition_date)][outcome] += 1


def _new_partition_counter() -> dict[str, int]:
    return {"transformed": 0, "unchanged": 0, "passthrough": 0, "failed": 0}


def _mongo_error_extra(exc: PyMongoError) -> dict:
    return {
        "collection": settings.mongo.processed_collection,
        "error": type(exc).__name__,
        "message": str(exc)[:200],
    }


def _validate_bodies(body_ids: Iterable[int] | None) -> list[str] | None:
    if body_ids is None:
        return None
    names = []
    for bid in body_ids:
        if bid not in BODIES:
            raise ValueError(f"Unknown body id: {bid} (known: {list(BODIES)})")
        names.append(BODIES[bid])
    return names


class TransformRunner:
    """Orchestrates a single ``[start_date, end_date]`` transform run.

    Instances are single-use — one ``run()`` per instance keeps the Mongo /
    MinIO clients scoped to a well-defined lifetime and avoids state leaking
    across CLI invocations.
    """

    def __init__(
        self,
        landing_collection: Collection | None = None,
        processed_collection: Collection | None = None,
        minio: Minio | None = None,
        mongo_client=None,
    ) -> None:
        self.log = get_json_logger("wrc.transform")
        # Dependency-injectable for tests; defaults hit the shared config.
        self._owns_mongo = mongo_client is None and landing_collection is None
        self.mongo_client = mongo_client or get_mongo_client()
        self.landing = landing_collection or get_collection(
            settings.mongo.landing_collection, self.mongo_client
        )
        self.processed = processed_collection or get_collection(
            settings.mongo.processed_collection, self.mongo_client
        )
        ensure_indexes(self.processed)
        self.minio = minio or get_minio_client()
        ensure_bucket(self.minio, settings.minio.processed_bucket)
        self.stats = TransformStats()

    def run(
        self,
        start_date: date,
        end_date: date,
        body_ids: list[int] | None = None,
    ) -> TransformStats:
        if end_date < start_date:
            raise ValueError(f"end_date {end_date} is before start_date {start_date}")

        body_names = _validate_bodies(body_ids)
        query = self._build_query(start_date, end_date, body_names)
        cursor = self.landing.find(query).sort("partition_date")

        found = self.landing.count_documents(query)
        self.log.info(
            "transform_started",
            extra={
                "event": "transform_started",
                "start_date": start_date.isoformat(),
                "end_date": end_date.isoformat(),
                "bodies": body_names,
                "found": found,
            },
        )

        try:
            for record in cursor:
                self._process_one(record)
        finally:
            self._emit_summary()
            if self._owns_mongo:
                self.mongo_client.close()

        return self.stats

    # -- internals -----------------------------------------------------------

    def _build_query(
        self, start_date: date, end_date: date, body_names: list[str] | None
    ) -> dict:
        q: dict = {
            "partition_date": {
                "$gte": start_date.isoformat(),
                "$lte": end_date.isoformat(),
            }
        }
        if body_names:
            q["body"] = {"$in": body_names}
        return q

    def _process_one(self, record: dict) -> None:
        identifier = record.get("identifier")
        body = record.get("body")
        partition_date = record.get("partition_date")
        landing_path = record.get("file_path")
        content_type = record.get("content_type", "")
        ext = self._ext_from_content_type(content_type)

        if identifier is None or landing_path is None or ext is None:
            self._record_failed(
                identifier,
                body,
                partition_date,
                reason="incomplete_landing_metadata",
                extra={"content_type": content_type, "landing_path": landing_path},
            )
            return

        try:
            raw = self._download(landing_path)
        except S3Error as exc:
            # S3-level error: object gone, forbidden, bucket missing. Per-record
            # recoverable — log and skip. The bucket-level cases (NoSuchBucket,
            # AccessDenied) will repeat for every record, which surfaces the
            # infra issue via a burst of identical failures in the summary.
            self._record_failed(
                identifier,
                body,
                partition_date,
                reason="landing_download_failed",
                extra={
                    "landing_path": landing_path,
                    "bucket": settings.minio.landing_bucket,
                    "error": type(exc).__name__,
                    "s3_code": exc.code,
                    "message": str(exc)[:200],
                },
            )
            return

        if ext == "html":
            try:
                payload = clean_html(raw, identifier)
            except ContentNotFoundError as exc:
                self._record_failed(
                    identifier,
                    body,
                    partition_date,
                    reason="content_not_found",
                    extra={
                        "landing_path": landing_path,
                        "raw_size": len(raw),
                        "message": str(exc)[:200],
                    },
                )
                return
            outcome_kind = "transformed"
        else:
            # PDF/DOC: task rule "don't apply any transformation". Passthrough
            # bytes verbatim so the processed object is byte-identical to the
            # landing object — file_hash therefore stays the same too.
            payload = raw
            outcome_kind = "passthrough"

        new_hash = sha256_hash(payload)
        processed_path = f"{body_slug(body)}/{identifier}.{ext}"

        try:
            existing = self.processed.find_one(
                {"identifier": identifier},
                {"file_hash": 1},
            )
        except PyMongoError as exc:
            self._record_failed(
                identifier, body, partition_date,
                reason="processed_mongo_failed",
                extra=_mongo_error_extra(exc),
            )
            return

        now = datetime.now(timezone.utc)

        if existing is not None and existing.get("file_hash") == new_hash:
            try:
                self.processed.update_one(
                    {"identifier": identifier},
                    {"$set": {"last_transformed_at": now}},
                )
            except PyMongoError as exc:
                self._record_failed(
                    identifier, body, partition_date,
                    reason="processed_mongo_failed",
                    extra=_mongo_error_extra(exc),
                )
                return
            self.stats.bump(body, partition_date, "unchanged")
            self.log.info(
                "record_unchanged",
                extra={
                    "event": "record_unchanged",
                    "identifier": identifier,
                    "file_hash": new_hash,
                    "body": body,
                    "partition_date": partition_date,
                },
            )
            return

        try:
            self._upload(processed_path, payload, ext)
        except S3Error as exc:
            self._record_failed(
                identifier,
                body,
                partition_date,
                reason="processed_upload_failed",
                extra={
                    "processed_path": processed_path,
                    "bucket": settings.minio.processed_bucket,
                    "payload_size": len(payload),
                    "error": type(exc).__name__,
                    "s3_code": exc.code,
                    "message": str(exc)[:200],
                },
            )
            return

        metadata = {
            "identifier": identifier,
            "title": record.get("title"),
            "description": record.get("description"),
            "date": record.get("date"),
            "partition_date": partition_date,
            "partition_end": record.get("partition_end"),
            "body": body,
            "body_id": record.get("body_id"),
            "source_url": record.get("source_url"),
            "doc_url": record.get("doc_url"),
            "content_type": content_type,
            "file_hash": new_hash,
            "file_size": len(payload),
            "bucket": settings.minio.processed_bucket,
            "file_path": processed_path,
            "source_bucket": record.get("bucket"),
            "source_file_path": landing_path,
            "source_file_hash": record.get("file_hash"),
            "last_transformed_at": now,
            "updated_at": now,
        }
        try:
            self.processed.update_one(
                {"identifier": identifier},
                {"$set": metadata, "$setOnInsert": {"first_transformed_at": now}},
                upsert=True,
            )
        except PyMongoError as exc:
            # Upload already succeeded — the object exists in MinIO but the
            # metadata upsert failed, so the record is effectively orphaned
            # until the next successful transform run replays it. Flag the
            # orphan path so an operator can reconcile manually.
            self._record_failed(
                identifier, body, partition_date,
                reason="processed_mongo_failed",
                extra={
                    **_mongo_error_extra(exc),
                    "orphan_processed_path": processed_path,
                },
            )
            return
        self.stats.bump(body, partition_date, outcome_kind)
        self.log.info(
            "record_transformed",
            extra={
                "event": "record_transformed",
                "identifier": identifier,
                "file_path": processed_path,
                "file_hash": new_hash,
                "kind": outcome_kind,
                "body": body,
                "partition_date": partition_date,
                "size_before": len(raw),
                "size_after": len(payload),
            },
        )

    def _download(self, key: str) -> bytes:
        response = self.minio.get_object(settings.minio.landing_bucket, key)
        try:
            return response.read()
        finally:
            response.close()
            response.release_conn()

    def _upload(self, key: str, payload: bytes, ext: str) -> None:
        import io

        self.minio.put_object(
            settings.minio.processed_bucket,
            key,
            io.BytesIO(payload),
            length=len(payload),
            content_type=EXT_MIME.get(ext, "application/octet-stream"),
        )

    def _ext_from_content_type(self, content_type: str) -> str | None:
        ct = (content_type or "").split(";", 1)[0].strip().lower()
        for ext, mime in EXT_MIME.items():
            if ct == mime:
                return ext
        return None

    def _record_failed(
        self,
        identifier,
        body,
        partition_date,
        *,
        reason: str,
        extra: dict | None = None,
    ) -> None:
        self.stats.bump(body or "?", partition_date or "?", "failed")
        payload = {
            "event": "record_failed",
            "reason": reason,
            "identifier": identifier,
            "body": body,
            "partition_date": partition_date,
        }
        if extra:
            payload.update(extra)
        self.log.error("record_failed", extra=payload)

    def _emit_summary(self) -> None:
        for (body, partition_date), counts in sorted(self.stats.per_partition.items()):
            self.log.info(
                "partition_summary",
                extra={
                    "event": "partition_summary",
                    "body": body,
                    "partition_date": partition_date,
                    **counts,
                },
            )
        self.log.info(
            "transform_summary",
            extra={
                "event": "transform_summary",
                "transformed": self.stats.transformed,
                "unchanged": self.stats.unchanged,
                "passthrough": self.stats.passthrough,
                "failed": self.stats.failed,
            },
        )
