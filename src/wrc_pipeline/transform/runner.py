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
two prefetched fields per already-processed identifier — ``file_hash`` (of
the cleaned bytes) and ``source_file_hash`` (of the landing bytes we cleaned
from). The cleaner is deterministic, so identical landing bytes always
produce identical cleaned bytes; ``landing.file_hash == prefetched
source_file_hash`` proves this record is unchanged without touching MinIO.
That fast path is what makes a warm re-run near-free at reference volume.
A second-chance check on the cleaned-hash catches "cleaner algo changed"
and legacy records missing ``source_file_hash``.

Batched writes: pending ``UpdateOne`` ops are flushed via
``bulk_write(ordered=False)`` every ``_BULK_BATCH_SIZE`` records (and once
at the end). Turns the per-record round-trip into one round-trip per batch;
essential to make the warm path collapse to seconds even at 1000x volume.
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from typing import Iterable

from minio import Minio
from minio.error import S3Error
from pymongo import UpdateOne
from pymongo.collection import Collection
from pymongo.errors import BulkWriteError

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

# Flush pending UpdateOne ops when this many are queued. 200 keeps peak
# memory bounded while still amortizing round-trip cost across many records.
_BULK_BATCH_SIZE = 200


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
        # Populated by ``run`` → ``_prefetch_existing``. Maps
        # ``identifier`` → (processed file_hash, processed source_file_hash).
        # Lets ``_process_one`` skip both the per-record find_one AND — when
        # source hashes match — the MinIO get_object entirely.
        self._existing: dict[str, tuple[str | None, str | None]] = {}
        # Queued UpdateOne ops flushed via ``bulk_write(ordered=False)``.
        self._pending_ops: list[UpdateOne] = []

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

        # Same range/body filter as the landing cursor below — so the prefetch
        # only pulls processed rows the run could actually touch.
        self._existing = self._prefetch_existing(query)

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
                "existing_processed": len(self._existing),
            },
        )

        try:
            for record in cursor:
                self._process_one(record)
                if len(self._pending_ops) >= _BULK_BATCH_SIZE:
                    self._flush_pending()
            self._flush_pending()
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
        landing_hash = record.get("file_hash")
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

        now = datetime.now(timezone.utc)
        existing = self._existing.get(identifier)  # (file_hash, source_file_hash) or None

        # Fast path: landing hash matches the stored source_file_hash.
        # Cleaner is deterministic → cleaned bytes would be byte-identical →
        # processed hash would match too. Skip the MinIO get_object + BS4
        # parse + re-hash entirely. This is the near-zero cost warm re-run.
        if (
            existing is not None
            and landing_hash is not None
            and existing[1] == landing_hash
        ):
            self._pending_ops.append(UpdateOne(
                {"identifier": identifier},
                {"$set": {"last_transformed_at": now}},
            ))
            self.stats.bump(body, partition_date, "unchanged")
            self.log.info(
                "record_unchanged",
                extra={
                    "event": "record_unchanged",
                    "identifier": identifier,
                    "file_hash": existing[0],
                    "body": body,
                    "partition_date": partition_date,
                    "skipped_download": True,
                },
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

        # Second-chance unchanged check: source hash differed (or was missing)
        # but the cleaned bytes still hash the same — legacy record without
        # source_file_hash, or a cleaner-algo change that landed on the same
        # output. Skip the put; backfill source_file_hash so next warm run
        # takes the fast path.
        if existing is not None and existing[0] == new_hash:
            self._pending_ops.append(UpdateOne(
                {"identifier": identifier},
                {"$set": {
                    "last_transformed_at": now,
                    "source_file_hash": landing_hash,
                }},
            ))
            self.stats.bump(body, partition_date, "unchanged")
            self.log.info(
                "record_unchanged",
                extra={
                    "event": "record_unchanged",
                    "identifier": identifier,
                    "file_hash": new_hash,
                    "body": body,
                    "partition_date": partition_date,
                    "skipped_download": False,
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
            "source_file_hash": landing_hash,
            "last_transformed_at": now,
            "updated_at": now,
        }
        self._pending_ops.append(UpdateOne(
            {"identifier": identifier},
            {"$set": metadata, "$setOnInsert": {"first_transformed_at": now}},
            upsert=True,
        ))
        # Keep the in-memory prefetch consistent so a duplicate identifier
        # later in the same run (shouldn't happen but be safe) takes the
        # right branch.
        self._existing[identifier] = (new_hash, landing_hash)
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

    def _prefetch_existing(
        self, query: dict
    ) -> dict[str, tuple[str | None, str | None]]:
        """One query for the whole run: ``{identifier: (file_hash, source_file_hash)}``.

        Same partition-date + optional body filter as the landing cursor, so
        we only pull rows the run could actually match against. Replaces the
        per-record ``find_one`` that dominated the warm path at N=339.
        """
        cursor = self.processed.find(
            query,
            {"identifier": 1, "file_hash": 1, "source_file_hash": 1, "_id": 0},
        )
        return {
            doc["identifier"]: (doc.get("file_hash"), doc.get("source_file_hash"))
            for doc in cursor
        }

    def _flush_pending(self) -> None:
        """Flush queued ``UpdateOne`` ops via ``bulk_write(ordered=False)``.

        Partial-success semantics: ``ordered=False`` means Mongo applies
        every op it can and reports the rest via ``BulkWriteError``. We log
        each per-op write error as ``record_failed`` (without decrementing
        stats — the discrepancy between summary counts and log events makes
        the failure visible). Non-bulk driver errors propagate.
        """
        if not self._pending_ops:
            return
        ops = self._pending_ops
        self._pending_ops = []
        try:
            self.processed.bulk_write(ops, ordered=False)
        except BulkWriteError as exc:
            for err in exc.details.get("writeErrors", []):
                self.log.error(
                    "record_failed",
                    extra={
                        "event": "record_failed",
                        "reason": "processed_bulk_write_failed",
                        "index": err.get("index"),
                        "code": err.get("code"),
                        "message": (err.get("errmsg") or "")[:200],
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
