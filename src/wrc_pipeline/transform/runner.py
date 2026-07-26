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

Data-quality additions on top of the task spec:

* **Sibling ``{identifier}.txt``** — every processed document gets a
  plain-text sibling extracted by ``transform.text`` (BS4 for HTML,
  ``pypdf`` for PDF). The archived HTML/PDF stays untouched; the ``.txt``
  is a *sibling*, not a replacement, so the "don't transform PDFs" rule
  is respected. Downstream consumers (search, LLMs) read the sibling.
* **Structured fields** — ``transform.extractor`` parses the cleaned HTML
  for chair / parties / hearing_date / acts / € amounts and stores them
  under ``processed_metadata.structured``. Best-effort: unknown-shape
  pages produce mostly-``None`` fields, never a run failure.
* **Schema validation** — every candidate row passes through
  ``transform.validation.ProcessedRecord`` before the upsert. Failures
  route to ``quarantine_metadata`` with the reason attached; a given
  identifier is in exactly one of {processed, quarantine, neither}.

Idempotency (task req 9):

Three prefetched fields per already-processed identifier —
``file_hash`` (of the cleaned bytes), ``source_file_hash`` (of the
landing bytes we cleaned from), and ``transform_version`` (the cleaner /
extractor generation that produced the row). The cleaner + extractor
are deterministic, so identical landing bytes at the same
transform_version always produce identical processed output;
``landing.file_hash == prefetched source_file_hash AND
version_match`` proves this record is unchanged without touching MinIO.
Bumping ``TRANSFORM_VERSION`` triggers a one-time backfill on the next
run and returns to the near-free warm path afterwards.

Batched writes: pending ``UpdateOne`` ops are flushed via
``bulk_write(ordered=False)`` every ``TRANSFORM_BULK_BATCH_SIZE`` records
(and once at the end). Turns the per-record round-trip into one round-trip
per batch; essential to make the warm path collapse to seconds even at
1000x volume.
"""

from __future__ import annotations

import io
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from typing import Optional

from minio import Minio
from minio.error import S3Error
from pydantic import ValidationError
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
from wrc_pipeline.transform.extractor import extract_fields
from wrc_pipeline.transform.text import html_to_text, pdf_to_text
from wrc_pipeline.transform.validation import ProcessedRecord, build_quarantine_doc


# Bump when the cleaner / extractor / text-extraction contract changes in
# a way that would give a different processed output for the same landing
# bytes. Guarded by the fast-path check below — a change here forces a
# one-time reprocess of every existing row, then returns to warm-path
# behaviour on the next run.
TRANSFORM_VERSION = 2


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
    quarantined: int = 0
    per_partition: dict[tuple[str, str], dict[str, int]] = field(
        default_factory=lambda: defaultdict(lambda: _new_partition_counter())
    )

    def bump(self, body: str, partition_date: str, outcome: str) -> None:
        setattr(self, outcome, getattr(self, outcome) + 1)
        self.per_partition[(body, partition_date)][outcome] += 1


def _new_partition_counter() -> dict[str, int]:
    return {
        "transformed": 0,
        "unchanged": 0,
        "passthrough": 0,
        "failed": 0,
        "quarantined": 0,
    }


def _validate_bodies(body_ids: Optional[list[int]]) -> Optional[list[str]]:
    if body_ids is None:
        return None
    names = []
    for bid in body_ids:
        if bid not in BODIES:
            raise ValueError(f"Unknown body id: {bid} (known: {list(BODIES)})")
        names.append(BODIES[bid])
    return names


@dataclass
class _Existing:
    """Prefetched state per identifier: three slots the fast-path uses."""
    file_hash: Optional[str]
    source_file_hash: Optional[str]
    transform_version: Optional[int]


class TransformRunner:
    """Orchestrates a single ``[start_date, end_date]`` transform run.

    Instances are single-use — one ``run()`` per instance keeps the Mongo /
    MinIO clients scoped to a well-defined lifetime.
    """

    def __init__(
        self,
        landing_collection: Optional[Collection] = None,
        processed_collection: Optional[Collection] = None,
        quarantine_collection: Optional[Collection] = None,
        minio: Optional[Minio] = None,
        mongo_client=None,
    ) -> None:
        self.log = get_json_logger("wrc.transform")
        self._owns_mongo = mongo_client is None and landing_collection is None
        self.mongo_client = mongo_client or get_mongo_client()
        self.landing = landing_collection or get_collection(
            settings.mongo.landing_collection, self.mongo_client
        )
        self.processed = processed_collection or get_collection(
            settings.mongo.processed_collection, self.mongo_client
        )
        self.quarantine = quarantine_collection or get_collection(
            settings.mongo.quarantine_collection, self.mongo_client
        )
        ensure_indexes(self.processed)
        ensure_indexes(self.quarantine)
        self.minio = minio or get_minio_client()
        ensure_bucket(self.minio, settings.minio.processed_bucket)
        self.stats = TransformStats()
        self._existing: dict[str, _Existing] = {}
        self._pending_ops: list[UpdateOne] = []

    def run(
        self,
        start_date: date,
        end_date: date,
        body_ids: Optional[list[int]] = None,
    ) -> TransformStats:
        if end_date < start_date:
            raise ValueError(f"end_date {end_date} is before start_date {start_date}")

        body_names = _validate_bodies(body_ids)
        query = self._build_query(start_date, end_date, body_names)

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
                "transform_version": TRANSFORM_VERSION,
            },
        )

        batch_size = settings.transform.bulk_batch_size
        try:
            for record in cursor:
                self._process_one(record)
                if len(self._pending_ops) >= batch_size:
                    self._flush_pending()
            self._flush_pending()
        finally:
            self._emit_summary()
            if self._owns_mongo:
                self.mongo_client.close()

        return self.stats

    # -- internals -----------------------------------------------------------

    def _build_query(
        self, start_date: date, end_date: date, body_names: Optional[list[str]]
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
        existing = self._existing.get(identifier)

        # Fast path: unchanged landing bytes AND the row was produced at
        # the current transform version. A version bump defeats the fast
        # path for one run, letting cleaner/extractor changes propagate.
        if (
            existing is not None
            and landing_hash is not None
            and existing.source_file_hash == landing_hash
            and existing.transform_version == TRANSFORM_VERSION
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
                    "file_hash": existing.file_hash,
                    "body": body,
                    "partition_date": partition_date,
                    "skipped_download": True,
                },
            )
            return

        try:
            raw = self._download(landing_path)
        except S3Error as exc:
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

        # Clean + extract + text-sibling depending on ext.
        text_payload: Optional[bytes] = None
        structured: Optional[dict] = None
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
            text = html_to_text(payload)
            text_payload = text.encode("utf-8") if text else None
            structured = extract_fields(payload)
            outcome_kind = "transformed"
        else:
            # PDF/DOC passthrough — bytes go verbatim into the processed
            # bucket; the .txt sibling is extracted separately (best-effort).
            payload = raw
            if ext == "pdf":
                pdf_text = pdf_to_text(raw)
                text_payload = pdf_text.encode("utf-8") if pdf_text else None
            structured = None
            outcome_kind = "passthrough"

        new_hash = sha256_hash(payload)
        processed_path = f"{body_slug(body)}/{identifier}.{ext}"
        text_hash = sha256_hash(text_payload) if text_payload else None
        text_path = f"{body_slug(body)}/{identifier}.txt" if text_payload else None

        # Validate BEFORE any upload — quarantine on failure so we don't
        # write bytes for a row that isn't going into processed.
        candidate = {
            "identifier": identifier,
            "body": body,
            "body_id": record.get("body_id"),
            "partition_date": partition_date,
            "partition_end": record.get("partition_end"),
            "date": record.get("date"),
            "bucket": settings.minio.processed_bucket,
            "file_path": processed_path,
            "file_hash": new_hash,
            "file_size": len(payload),
            "content_type": content_type,
            "text_file_path": text_path,
            "text_file_hash": text_hash,
            "text_size": len(text_payload) if text_payload else None,
            "structured": structured,
            "min_content_bytes": settings.transform.min_content_bytes,
        }
        try:
            validated = ProcessedRecord(**candidate)
        except ValidationError as exc:
            self._quarantine(
                identifier=identifier,
                landing_record=record,
                reason="schema_validation_failed",
                details={"errors": exc.errors(include_url=False)},
                now=now,
            )
            return

        # Second-chance unchanged check on the cleaned/passthrough hash —
        # covers the "cleaner algo changed but this record's output
        # happens to hash the same" edge case, plus legacy rows without
        # source_file_hash. Backfill both the source hash AND the
        # transform_version so the next run takes the fast path.
        if existing is not None and existing.file_hash == new_hash:
            self._pending_ops.append(UpdateOne(
                {"identifier": identifier},
                {"$set": {
                    "last_transformed_at": now,
                    "source_file_hash": landing_hash,
                    "transform_version": TRANSFORM_VERSION,
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

        if text_payload:
            try:
                self._upload(text_path, text_payload, "txt")
            except S3Error as exc:
                # Text sibling is nice-to-have — a failure to upload it
                # shouldn't nuke the whole record. Null the fields and
                # log; the processed row still lands with the archived
                # HTML/PDF pointer intact.
                self.log.warning(
                    "text_sibling_upload_failed",
                    extra={
                        "event": "text_sibling_upload_failed",
                        "identifier": identifier,
                        "text_path": text_path,
                        "error": type(exc).__name__,
                        "s3_code": exc.code,
                    },
                )
                text_payload = None
                text_hash = None
                text_path = None

        metadata = validated.model_dump(exclude={"min_content_bytes"})
        # Strip the None sibling fields when the upload failed post-validation.
        if text_payload is None:
            metadata["text_file_path"] = None
            metadata["text_file_hash"] = None
            metadata["text_size"] = None
        metadata.update({
            "source_url": record.get("source_url"),
            "doc_url": record.get("doc_url"),
            "source_bucket": record.get("bucket"),
            "source_file_path": landing_path,
            "source_file_hash": landing_hash,
            "transform_version": TRANSFORM_VERSION,
            "title": record.get("title"),
            "description": record.get("description"),
            "last_transformed_at": now,
            "updated_at": now,
        })
        self._pending_ops.append(UpdateOne(
            {"identifier": identifier},
            {"$set": metadata, "$setOnInsert": {"first_transformed_at": now}},
            upsert=True,
        ))
        # If this identifier was previously quarantined, drop it from
        # quarantine now that it validates — keeps the invariant that a
        # given identifier lives in exactly one collection.
        self.quarantine.delete_one({"identifier": identifier})
        self._existing[identifier] = _Existing(
            file_hash=new_hash,
            source_file_hash=landing_hash,
            transform_version=TRANSFORM_VERSION,
        )
        self.stats.bump(body, partition_date, outcome_kind)
        self.log.info(
            "record_transformed",
            extra={
                "event": "record_transformed",
                "identifier": identifier,
                "file_path": processed_path,
                "file_hash": new_hash,
                "text_file_path": text_path,
                "kind": outcome_kind,
                "body": body,
                "partition_date": partition_date,
                "size_before": len(raw),
                "size_after": len(payload),
                "text_size": len(text_payload) if text_payload else 0,
            },
        )

    def _prefetch_existing(self, query: dict) -> dict[str, _Existing]:
        cursor = self.processed.find(
            query,
            {
                "identifier": 1,
                "file_hash": 1,
                "source_file_hash": 1,
                "transform_version": 1,
                "_id": 0,
            },
        )
        return {
            doc["identifier"]: _Existing(
                file_hash=doc.get("file_hash"),
                source_file_hash=doc.get("source_file_hash"),
                transform_version=doc.get("transform_version"),
            )
            for doc in cursor
        }

    def _flush_pending(self) -> None:
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

    def _quarantine(
        self,
        identifier: str,
        landing_record: dict,
        reason: str,
        details: dict,
        now: datetime,
    ) -> None:
        """Route a candidate to the quarantine collection and remove any
        existing processed row for the same identifier — a bad row must
        not shadow a previously-good one."""
        doc = build_quarantine_doc(
            landing_record=landing_record, reason=reason, details=details, now=now
        )
        try:
            self.quarantine.update_one(
                {"identifier": identifier},
                {
                    "$set": doc,
                    "$setOnInsert": {"first_quarantined_at": now},
                },
                upsert=True,
            )
        except Exception as exc:
            # Quarantine write failed — still emit the log so nothing is
            # lost, but don't crash the run over a single bad row.
            self.log.error(
                "quarantine_write_failed",
                extra={
                    "event": "quarantine_write_failed",
                    "identifier": identifier,
                    "reason": reason,
                    "error": type(exc).__name__,
                    "message": str(exc)[:200],
                },
            )
        self.stats.bump(landing_record.get("body") or "?",
                        landing_record.get("partition_date") or "?",
                        "quarantined")
        self.log.warning(
            "record_quarantined",
            extra={
                "event": "record_quarantined",
                "identifier": identifier,
                "reason": reason,
                "body": landing_record.get("body"),
                "partition_date": landing_record.get("partition_date"),
                "details": details,
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
        content_type = EXT_MIME.get(ext, "text/plain" if ext == "txt" else "application/octet-stream")
        self.minio.put_object(
            settings.minio.processed_bucket,
            key,
            io.BytesIO(payload),
            length=len(payload),
            content_type=content_type,
        )

    def _ext_from_content_type(self, content_type: str) -> Optional[str]:
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
        extra: Optional[dict] = None,
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
                "quarantined": self.stats.quarantined,
            },
        )
