"""Item pipelines: hash, upload, upsert.

Idempotency contract (task req 9, decisions.md §4.3):

* MongoDB has a **unique index on ``identifier``** and every write is an
  upsert — running the same date range twice can never create duplicate
  records.
* Change detection uses a **content fingerprint**, not the raw byte hash: WRC
  injects a volatile ``<!-- Elapsed time: X -->`` comment at the tail of every
  HTML response (probed and confirmed 2026-07-25), so raw bytes differ per
  fetch even when the decision text is unchanged. ``content_hash`` is the
  fingerprint (volatile marker stripped) and drives the skip/upload decision.
  ``file_hash`` remains SHA-256 of the exact stored bytes so a reviewer can
  reproduce it against the MinIO object.
* Three outcomes per item:
    - **inserted** — first time we've seen this identifier.
    - **updated**  — identifier known but ``content_hash`` differs; re-upload
      to a *new* fingerprint-suffixed key (the old object is left in place
      because the Landing Zone is append-only) and refresh metadata.
    - **unchanged** — content_hash matches; skip MinIO put, refresh only
      ``last_seen_at`` in Mongo (proves the re-run visited this record).

Single class — the three steps (hash, upload, upsert) share state (existing
Mongo record + hash decide whether we upload; the Mongo write needs the final
object path). Splitting into three pipelines would mean passing partial state
through the item for no benefit.
"""

from __future__ import annotations

import io
from collections import defaultdict
from datetime import datetime, timezone

from itemadapter import ItemAdapter
from scrapy import signals
from scrapy.exceptions import DropItem

from wrc_pipeline.config.settings import settings
from wrc_pipeline.logging_setup import get_json_logger
from wrc_pipeline.scrapers.utils.bodies import body_slug
from wrc_pipeline.storage.hashing import content_fingerprint, sha256_hash
from wrc_pipeline.storage.minio import ensure_bucket, get_minio_client
from wrc_pipeline.storage.mongo import ensure_landing_indexes, get_collection, get_mongo_client


# Content-Type prefix -> extension. Small & explicit; unknown types drop the
# item with a logged reason (task tip: every non-scraped record must be logged
# with a reason).
CONTENT_TYPE_EXT = {
    "text/html": "html",
    "application/pdf": "pdf",
    "application/msword": "doc",
    "application/vnd.openxmlformats-officedocument.wordprocessingml.document": "docx",
}
_EXT_MIME = {v: k for k, v in CONTENT_TYPE_EXT.items()}

_PARTITION_OUTCOMES = ("inserted", "updated", "unchanged", "dropped", "failed")


def _ext_for(content_type: str) -> str | None:
    ct = (content_type or "").split(";", 1)[0].strip().lower()
    return CONTENT_TYPE_EXT.get(ct)


def _new_partition_counter() -> dict[str, int]:
    return {k: 0 for k in _PARTITION_OUTCOMES}


class StoragePipeline:
    @classmethod
    def from_crawler(cls, crawler):
        obj = cls()
        obj.crawler = crawler
        # errback in the spider covers download-time failures; item_error covers
        # exceptions raised by *this* pipeline that aren't DropItem (DropItem is
        # already counted below via ``dropped``).
        crawler.signals.connect(obj.on_item_error, signal=signals.item_error)
        return obj

    def open_spider(self, spider):
        self.log = get_json_logger("wrc.pipeline")
        self.mongo_client = get_mongo_client()
        self.collection = get_collection(settings.mongo.landing_collection, self.mongo_client)
        ensure_landing_indexes(self.collection)
        self.minio = get_minio_client()
        ensure_bucket(self.minio, settings.minio.landing_bucket)
        self.stats = {"inserted": 0, "updated": 0, "unchanged": 0, "dropped": 0}
        # per (body, partition_date_iso) counters — populated by process_item
        # and item_error, drained at close_spider into partition_summary events.
        self.partition_stats: dict[tuple[str, str], dict[str, int]] = defaultdict(
            _new_partition_counter
        )

    def close_spider(self, spider):
        # Emit one partition_summary per (body, partition) covered by the run.
        # Union spider-side totals (records the search page said existed) with
        # pipeline-side outcomes so a reviewer can reconcile found vs scraped.
        totals: dict[tuple[str, str], int] = getattr(spider, "partition_totals", {})
        keys = set(totals) | set(self.partition_stats)
        for key in sorted(keys):
            body, partition_date = key
            counts = self.partition_stats.get(key, _new_partition_counter())
            found = totals.get(key, 0)
            scraped = counts["inserted"] + counts["updated"] + counts["unchanged"]
            self.log.info(
                "partition_summary",
                extra={
                    "event": "partition_summary",
                    "body": body,
                    "partition_date": partition_date,
                    "found": found,
                    "scraped": scraped,
                    **counts,
                },
            )

        self.log.info(
            "storage_summary",
            extra={
                "event": "storage_summary",
                "spider": spider.name,
                **self.stats,
            },
        )
        self.mongo_client.close()

    def on_item_error(self, item, response, spider, failure):
        adapter = ItemAdapter(item) if item is not None else None
        body = adapter.get("body") if adapter else None
        partition_date = adapter.get("partition_date") if adapter else None
        self._bump_partition(body, partition_date, "failed")
        self.log.error(
            "record_failed",
            extra={
                "event": "record_failed",
                "reason": "pipeline_exception",
                "url": getattr(response, "url", None),
                "error": type(failure.value).__name__ if failure else None,
                "error_message": (str(failure.value)[:300] if failure else None),
                "identifier": adapter.get("identifier") if adapter else None,
                "body": body,
                "partition_date": partition_date,
            },
        )

    def process_item(self, item, spider):
        adapter = ItemAdapter(item)
        payload: bytes = adapter.get("_body_bytes") or b""
        identifier = adapter.get("identifier")
        doc_url = adapter.get("doc_url")
        body = adapter.get("body")
        partition_date = adapter.get("partition_date")

        if not payload:
            self._record_dropped(body, partition_date, "empty_body", identifier, doc_url)
            raise DropItem(f"empty body identifier={identifier} url={doc_url}")

        ext = _ext_for(adapter.get("content_type", ""))
        if ext is None:
            self._record_dropped(
                body,
                partition_date,
                "unsupported_content_type",
                identifier,
                doc_url,
                content_type=adapter.get("content_type"),
            )
            raise DropItem(
                f"unsupported content_type={adapter.get('content_type')!r} "
                f"identifier={identifier} url={doc_url}"
            )

        file_hash = sha256_hash(payload)
        fingerprint = content_fingerprint(payload, ext)
        partition_month = partition_date[:7]  # YYYY-MM — stable bucket at any partition_size
        # Landing Zone is append-only (task tip: "don't delete/update stored
        # data in the Landing Zone"). Suffixing the fingerprint means a real
        # content change writes a new object instead of overwriting the
        # previous bytes. Same content -> same key -> idempotent no-op.
        object_path = (
            f"{body_slug(body)}/{partition_month}/"
            f"{identifier}-{fingerprint[:12]}.{ext}"
        )
        now = datetime.now(timezone.utc)

        existing = self.collection.find_one(
            {"identifier": identifier},
            {"content_hash": 1},
        )
        unchanged = existing is not None and existing.get("content_hash") == fingerprint

        if unchanged:
            self.collection.update_one(
                {"identifier": identifier},
                {"$set": {"last_seen_at": now}},
            )
            self.stats["unchanged"] += 1
            self._bump_partition(body, partition_date, "unchanged")
            self.log.info(
                "record_unchanged",
                extra={
                    "event": "record_unchanged",
                    "identifier": identifier,
                    "file_hash": file_hash,
                    "body": body,
                    "partition_date": partition_date,
                },
            )
            del item["_body_bytes"]
            adapter["file_hash"] = file_hash
            adapter["file_path"] = object_path
            return item

        self.minio.put_object(
            settings.minio.landing_bucket,
            object_path,
            io.BytesIO(payload),
            length=len(payload),
            content_type=_EXT_MIME.get(ext, "application/octet-stream"),
        )

        adapter["file_hash"] = file_hash
        adapter["file_path"] = object_path
        del item["_body_bytes"]

        metadata = {
            "identifier": identifier,
            "title": adapter.get("title"),
            "description": adapter.get("description"),
            "date": adapter.get("date"),
            "partition_date": partition_date,
            "partition_end": adapter.get("partition_end"),
            "body": body,
            "body_id": adapter.get("body_id"),
            "source_url": adapter.get("source_url"),
            "doc_url": doc_url,
            "content_type": adapter.get("content_type"),
            "scraped_at": adapter.get("scraped_at"),
            "file_hash": file_hash,
            "content_hash": fingerprint,
            "file_size": len(payload),
            "bucket": settings.minio.landing_bucket,
            "file_path": object_path,
            "last_seen_at": now,
            "updated_at": now,
        }
        self.collection.update_one(
            {"identifier": identifier},
            {"$set": metadata, "$setOnInsert": {"first_scraped_at": now}},
            upsert=True,
        )
        outcome = "inserted" if existing is None else "updated"
        self.stats[outcome] += 1
        self._bump_partition(body, partition_date, outcome)
        self.log.info(
            "record_stored",
            extra={
                "event": "record_stored",
                "identifier": identifier,
                "file_path": object_path,
                "file_hash": file_hash,
                "change": "new" if existing is None else "content_changed",
                "body": body,
                "partition_date": partition_date,
            },
        )
        return item

    # -- helpers -------------------------------------------------------------

    def _bump_partition(self, body, partition_date, outcome: str) -> None:
        if body is None or partition_date is None:
            return
        self.partition_stats[(body, partition_date)][outcome] += 1

    def _record_dropped(
        self,
        body,
        partition_date,
        reason: str,
        identifier,
        url,
        **extra,
    ) -> None:
        self.stats["dropped"] += 1
        self._bump_partition(body, partition_date, "dropped")
        self.log.warning(
            "record_dropped",
            extra={
                "event": "record_dropped",
                "reason": reason,
                "identifier": identifier,
                "url": url,
                "body": body,
                "partition_date": partition_date,
                **extra,
            },
        )
