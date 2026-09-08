"""Item pipelines: canonicalize, hash, upload, upsert.

Idempotency contract (task req 9):

* MongoDB has a **unique index on ``identifier``** and every write is an
  upsert — running the same date range twice can never create duplicate
  records.
* Change detection uses a single ``file_hash`` per task req 9's literal
  wording. To make that hash stable across re-fetches we **canonicalize the
  payload before it lands** — WRC injects volatile server comments
  (``<!-- Elapsed time -->``, ``<!-- cached or not being index.aspx page -->``)
  that drift per request; ``storage.hashing.canonicalize_html`` strips them
  before both the ``put_object`` and the ``sha256_hash`` call. Result:
  ``sha256(<object-in-minio>) == file_hash`` in Mongo, and re-runs of an
  unchanged decision produce byte-identical objects → identical hash → skip.
* Three outcomes per item:
    - **inserted** — first time we've seen this identifier.
    - **updated**  — identifier known but ``file_hash`` differs; re-upload
      to a *new* hash-suffixed key (the old object is left in place because
      the Landing Zone is append-only) and refresh metadata.
    - **unchanged** — hash matches; skip MinIO put, refresh only
      ``last_seen_at`` in Mongo (proves the re-run visited this record).

* An in-memory ``{identifier: file_hash}`` map is prefetched once in
  ``open_spider`` over the spider's requested date range. Change detection is a
  dict lookup instead of a sequential ``find_one`` per item; at 400 items
  that's 400 fewer Mongo round-trips.

Single class — the three steps (canonicalize+hash, upload, upsert) share
state (existing hash decides whether we upload; the Mongo write needs the
final object path). Splitting into three pipelines would mean passing partial
state through the item for no benefit.
"""

from __future__ import annotations

import io
from collections import defaultdict
from datetime import datetime, timezone

from itemadapter import ItemAdapter
from minio.error import S3Error
from pymongo.errors import PyMongoError
from scrapy import signals
from scrapy.exceptions import DropItem

from wrc_pipeline.config.settings import settings
from wrc_pipeline.logging_setup import get_json_logger
from wrc_pipeline.scrapers.utils.bodies import body_slug
from wrc_pipeline.storage.hashing import canonicalize_html, sha256_hash
from wrc_pipeline.storage.minio import ensure_bucket, get_minio_client
from wrc_pipeline.storage.mongo import ensure_indexes, ensure_oig_indexes, get_collection, get_mongo_client


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

# row_parse_failed is a spider-side counter merged into partition_summary at
# close_spider time — it's not one of the outcomes this pipeline itself owns.
_PARTITION_OUTCOMES = ("inserted", "updated", "unchanged", "dropped", "failed")

# for the hash cache: distinguishes "identifier never seen" from
# "identifier known but file_hash column is None" (legacy records written
# before canonicalization landed).
_MISSING = object()


# Say the server returns Content-Type: text/html; charset=utf-8:
def _ext_for(content_type: str) -> str | None:
    ct = (content_type or "").split(";", 1)[0].strip().lower()
    return CONTENT_TYPE_EXT.get(ct)


def _new_partition_counter() -> dict[str, int]:
    return {k: 0 for k in _PARTITION_OUTCOMES}


def _classify_pipeline_exception(exc: BaseException | None) -> tuple[str, dict]:
    """Map an uncaught pipeline exception to a stable ``reason`` label plus
    any driver-specific fields worth surfacing in the log.

    Kept explicit (per-driver) instead of a generic fallback: a reviewer
    filtering ``record_failed`` events by reason wants to know at a glance
    whether MinIO or Mongo was the culprit, without opening the traceback.
    """
    if isinstance(exc, S3Error):
        return "minio_write_failed", {"s3_code": exc.code}
    if isinstance(exc, PyMongoError):
        return "mongo_write_failed", {}
    return "pipeline_exception", {}

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
    async def open_spider(self, spider):
        self.log = get_json_logger("wrc.pipeline") #  All log lines from this pipeline will have "logger": "wrc.pipeline"
        self.mongo_client = get_mongo_client()
        self.collection = get_collection(settings.mongo.landing_collection, self.mongo_client)
        await ensure_indexes(self.collection)
        self.minio = get_minio_client()
        ensure_bucket(self.minio, settings.minio.landing_bucket)
        self.stats = {"inserted": 0, "updated": 0, "unchanged": 0, "dropped": 0} # global total across the entire crawl {"event": "storage_summary", "inserted": 142, "updated": 3, "unchanged": 891, "dropped": 2}

        # per (body, partition_date_iso) counters — populated by process_item
        # and item_error, drained at close_spider into partition_summary events.

        # {"event": "partition_summary", "body": "Labour Court", "partition_date": "2024-01-01", "inserted": 12, ...}
        # {"event": "partition_summary", "body": "Labour Court", "partition_date": "2024-02-01", "inserted": 8, ...}
        self.partition_stats: dict[tuple[str, str], dict[str, int]] = defaultdict(
            _new_partition_counter
        )
        # One Mongo query per crawl instead of one ``find_one`` per item.
        # Assumes ``partition_date`` tagged on existing records overlaps the
        # spider's requested range — true whenever Dagster and the spider
        # agree on partition granularity (see ``SCRAPER_PARTITION_SIZE``).
        # prefetch {identifier → file_hash} pairs from Mongo so that per-item change detection (based on file_hash) is a dict lookup instead of a Mongo query per item.
        self._hash_cache: dict[str, str | None] = await self._prefetch_hashes(spider)
        self.log.info(
            "hash_cache_prefetched",
            extra={
                "event": "hash_cache_prefetched",
                "records": len(self._hash_cache),
                "range_start": spider.range_start.isoformat(),
                "range_end": spider.range_end.isoformat(),
            },
        )

    async def _prefetch_hashes(self, spider) -> dict[str, str | None]:
        start = spider.range_start.isoformat()
        end = spider.range_end.isoformat()
        query: dict = {"partition_date": {"$gte": start, "$lte": end}}
        # Scope by body when the spider was launched for a subset. In the
        # Dagster per-body-subprocess mode every run is a single body, so
        # this trims the prefetch by up to Nbodies× — significant once the
        # landing collection has been backfilled across multiple bodies.
        body_ids = getattr(spider, "body_ids", None)
        if body_ids:
            query["body_id"] = {"$in": list(body_ids)}
        cursor = self.collection.find(
            query,
            {"identifier": 1, "file_hash": 1, "_id": 0},
        )
        return {doc["identifier"]: doc.get("file_hash") async for doc in cursor}

    async def close_spider(self, spider):
        # Emit one partition_summary per (body, partition) covered by the run.
        # Union spider-side totals (records the search page said existed) with
        # pipeline-side outcomes so a reviewer can reconcile found vs scraped.
        totals: dict[tuple[str, str], int] = getattr(spider, "partition_totals", {})
        # something wrong in parse_search
        row_failures: dict[tuple[str, str], int] = getattr(
            spider, "partition_row_failures", {}
        )
        # Spider-side HTTP-level failures (post-retry errbacks). Merged into
        # ``failed`` so the found/scraped/failed equation reconciles from
        # partition_summary alone; keys also enter the union so partitions
        # whose first search-page died still appear in the summary stream
        # (otherwise they'd vanish, having populated neither totals nor stats).
        http_failures: dict[tuple[str, str], int] = getattr(
            spider, "partition_http_failures", {}
        )
    # produces a set of keys body: partition    keys = {("Labour Court", "2024-01-01"), ("WRC", "2024-01-01"), ("Labour Court", "2024-02-01")}
        keys = (
            set(totals)
            | set(self.partition_stats)
            | set(row_failures)
            | set(http_failures)
        )
        for key in sorted(keys):
            body, partition_date = key
            # returns {"inserted": 10, "updated": 1, "unchanged": 0, "dropped": 0, "failed": 0} 
            counts = self.partition_stats.get(key, _new_partition_counter()).copy()
            # # counts is now: {"inserted": 10, "updated": 1, "unchanged": 0, "dropped": 0, "failed": 0, "row_parse_failed": 1}
            counts["row_parse_failed"] = row_failures.get(key, 0)
            # counts["failed"] already tracks pipeline-level failures such errors during MinIO upload or Mongo upsert
            counts["failed"] += http_failures.get(key, 0)
            found = totals.get(key, 0)
            scraped = counts["inserted"] + counts["updated"] + counts["unchanged"]
            self.log.info(
                # for each partition (one month)
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
        await self.mongo_client.close()

    def on_item_error(self, item, response, spider, failure):
        adapter = ItemAdapter(item) if item is not None else None
        body = adapter.get("body") if adapter else None
        partition_date = adapter.get("partition_date") if adapter else None
        exc = failure.value if failure else None
        reason, extra_fields = _classify_pipeline_exception(exc)
        self._bump_partition(body, partition_date, "failed")
        self.log.error(
            "record_failed",
            extra={
                "event": "record_failed",
                "reason": reason,
                "url": getattr(response, "url", None),
                "error": type(exc).__name__ if exc is not None else None,
                "error_message": str(exc)[:300] if exc is not None else None,
                "identifier": adapter.get("identifier") if adapter else None,
                "body": body,
                "partition_date": partition_date,
                **extra_fields,
            },
        )

    async def process_item(self, item, spider):
        adapter = ItemAdapter(item)
        payload: bytes = adapter.get("_body_bytes") or b""
        identifier = adapter.get("identifier")
        doc_url = adapter.get("doc_url")
        body = adapter.get("body")
        partition_date = adapter.get("partition_date")

        if not payload:
            self._record_dropped(body, partition_date, "empty_body", identifier, doc_url)
            raise DropItem(f"empty body identifier={identifier} url={doc_url}") # signal

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

        # Canonicalize HTML *before* hashing/storing so file_hash tracks the
        # stable decision content, not per-request server jitter. PDF/DOC
        # bodies are byte-stable already — pass through untouched.
        stored_payload = canonicalize_html(payload) if ext == "html" else payload
        file_hash = sha256_hash(stored_payload)
        partition_month = partition_date[:7]  # YYYY-MM — stable bucket at any partition_size
        # Landing Zone is append-only (task tip: "don't delete/update stored
        # data in the Landing Zone"). Suffixing file_hash means a real content
        # change writes a new object instead of overwriting the previous
        # bytes. Same content -> same key -> idempotent no-op.
        # produces labour_court/2024-01/TED2616-a3f9b7c2d1e4.html
        object_path = (
            f"{body_slug(body)}/{partition_month}/"
            f"{identifier}-{file_hash[:12]}.{ext}"
        )
        now = datetime.now(timezone.utc)

        # Prefetched cache — dict lookup instead of a per-item find_one round-trip.
        # ``None`` marks "identifier known but file_hash never stored" (rare —
        # legacy pre-canonicalization records); treat as changed so we re-upload.
        cached_hash = self._hash_cache.get(identifier, _MISSING)
        existed = cached_hash is not _MISSING
        unchanged = existed and cached_hash == file_hash

        if unchanged:
            await self.collection.update_one(
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

        # executes for both inserted (new) and updated (changed hash)
        self.minio.put_object(
            settings.minio.landing_bucket,
            object_path,
            io.BytesIO(stored_payload),
            length=len(stored_payload),
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
            "file_size": len(stored_payload),
            "bucket": settings.minio.landing_bucket,
            "file_path": object_path,
            "last_seen_at": now,
            "updated_at": now,
        }
        await self.collection.update_one(
            {"identifier": identifier},
            # $setOnInsert only updates the field first_scraped_at when the item is inserted for first time
            {"$set": metadata, "$setOnInsert": {"first_scraped_at": now}},
            upsert=True,
        )
        outcome = "inserted" if not existed else "updated"
        self.stats[outcome] += 1
        self._bump_partition(body, partition_date, outcome)
        self._hash_cache[identifier] = file_hash
        self.log.info(
            "record_stored",
            extra={
                "event": "record_stored",
                "identifier": identifier,
                "file_path": object_path,
                "file_hash": file_hash,
                "change": "new" if not existed else "content_changed",
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


class OigStoragePipeline:
    """Hash + MinIO upload + MongoDB upsert for OIG advisory opinions.

    Each opinion can carry multiple PDFs (original + modification/termination
    documents). All of them are stored; _pdf_bytes[i] corresponds to
    documents[i]. A None entry means that PDF download failed — the document
    metadata is still recorded in Mongo but file_path / file_hash are null.

    Unique index on opinion_id provides idempotency: re-running the same
    year overwrites metadata and re-uploads only changed files.
    """

    async def open_spider(self, spider):
        self.log = get_json_logger("oig.pipeline")
        self.mongo_client = get_mongo_client()
        self.collection = get_collection(
            settings.mongo.oig_landing_collection, self.mongo_client
        )
        await ensure_oig_indexes(self.collection)
        self.minio = get_minio_client()
        ensure_bucket(self.minio, settings.minio.landing_bucket)
        self.stats = {"stored": 0, "failed": 0}

    async def close_spider(self, spider):
        self.log.info(
            "oig_storage_summary",
            extra={
                "event": "oig_storage_summary",
                "year": spider.year,
                "total_found": spider.total_found,
                "http_failures": spider.http_failures,
                **self.stats,
            },
        )
        await self.mongo_client.close()

    async def process_item(self, item, spider):
        from wrc_pipeline.scrapers.items import OigItem
        if not isinstance(item, OigItem):
            return item

        opinion_id = item["opinion_id"]
        year = item["year"]
        documents = item.get("documents") or []
        pdf_bytes_list = item.get("_pdf_bytes") or []
        now = datetime.now(timezone.utc)

        stored_documents = []
        for i, doc_meta in enumerate(documents):
            pdf_bytes = pdf_bytes_list[i] if i < len(pdf_bytes_list) else None
            if pdf_bytes is None:
                stored_documents.append(
                    {**doc_meta, "file_path": None, "file_hash": None, "file_size": None}
                )
                continue

            filename = doc_meta["url"].rstrip("/").split("/")[-1]
            file_hash = sha256_hash(pdf_bytes)
            object_path = f"oig/{year}/{opinion_id}/{filename}"

            try:
                self.minio.put_object(
                    settings.minio.landing_bucket,
                    object_path,
                    io.BytesIO(pdf_bytes),
                    length=len(pdf_bytes),
                    content_type="application/pdf",
                )
            except S3Error as exc:
                self.log.error(
                    "pdf_upload_failed",
                    extra={
                        "event": "pdf_upload_failed",
                        "opinion_id": opinion_id,
                        "object_path": object_path,
                        "s3_code": exc.code,
                        "error_message": str(exc)[:200],
                    },
                )
                stored_documents.append(
                    {**doc_meta, "file_path": None, "file_hash": None, "file_size": None}
                )
                continue

            stored_documents.append(
                {
                    **doc_meta,
                    "file_path": object_path,
                    "file_hash": file_hash,
                    "file_size": len(pdf_bytes),
                }
            )

        metadata = {
            "opinion_id": opinion_id,
            "year": year,
            "status": item.get("status"),
            "outcome": item.get("outcome"),
            "posted_date": item.get("posted_date"),
            "last_updated": item.get("last_updated"),
            "summary": item.get("summary"),
            "detail_url": item.get("detail_url"),
            "documents": stored_documents,
            "updates": item.get("updates"),
            "scraped_at": item.get("scraped_at"),
            "bucket": settings.minio.landing_bucket,
            "last_seen_at": now,
            "updated_at": now,
        }

        try:
            await self.collection.update_one(
                {"opinion_id": opinion_id},
                {"$set": metadata, "$setOnInsert": {"first_scraped_at": now}},
                upsert=True,
            )
        except PyMongoError as exc:
            self.stats["failed"] += 1
            self.log.error(
                "opinion_upsert_failed",
                extra={
                    "event": "opinion_upsert_failed",
                    "opinion_id": opinion_id,
                    "error": type(exc).__name__,
                    "error_message": str(exc)[:200],
                },
            )
            return item

        self.stats["stored"] += 1
        self.log.info(
            "opinion_stored",
            extra={
                "event": "opinion_stored",
                "opinion_id": opinion_id,
                "year": year,
                "status": item.get("status"),
                "outcome": item.get("outcome"),
                "documents_count": len(stored_documents),
                "bucket": settings.minio.landing_bucket,
            },
        )
        return item
