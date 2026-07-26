"""MongoDB client + landing-collection index bootstrap.

Framework-agnostic: no Scrapy imports here so the same helpers are reusable
from the transform script and Dagster ops (decisions.md §4.5).

Index rationale (decisions.md §4.2):

* ``identifier`` unique — anchors the idempotency contract. Every upsert filters
  by identifier; a unique index makes duplicate records physically impossible
  even if two workers race on the same partition.
* ``partition_date`` — partition-scoped batch reads (transform script fetches
  by date range; future scaling-lever: per-partition identifier prefetch).
* ``file_hash`` — reverse-lookup by content, useful for dedup audits.
* ``body`` — filter by tribunal in reports/queries.
"""

from __future__ import annotations

from pymongo import ASCENDING, MongoClient
from pymongo.collection import Collection

from wrc_pipeline.config.settings import settings


# Hardcoded rather than env-driven: these are safety nets against a wedged
# server, not operational tuning knobs. Without them the driver's defaults
# turn "server unreachable" into a 30 s silent stall per operation.
_SERVER_SELECTION_TIMEOUT_MS = 5000
_CONNECT_TIMEOUT_MS = 5000
_SOCKET_TIMEOUT_MS = 30000


def get_mongo_client(uri: str | None = None) -> MongoClient:
    return MongoClient(
        uri or settings.mongo.uri,
        serverSelectionTimeoutMS=_SERVER_SELECTION_TIMEOUT_MS,
        connectTimeoutMS=_CONNECT_TIMEOUT_MS,
        socketTimeoutMS=_SOCKET_TIMEOUT_MS,
    )


def get_collection(name: str, client: MongoClient | None = None) -> Collection:
    client = client or get_mongo_client()
    return client[settings.mongo.db][name]


def ensure_indexes(collection: Collection) -> None:
    # Same index set for landing and processed — both are queried by
    # identifier (idempotency), partition_date (range scans), file_hash
    # (dedup audits), and body (per-tribunal reports).
    collection.create_index([("identifier", ASCENDING)], unique=True, name="identifier_unique")
    collection.create_index([("partition_date", ASCENDING)], name="partition_date_idx")
    collection.create_index([("file_hash", ASCENDING)], name="file_hash_idx")
    collection.create_index([("body", ASCENDING)], name="body_idx")
