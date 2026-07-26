# Architecture

Scrapy → MinIO landing → BS4 transform → MinIO processed; metadata in Mongo, orchestrated by Dagster. Run instructions: [README.md](README.md). Bench numbers: [bench/](bench/).

## Partition size

Default **monthly** (`SCRAPER_PARTITION_SIZE=monthly`, matching the Dagster `MonthlyPartitionsDefinition`); weekly and daily also implemented. Monthly amortises Scrapy subprocess startup cost across enough in-partition work that launch overhead stays a small fraction of wall-clock — daily lost proportionally more time to process launches in bench iterations. Site pagination caps at 10 records/page, so smaller partitions minimise cursor drift. Dagster's monthly partition key maps 1-to-1 to Mongo's `partition_date` column, giving a stable per-month "materialized?" checkbox. Bench harness validated the scheme at 144 monthly slices (3 y × 4 bodies × 12 months) end-to-end with zero HTTP failures at ingest (see `bench/results/ymb_3yr_4bodies_c12t7`); switch to daily for very high-volume back-fills, weekly as a middle ground.

## Retries & rate limiting

**Aggregate load kept near ~16 concurrent** — the posture our internal load testing found reliably sustainable on multi-hour crawls. Shipped defaults: `AUTOTHROTTLE_TARGET_CONCURRENCY=4` × `DAGSTER_MAX_CONCURRENT_RUNS=4`. Faster configurations (aggregate ~80+) were measurably quicker on isolated benches but drew server-side blocking on sustained runs, so we don't ship them. Hard ceiling `CONCURRENT_REQUESTS=CONCURRENT_REQUESTS_PER_DOMAIN=8` gives 2× headroom over target — bounds AutoThrottle bursts without letting effective concurrency climb back into the range that historically triggered blocking.

**AutoThrottle** (`start_delay=0.25s`, `max_delay=10s`, `target=4`) adapts per-slot delay as `latency / target_concurrency`, so it backs off automatically when WRC latency climbs.

**Retries.** `RetryMiddleware` with `RETRY_TIMES=5` and an extended `RETRY_HTTP_CODES = [403, 408, 429, 500, 502, 503, 504, 520, 521, 522, 523, 524]` — the 403 + 520–524 additions cover WAF challenges and Cloudflare origin/edge errors that would otherwise land as terminal `record_failed` events. Post-retry failures are caught by an `errback` on every `Request` and by the `item_error` signal — both emit `record_failed` JSON events with URL + status + error class, so `total_found = scraped + failed + row_parse_failed` reconciles from the log stream alone. `ROBOTSTXT_OBEY=true`.

**Anti-fingerprint hygiene** — three cheap layers that make requests look like a real browser session at the WAF layer:

- `RotatingUserAgentMiddleware` (`scrapers/middlewares.py`) — random `User-Agent` per request from a 6-string pool (Windows/macOS/Linux × Chrome/Edge/Safari/Firefox); pool overridable via `SCRAPER_USER_AGENT_POOL` (pipe-separated).
- `DEFAULT_REQUEST_HEADERS` — `Accept-Language: en-IE`, `Upgrade-Insecure-Requests`, the `Sec-Fetch-Dest/Mode/Site/User` quartet, `DNT: 1` — the shape a real browser sends on a top-level navigation.
- Cloudflare-aware `RETRY_HTTP_CODES` (above).

## Deduplication

Single `file_hash`, single Mongo unique index, canonicalised payload.

- `file_hash` = SHA-256 of the exact stored bytes; `sha256(minio_object) == file_hash` in Mongo.
- Stability from **canonicalising before storing**: WRC injects volatile server comments (`Elapsed time`, `cached or not being index.aspx`); `storage.hashing.canonicalize_html` strips them once so `put_object`, `sha256_hash`, and Mongo compare all see the same bytes. PDF/DOC pass through unchanged.
- Mongo `identifier` is a unique index; every write is `update_one(upsert=True)`. Parallel workers physically cannot create duplicates.
- Landing objects are immutable — keys `{body_slug}/{YYYY-MM}/{identifier}-{hash[:12]}.{ext}`; a real content change writes a new key.
- **Transform warm-path fast bypass**: when `landing.file_hash == processed.source_file_hash` at the current `TRANSFORM_VERSION`, we skip MinIO get + BS4 clean + rehash entirely. Bumping `TRANSFORM_VERSION` forces a one-time reprocess when the cleaner contract changes. Measured 14× warm-run speedup.
- **Pydantic validation gate** — `ProcessedRecord` validates every candidate before the upsert; failures route to `quarantine_metadata` with the reason attached, so a given identifier lives in exactly one of {processed, quarantine, neither}.

## Scaling to 50+ sources

Bodies are already configuration (`SCRAPER_BODIES`); sources need the same treatment plus platform-scale investments:

1. **Sources as a YAML/JSON registry.** Per-site list/detail selectors, pagination scheme, date-param names, rate-limit floor. One `GenericLegalSpider` reads the registry; extension modules for oddballs.
2. **Source-first storage + gold layer.** Keys `{source}/{body}/{YYYY-MM}/...`; Mongo `source` field with compound index `(source, partition_date)`; hot/warm/cold tiering by partition age. Add analytical aggregates (per-source freshness, quarantine ratio) so ops can see "which source hasn't shipped in 3 days" without touching raw metadata.
3. **Orchestration scale-out.** Partition axis becomes `(source × month)`; `QueuedRunCoordinator` moves to Postgres; a Kubernetes run launcher fans partitions across workers. Per-source concurrency caps in the registry — a small government site tolerates ~40 concurrent while a commercial API tolerates ~200; one global knob doesn't fit.
4. **Observability + SLOs.** Prometheus metrics labelled by source (`found`, `scraped`, `failed`, `quarantine_ratio`); Grafana dashboards tracking freshness and volume-delta anomalies >2σ from the trailing-30-day mean — schema drift on a source typically surfaces as a silent volume drop *before* the error rate climbs.
5. **Perf headroom for 1000× scale.** Streaming download handler for >1 MB payloads (bypass `response.body` full-buffering); batched scraper-pipeline writes via a Twisted queue once single-source throughput exceeds ~50 rec/s.
