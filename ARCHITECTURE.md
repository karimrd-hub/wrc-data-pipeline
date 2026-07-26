# Architecture

Scrapy → MinIO landing → BS4 transform → MinIO processed; metadata in Mongo,
orchestrated by Dagster. Run instructions and env-var reference:
[README.md](README.md). Empirical benchmark numbers: [bench/](bench/).

## Partition size

Default is **monthly** (`SCRAPER_PARTITION_SIZE=monthly`, matching the
Dagster asset). Weekly and daily are also implemented in
`scrapers/utils/dates.py:iter_partitions` and switchable via env var.
Reasons monthly is the default:

- Recon gave month-level volumes (Labour Court Jan 2026 = 40, full-year
  2025 = 361, WRC full-year 2025 ≈ 2 571). A month is small enough to
  reason about, big enough to amortise Scrapy subprocess startup cost.
- Site's visible pagination window caps at 10 records/page; smaller
  partitions minimise pagination-loop cursor drift.
- Dagster's `MonthlyPartitionsDefinition` lines up 1-to-1 with the
  `partition_date` column in Mongo, giving a stable per-month "was this
  materialized?" checkbox.
- Empirically validated at scale: `bench/results/ymb_3yr_4bodies_c12t7`
  materialised 144 monthly partitions (3 years × 4 bodies × 12 months) in
  15 min at 601 records/min, zero HTTP failures.

Switch to daily when back-filling a very high-volume body to keep
individual runs short and re-runnable; weekly is a middle ground.

## Retries & rate limiting

- **AutoThrottle** (`SCRAPER_AUTOTHROTTLE_ENABLED=true`) is the primary
  throughput lever. Start delay `0.25 s`, max `10 s`, target concurrency
  `8`; delay adapts per-slot as `latency / target_concurrency`, so it
  backs off automatically when WRC's response time climbs.
- **Fixed ceilings**: `CONCURRENT_REQUESTS=16`,
  `CONCURRENT_REQUESTS_PER_DOMAIN=16`. AutoThrottle grows into these but
  never past them.
- **Retries**: Scrapy's `RetryMiddleware` with `RETRY_TIMES=5`. Retriable
  status codes and connection errors are the built-in defaults — WRC
  runs on IIS/ASP.NET and does occasionally 5xx.
- **Post-retry failures** are caught by an `errback` on every `Request`
  and by the `item_error` signal — both emit a `record_failed` JSON
  event with URL + status + error class, so a reviewer can reconcile
  `total_found` vs `scraped + failed + row_parse_failed` from the JSON
  log alone.
- **robots.txt** is respected (`ROBOTSTXT_OBEY=true`); recon confirmed
  our targets are not disallowed.
- **Empirically validated**: `bench/results/par_t16` sustained aggregate
  80 concurrent requests (5 processes × per-process target=16) with
  **zero retries, zero `record_failed`, zero 5xx** across 2170 records —
  well past the reference-implementation's claimed "a few concurrent"
  ceiling.

## Deduplication

Single `file_hash`, single Mongo unique index, canonicalized payload.

- `file_hash` — SHA-256 of the **exact stored bytes**. Reproducible
  against the MinIO object (`sha256 <object> == file_hash` in Mongo).
- Stability across re-fetches comes from **canonicalizing the payload
  before storing it**, not from a second hash. WRC injects volatile
  server comments (`Elapsed time`, `cached or not being index.aspx page`)
  that would otherwise flip `sha256(raw_bytes)` on every request;
  `storage.hashing.canonicalize_html` strips them once, so everything
  downstream (`put_object`, `sha256_hash`, Mongo compare) sees the same
  bytes. PDF/DOC payloads are byte-stable already and pass through.
- Mongo `identifier` is a **unique index**; every write is
  `update_one({identifier: …}, upsert=True)`. Parallel workers on the
  same partition physically cannot create a duplicate.
- Landing Zone objects are **immutable**. Keys are
  `{body_slug}/{YYYY-MM}/{identifier}-{file_hash[:12]}.{ext}` — a real
  content change writes to a new key, satisfying the task requirement
  that landing bytes are never mutated. `file_path` in Mongo always
  points at the latest version.
- **Transform warm-path fast bypass**: when
  `landing.file_hash == processed.source_file_hash`, the cleaner is
  deterministic so cleaned output would be byte-identical — we skip the
  MinIO get + BS4 clean + rehash entirely. `TRANSFORM_VERSION` stamped
  on every processed row guards this: bumping it forces a one-time
  reprocess when the cleaner/extractor contract changes, then returns
  to the near-zero-cost warm path. Measured: transform warm run of 339
  records dropped from 7.08 s to 0.51 s (~14× faster) after this
  landed — see `bench/results/after-fix/`.
- **Pydantic validation gate**: every candidate row passes through
  `transform.validation.ProcessedRecord` before the upsert. Failures
  route to `quarantine_metadata` with the reason attached — a given
  identifier is in exactly one of {processed, quarantine, neither}.
- **Batched writes**: `bulk_write(ordered=False)` every
  `TRANSFORM_BULK_BATCH_SIZE=200` records. Partial-success semantics
  logged per failed op.

## Data quality (beyond task spec)

- **Plain-text sibling `{identifier}.txt`** — BS4 for HTML, `pypdf` for
  PDF, written next to the archived HTML/PDF (which stays untouched).
  Corpus becomes searchable/embeddable without re-parsing.
- **Structured fields** — `transform.extractor` pulls chair, parties,
  hearing_date, acts_cited, award_amount into
  `processed_metadata.structured`. HTML blob → queryable dataset.
- **Pydantic gate** — per-body identifier regex + `date ≤ partition_end`
  + size floor; failures route to `quarantine_metadata` (also referenced
  under Deduplication).

## What would change for 50+ sources

The current design already treats bodies as configuration
(`SCRAPER_BODIES` in `.env`). Scaling to sources needs the same
treatment plus orchestration-scale investments a working data platform
would demand:

1. **Sources as configuration, not code.** Move per-site knowledge —
   list-page selectors, detail-page selectors, pagination scheme,
   date-filter parameter names, robots hint, rate-limit floor — into a
   YAML/JSON registry. One `GenericLegalSpider` reads it; per-source
   overrides land as extension modules for pages that don't fit the
   template.
2. **Source-first storage + tiering + a gold layer.** Object keys
   become `{source}/{body_slug}/{YYYY-MM}/{identifier}-{hash}.{ext}`;
   Mongo gains a `source` field with a compound index `(source,
   partition_date)`. Hot→warm→cold object-storage tiering by partition
   age. Add a **gold layer** of analytical aggregates
   (per-source freshness, volumes, quarantine ratios) so ops can see
   "which source hasn't shipped in 3 days" without querying raw
   metadata.
3. **Orchestration scale-out.** Dagster partition axis becomes
   `(source × month)`; `QueuedRunCoordinator` moves off SQLite to
   Postgres; a Kubernetes run launcher or Celery pool fans partitions
   across workers. **Per-source concurrency caps** in the registry —
   a small government site tolerates ~40 concurrent while a commercial
   API tolerates ~200; one global knob doesn't fit.
4. **Observability + SLOs.** Prometheus metrics on `found`,
   `scraped`, `failed`, `quarantine_ratio` labelled by `source`.
   Grafana dashboard tracking freshness
   (`max(partition_date)` per source) and volume-delta anomalies >2σ
   from the trailing-30-day mean. Schema drift on a source typically
   surfaces as a silent volume drop *before* the error rate climbs, so
   volume anomaly detection is the leading indicator.
5. **Data-quality contracts per source.** The pydantic gate we ship is
   source-agnostic today; extend it to source-specific rules (this
   source's `date` is always ISO, that source's identifiers match a
   known regex). Quarantine already exists; add a `source` label so
   per-source recovery is a filtered query. Great Expectations for
   gold-layer aggregates.
6. **Perf headroom for 1000× scale.** Streaming downloads via a custom
   Twisted download handler for >1 MB payloads (bypass
   `response.body` full-buffering); batched scraper-pipeline writes via
   a Twisted queue once single-source throughput exceeds ~50 rec/s
   (transform already batches at 200-op flushes).
7. **Source-config CI/CD.** PR review each new source config
   (selector correctness, robots compliance, rate-limit floor); canary
   crawl on a small date range before merging; blue/green partition
   sets so a mid-flight selector fix doesn't retroactively invalidate
   the historical corpus.

