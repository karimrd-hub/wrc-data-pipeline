# Architecture

Scrapy → MinIO landing → BS4 transform → MinIO processed; metadata in Mongo, orchestrated by Dagster. Run instructions and env-var reference: [README.md](README.md). Decision log: [docs/decisions.md](docs/decisions.md).

## Partition size

Default is **monthly** (`SCRAPER_PARTITION_SIZE=monthly`, matching the
Dagster asset). Weekly and daily are also implemented in
`scrapers/utils/dates.py:iter_partitions` and switchable via env var. Reasons
for monthly as the default:

- Recon `docs/decisions.md §7.3` gave month-level volumes (Labour Court
  Jan 2026 = 40, full-year 2025 = 361, WRC full-year 2025 = 2 571). A month
  is one HTTP round-trip's worth of pagination for even the busiest body —
  small enough to reason about, big enough to amortise startup cost.
- Site date filter accepts arbitrary ranges (§7.3), but the visible page
  window caps at 10; smaller partitions minimise the pagination-loop's
  cursor drift.
- Dagster's `MonthlyPartitionsDefinition` gives us a stable per-month
  "was this materialized?" checkbox that lines up 1-to-1 with the
  `partition_date` column in Mongo.

Switch to daily when back-filling a very high-volume body (e.g. WRC ≥50 docs/day) to
keep individual runs short and re-runnable; weekly is a middle ground.

## Retries & rate limiting

- **AutoThrottle** (`SCRAPER_AUTOTHROTTLE_ENABLED=true`) is the primary
  throughput lever. Start delay 1 s, max 10 s, target concurrency 8;
  raises on quiet responses, backs off on 5xx or slow ones.
- **Fixed ceilings**: `CONCURRENT_REQUESTS=32`,
  `CONCURRENT_REQUESTS_PER_DOMAIN=16`. AutoThrottle grows into these but
  never past them.
- **Retries**: Scrapy's `RetryMiddleware` with `RETRY_TIMES=3`. Retriable
  status codes and connection errors are the built-in defaults — WRC
  runs on IIS/ASP.NET and does occasionally 5xx.
- **Post-retry failures** are caught by an `errback` on every `Request`
  and by the `item_error` signal — both emit a `record_failed` JSON
  event with URL + status + error class, so a reviewer can reconcile
  `total_found` vs `scraped + failed` from the JSON log alone.
- **robots.txt** is respected (`ROBOTSTXT_OBEY=true`); recon
  §7.6 confirmed our targets are not disallowed.

## Deduplication

Two-hash design, single Mongo unique index:

- `file_hash` — SHA-256 of the **exact stored bytes**. Reproducible
  against the MinIO object.
- `content_hash` — SHA-256 of the payload after stripping known volatile
  server markers (`Elapsed time`, `cached or not being index.aspx page`).
  Drives the skip/upload branch, so re-scraping a decision the site
  hasn't changed is a Mongo `$set` on `last_seen_at` and zero MinIO
  puts.

Mongo `landing_metadata.identifier` is a **unique index** — every write
is `update_one({identifier: …}, upsert=True)`. Even a parallel worker
racing on the same partition physically cannot create a duplicate.

Landing Zone objects are **immutable**. Keys are
`{body_slug}/{YYYY-MM}/{identifier}-{content_hash[:12]}.{ext}`, so a
real content change writes to a new key rather than overwriting the
previous bytes — required by the task tip
("don't delete/update stored data in the Landing Zone"). `file_path` in
Mongo always points at the latest version; older bytes remain
recoverable by prefix scan.

The transform step does the same trick for its collection: compare the
freshly-computed hash of the cleaned output against
`processed_metadata.file_hash`. Skip identical, upsert new.

## What would change for 50+ sources

1. **Config-driven per-source spider**. Body ids + selectors + date
   parameter naming would move from `scrapers/utils/bodies.py` and the
   hard-coded WRC CSS into a per-source YAML/JSON. One `GenericLegalSpider`
   parametrised by that config, plus per-source overrides for pages that
   don't fit the template.
2. **Split ingest by source, share transform**. Landing collection gains
   a `source` field (`wrc`, `bailii`, `curia`, …) and the object-store
   layout becomes `{source}/{body_slug}/{YYYY-MM}/…`. Transform stays
   source-agnostic because it just cleans HTML; source-specific quirks
   move into pluggable pre/post-clean hooks.
3. **Orchestrator scale-out**. Dagster's partition axis becomes
   `(source × month)`; a `dagster daemon` + separate `dagster-webserver`
   + a job queue (Kubernetes run launcher, or Celery for a lighter
   footprint) fan work out.
4. **Streaming downloads**. `response.body` is fine at 30 KB × 1 000
   docs; at 1 000× and with PDF-heavy sources (WRC serves PDFs on some
   bodies) we'd move to a custom Twisted download handler that pipes
   directly to a MinIO multipart upload — avoids buffering multi-MB
   PDFs in memory.
5. **Bulk Mongo writes**. `bulk_write([UpdateOne(...)])` batched per
   partition; per-item was chosen at 500-1000 docs because the round-trip
   cost is negligible and the three-outcome branching stays readable.
6. **Observability**. JSON logs already scrape cleanly into Loki/CloudWatch.
   Beyond that: OpenTelemetry spans around each partition; a Grafana
   board tracking found-vs-scraped-vs-failed per source × day.

## Task req 6b interpretation

"Scrape all the web pages" is read as "all detail pages across records"
(one detail-page GET per record), not "if a decision spans multiple
linked pages, follow them all". Every WRC decision detail page we
inspected is a single self-contained `<div class="content">` — there is
no next-page pagination within a decision. See
`docs/decisions.md §4.6`.
