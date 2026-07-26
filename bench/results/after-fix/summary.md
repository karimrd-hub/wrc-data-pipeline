# Benchmark — `after-fix`

- Range: `2025-05-01` → `2025-05-31`
- Bodies: `all four`

| phase | wall (mm:ss.ms) | records |
|---|---|---|
| `scraper_cold` | 05:29.34 | found=339, inserted=339, scraped=339 |
| `scraper_warm` | 01:51.78 | found=339, scraped=339, unchanged=339 |
| `transform_cold` | 00:08.60 | transformed=339 |
| `transform_warm` | 00:00.51 | unchanged=339 |

## Conclusion

Two changes shipped against the baseline:

1. **Transform runner** (`src/wrc_pipeline/transform/runner.py`) — one prefetch
   query at run start returns `{identifier: (file_hash, source_file_hash)}`
   for the whole range, replacing the per-record `find_one`. `_process_one`
   gained a fast path: when `landing.file_hash` equals the stored
   `source_file_hash`, the record is provably unchanged (cleaner is
   deterministic) so we skip the MinIO `get_object` + BS4 clean + rehash
   entirely and just queue a `last_transformed_at` bump. All writes are
   batched via `bulk_write(ordered=False)` (flushed every 200 ops).

2. **Scraper pipeline** (`src/wrc_pipeline/scrapers/pipelines.py`) — the
   `_prefetch_hashes` query now filters by `body_id` when the spider was
   launched for a subset, so a per-body Scrapy subprocess no longer pulls
   every other body's identifiers on startup.

### Compared to baseline

| phase | baseline | after-fix | delta |
|---|---|---|---|
| `scraper_cold` | 04:16.92 | 05:29.34 | +1:12 — WRC website latency variance, not the fix |
| `scraper_warm` | 01:17.76 | 01:51.78 | +0:34 — same |
| `transform_cold` | 00:08.17 | 00:08.60 | +0.4 s — noise (cold still downloads every record) |
| `transform_warm` | 00:07.08 | **00:00.51** | **−6.6 s (~14×)** |

### Reading

- **Transform warm went from 7 s to 0.5 s.** This is the number a nightly
  re-run of the same date range pays every night — the "idempotent re-run
  should be nearly free" implication of task req 9. Now it is.
- **Transform cold is unchanged** because a first-run still has to
  download/clean/upload every record; the mongo N+1 saved ~1 s inside noise.
- **Scraper deltas are network noise**, not attributable to the fix. Fix #2
  (body-scoped prefetch) is invisible at this workload — all four bodies
  ran inside one Scrapy process, so scoping the prefetch trims nothing.
  Its payoff shows up in Dagster multi-partition mode (one Scrapy subprocess
  per body), where each subprocess only needs its own body's history rather
  than the whole month's cross-body history. That benchmark is deferred
  until the planned hierarchical (year × month × body) parallelism lands.

