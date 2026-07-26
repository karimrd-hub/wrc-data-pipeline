# Parallel benchmark — `par_t16`

- Range: `2020-01-01` → `2024-12-31`
- Body id: `3` (single body)
- Parallel workers: `5` (one per year: 2020, 2021, 2022, 2023, 2024)
- Per-process AutoThrottle target: `16` (parallel aggregate ≈ `80.0`)

| mode | wall (mm:ss.ms) | records | records/min |
|---|---|---|---|
| parallel (5 processes) | 03:22.19 | found=2172, inserted=2166, scraped=2170, updated=4 | 644.0 |

## Health check

Aggregated across all 5 workers:

- `partition_summary` totals: `found=2172, scraped=2170, inserted=2166, updated=4, unchanged=0, dropped=0, failed=0, row_parse_failed=0`
- Retry attempts logged: **0**
- `record_failed` events: **0**
- HTTP response status counts (from Scrapy stats): every worker reports **200-only** — no 429, no 5xx, no ignored responses.
- `partition_summary` events: **60** (12 months × 5 workers) — no partition dropped.
- Per-worker record split: 338 / 426 / 472 / 426 / 508 — roughly consistent with Labour Court's year-on-year volume.

The 2-record `found` vs `scraped` gap reconciles exactly with `dupefilter/filtered=1` in workers 2021 and 2023 (2 total) — same duplicate-URL / `RFPDupeFilter` interaction as prior runs, correct behaviour.

## Comparison across runs

| target/proc | aggregate | records/min | wall | notes |
|---|---|---|---|---|
| 0.80 (fair-aggregate divide) | ~4 | 156 | 13:57 | old `labour_court_5yr` — divided target for fair aggregate |
| 8   | ~40 | 292 | 07:26 | `t8` — first realistic run |
| **16** | **~80** | **644** | **03:22** | **this run** |

Roughly **2.2× throughput when doubling per-process target** — better than linear. Zero failures at aggregate ~80 concurrent means we are not at WRC's ceiling; the per-process AutoThrottle target is still the binding constraint on total throughput.

## What Scrapy saw

Aggregate downloader stats across the 5 workers:

- **Total HTTP requests**: 378 + 476 + 524 + 475 + 566 = **2419** (matches expected: 2170 detail + ~250 search pagination).
- **All 200-OK**, no retries, no dupe-filter loss beyond the 2 legitimate cross-partition duplicates.
- Aggregate download: ~269 MB / 202 s = **1.33 MB/s** — nowhere near bandwidth-bound.
- Per-worker request rate: ~2.4 req/s → ~12 req/s aggregate. Consistent with AutoThrottle at target=16 given WRC's ~5–6 s average response time.

## Workload

- Body: Labour Court (id `3`)
- Range: 2020-01-01 → 2024-12-31 (5 years × 12 months = 60 monthly partitions)
- Wall-clock: 202.189 s from external `date +%s.%N`

## Reproduce

```bash
SCRAPER_AUTOTHROTTLE_TARGET_CONCURRENCY=16 bash bench/run-parallel.sh par_t16 --force
```
