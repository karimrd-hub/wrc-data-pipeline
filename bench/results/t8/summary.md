# Parallel benchmark — `t8`

- Range: `2020-01-01` → `2024-12-31`
- Body id: `3` (single body)
- Parallel workers: `5` (one per year: 2020, 2021, 2022, 2023, 2024)
- Per-process AutoThrottle target: `8` (parallel aggregate ≈ `40.0`)

| mode | wall (mm:ss.ms) | records | records/min |
|---|---|---|---|
| parallel (5 processes) | 07:26.01 | found=2172, inserted=2167, scraped=2171, updated=4 | 292.1 |

## Health check

Aggregated across all 5 workers:

- `partition_summary` totals: `found=2172, scraped=2171, inserted=2167, updated=4, unchanged=0, dropped=0, failed=0, row_parse_failed=0`
- Retry attempts logged (Scrapy `RetryMiddleware`): **0**
- `record_failed` events: **0**

No 429s, no 5xx retries, no per-record drops — aggregate load of ~40 concurrent
requests on WRC held up cleanly. The 1-record `found` vs `scraped` gap is
the same duplicate-URL / `RFPDupeFilter` interaction we saw at
`target=0.80` (see `bench/results/labour_court_5yr/summary.md`): a decision
listed under two search partitions gets fetched once, not twice, in the same
worker process. Correct behaviour, not a loss.

## Context

- Prior data point at `target=0.80 per process` (aggregate ≈ 4) hit **156 records/min**
  (see `bench/results/labour_court_5yr/summary.md`).
- This run at `target=8 per process` (aggregate ≈ 40) hits **292 records/min** — ~1.9× higher.
- Next data point: `t16` (aggregate ≈ 80) will show whether we're bounded by
  our per-process ceiling or by WRC's server capacity.

## Workload

- Body: Labour Court (id `3`)
- Range: 2020-01-01 → 2024-12-31 (5 years × 12 months = 60 monthly partitions)
- Wall-clock: 446.008 s from external `date +%s.%N`

## Reproduce

```bash
SCRAPER_AUTOTHROTTLE_TARGET_CONCURRENCY=8 bash bench/run-parallel.sh t8 --force
```
