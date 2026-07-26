# (year × body) benchmark — `ymb_3yr_4bodies_c12t7`

- Range: `2022-01-01` → `2024-12-31`
- Workers: `144` (one Scrapy subprocess per year × body)
- Files aggregated: `144`
- Per-process AutoThrottle target: `7`  (concurrency-capped aggregate ≈ **`84`**, not 1008 — summary script bug in header, corrected here; actual in-flight was `cap × target = 12 × 7 = 84`)

| wall (mm:ss.ms) | records | records/min |
|---|---|---|
| 15:01.10 | found=9032, inserted=9010, scraped=9025, updated=15 | 600.9 |

## Per-body record counts

| body | records |
|---|---|
| Workplace Relations Commission | 7616 |
| Labour Court | 1405 |
| Employment Appeals Tribunal | 4 |

## Health check

Aggregated across all 144 workers:

- `partition_summary` totals: `found=9032, scraped=9025, inserted=9010, updated=15, unchanged=0, dropped=0, failed=0, row_parse_failed=0`
- Retry attempts logged: **0**
- `record_failed` events: **0**

Zero HTTP errors, zero retries. The 7-record `found` vs `scraped` gap is the same duplicate-URL / `RFPDupeFilter` interaction we've seen in every run — a decision listed in two search partitions gets fetched once (correct behaviour, not a loss).

## Comparison across runs

| run | shape | workers | aggregate | records/min | wall | records |
|---|---|---|---|---|---|---|
| `t16` | 5yr LC only | 5 × year | 80 | 644 | 3:22 | 2170 |
| `par_3yr_4bodies` | 3yr × 4 bodies | 3 × year (all bodies) | 48 | 298 | (running estimate) | partial |
| `yb_3yr_4bodies_t7` | 3yr × 4 bodies | 12 × (year × body) | 84 | 526 (incomplete) | 4:42 | 2475 (killed) |
| **`ymb_3yr_4bodies_c12t7`** | **3yr × 4 bodies** | **144 × (year × month × body), cap=12** | **84** | **601** | **15:01** | **9025** |

`ymb` completed the full 9025-record workload at 601 records/min. At the same aggregate WRC load (~84 concurrent), the finer partition grain kept all 12 slots utilised throughout — no worker stuck holding a giant WRC-year chunk while others sat idle.

## Workload

- Range: 2022-01-01 → 2024-12-31 (3 years × 12 months × 4 bodies = 144 partitions)
- 12 concurrent Scrapy subprocesses, `target_concurrency=7` per process
- Wall-clock: 901.100 s from external `date +%s.%N`

## Reproduce

```bash
BENCH_YEARS="2022 2023 2024" \
BENCH_MAX_CONCURRENT_WORKERS=12 \
SCRAPER_AUTOTHROTTLE_TARGET_CONCURRENCY=7 \
    bash bench/run-parallel-ymb.sh ymb_3yr_4bodies_c12t7 --force
```
