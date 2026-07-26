# Parallel benchmark — `labour_court_5yr`

- Range: `2020-01-01` → `2024-12-31`
- Body id: `3` (single body)
- Parallel workers: `5` (one per year: 2020, 2021, 2022, 2023, 2024)
- AutoThrottle target — serial: `4.0`, per-parallel-process: `0.80` (so aggregate WRC load ≈ same as serial)

| mode | wall (mm:ss.ms) | records |
|---|---|---|
| serial (1 process) | 24:16.76 | found=2172, inserted=2167, scraped=2172, updated=5 |
| parallel (5 processes) | 13:56.88 | found=2172, inserted=2167, scraped=2171, updated=4 |

**Speedup: 1.74×** (serial wall / parallel wall)

Record-count reconciliation: MISMATCH — serial=2172, parallel=2171.
