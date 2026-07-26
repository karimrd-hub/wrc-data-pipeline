# Benchmark

Fixed-workload wall-clock benchmark used to compare pipeline versions
(baseline vs. after-fix). Default workload: **May 2025, all four bodies**.

## Prerequisites

Full compose stack must be up (the benchmark runs inside the
`dagster-webserver` image, which is where Python + Scrapy live):

```bash
docker compose up -d --build
```

## Run

```bash
bash bench/run.sh <label>              # first run
bash bench/run.sh <label> --force      # overwrite an existing label
```

`<label>` becomes a directory under `bench/results/`. The script uses a
dedicated Mongo DB (`wrc_bench`) and two dedicated MinIO buckets
(`landing-bench`, `processed-bench`) so it will **not** touch your existing
`wrc` DB or `landing-zone` / `processed` buckets.

Four phases run in sequence, each as its own `docker compose run --rm --no-deps
dagster-webserver …`:

| phase | what it measures |
|---|---|
| `scraper_cold` | first crawl, empty DB — network-bound |
| `scraper_warm` | re-crawl, everything hashes as `unchanged` — Mongo/threadpool-bound |
| `transform_cold` | first transform, empty processed collection |
| `transform_warm` | re-transform, everything hashes as `unchanged` |

Each phase writes `<phase>.jsonl` — the pipeline's JSON-line event stream
(stdout + stderr merged) — to `bench/results/<label>/`. Wall time is derived
from the first vs. last event timestamp inside the log; `summary.md` is
generated at the end.

## Override workload

```bash
BENCH_START_DATE=2025-01-01 BENCH_END_DATE=2025-01-31 \
BENCH_BODIES=3,15376 \
    bash bench/run.sh smaller
```

## Compare two labels

```bash
diff -u bench/results/baseline/summary.md bench/results/after-fix/summary.md
```
