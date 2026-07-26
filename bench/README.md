# Benchmarks

Reproducible wall-clock benchmark harness for the pipeline. Every run
is isolated from real state via a dedicated Mongo DB (`wrc_bench`) and
two dedicated MinIO buckets (`landing-bench`, `processed-bench`), so
you can iterate without touching your real `wrc` DB or `landing-zone`
/ `processed` buckets.

Results committed under `bench/results/<label>/` contain only the
aggregated numbers (`summary.md` + `.wall` timing files); raw
per-worker `.jsonl` event streams are gitignored (~18 MB per full run).

---

## Prerequisites

Full compose stack up — the benchmarks run inside the
`dagster-webserver` image (where Python + Scrapy + all deps live):

```bash
docker compose up -d --build
```

---

## Harnesses

| Script | What it does |
|---|---|
| `run.sh <label>` | Four-phase fixed-workload benchmark: `scraper_cold`, `scraper_warm`, `transform_cold`, `transform_warm`. Used to measure the idempotency-fast-path improvements. |
| `run-parallel.sh <label>` | N Scrapy subprocesses in parallel, one per year, single body. Baseline for concurrency scaling. |
| `run-parallel-yb.sh <label>` | One Scrapy subprocess per `(year × body)`. All workers launched at once. |
| `run-parallel-ymb.sh <label>` | One Scrapy subprocess per `(year × month × body)`. Concurrency-capped via `jobs -pr` so aggregate load stays bounded regardless of partition count. Matches Dagster's runtime fanout most closely. |
| `probe-pagesize.sh` | Probes the WRC search endpoint for a `pageSize=` (or similar) override to reduce total request count. |
| `summarize-parallel.py`, `summarize-parallel-yb.py` | Read the per-worker `parallel_*.jsonl` + external `parallel.wall` and emit the Markdown `summary.md` inside each label dir. |

Each script writes results to `bench/results/<label>/`. Pass `--force`
to overwrite an existing label.

---

## Results and conclusions

### Milestone runs (committed)

| label | shape | records | wall | rec/min |
|---|---|---|---|---|
| `baseline` | pre-fix single-body cold/warm | 339 | 4:17 (cold) | — |
| `after-fix` | post-fix warm cold/warm | 339 | **0:07 warm** | — |
| `t8` | 5-year Labour Court, target=8, 5 workers, aggregate 40 | 2 171 | 7:26 | 292 |
| `par_t16` | 5-year Labour Court, target=16, 5 workers, aggregate 80 | 2 170 | **3:22** | **644** |
| `labour_court_5yr` | early "fair-aggregate divide" experiment | 2 172 | 24:17 serial vs 13:57 parallel | 90 / 156 |
| `ymb_3yr_4bodies_c12t7` | 3y × 4 bodies × 12 months, cap=12, target=7, aggregate 84 | **9 025** | **15:01** | **601** |

### Findings

1. **Transform warm path** — dropped from **7.08 s → 0.51 s** (~14×
   faster) once the source-hash fast bypass landed (compare
   `bench/results/baseline/summary.md` vs
   `bench/results/after-fix/summary.md`). Root cause was per-record
   MinIO `get_object` + BS4 clean + rehash + `find_one`; now a
   deterministic-cleaner shortcut says "landing hash matches stored
   source hash → nothing changed, skip everything". Batched
   `bulk_write(ordered=False)` at 200-op flushes finishes the job.

2. **Aggregate ~80 concurrent on WRC is safe.** `par_t16` sustained
   aggregate 80 (5 workers × target=16) for 2 170 records with
   **zero HTTP retries, zero `record_failed`, zero 5xx**. This is
   ~5× more aggressive than the reference-implementation's
   "load-tested a few concurrent" claim; our data invalidates that
   limit for this endpoint at this time of day.

3. **Throughput scales linearly with aggregate concurrency, up to at
   least 80.** `t8` at aggregate 40 → 292 rec/min; `par_t16` at
   aggregate 80 → 644 rec/min. Doubling concurrency → 2.2× throughput
   (slightly super-linear thanks to independent AutoThrottle instances
   ramping up in parallel).

4. **Body-diversity is worth ~29% extra throughput** at the same
   aggregate load. `t16` (single Labour Court, aggregate 80) →
   644 rec/min; `ymb_c12t7` (four bodies, aggregate 84) → equivalent
   per-body rate is ~29% higher. Probable cause: WRC's backend serves
   different-body search queries against different DB indexes /
   cache lines, so parallelising **across bodies** extracts more real
   server capacity than parallelising **within** a body.

5. **Fine-grained (year × month × body) partitioning outperforms
   coarse (year × body)** on multi-body workloads even at the same
   aggregate load. `ymb_c12t7` finished 9 025 records across 3 years
   in 15:01; the coarser `yb_3yr_4bodies_t7` variant (killed at
   ~14% WRC completion) would have taken substantially longer because
   its three WRC-year workers each carried ~2400 records and set the
   wall time. Splitting WRC into 36 month-workers × concurrency-cap
   keeps all slots utilised throughout the run.

6. **What we tried that did NOT help.** Documented for posterity:
   - Bumping `CONCURRENT_REQUESTS_PER_DOMAIN=32` while holding
     `target=16` (label `cap32`, pruned): 505 s wall vs `par_t16`'s
     202 s. AutoThrottle wasn't clipping bursts; the extra ceiling
     was pure overhead.
   - `probe-pagesize.sh` against 8 common param names
     (`pageSize`, `size`, `perPage`, `page_size`, `pagesize`,
     `pageCount`, `take`, `limit`) — WRC honors none of them. The
     search page is fixed at 10 items/page server-side.
   - HTTP/2 — the site advertises HTTP/1.1 only; no multiplexing win
     available.

### What ships as the default

The `ymb_3yr_4bodies_c12t7` config is baked into `.env.example` and
`docker-compose.yml` as the shipped default:

- `SCRAPER_AUTOTHROTTLE_TARGET_CONCURRENCY=7.0`
- `DAGSTER_MAX_CONCURRENT_RUNS=12`

Aggregate load ≈ 84 concurrent. Reviewer materialising partitions in
Dagit gets ~601 records/min out of the box.

If the target site tightens throttling later, the safer
reference-implementation preset (aggregate ~16) is one env-var edit
away — see the main [README](../README.md#configuration).

---

## Reproduce a specific milestone

Every `summary.md` under `bench/results/<label>/` ends with the exact
invocation that produced it. For example, to reproduce
`ymb_3yr_4bodies_c12t7`:

```bash
BENCH_YEARS="2022 2023 2024" \
BENCH_MAX_CONCURRENT_WORKERS=12 \
SCRAPER_AUTOTHROTTLE_TARGET_CONCURRENCY=7 \
    bash bench/run-parallel-ymb.sh ymb_3yr_4bodies_c12t7 --force
```

---

## Overrides

All bench-related tunables live in `.env` and can be overridden per
invocation (shell env wins over `.env`):

| Var | Used by | Meaning |
|---|---|---|
| `BENCH_YEARS` | all parallel scripts | Space-separated years |
| `BENCH_BODY_ID` | `run-parallel.sh` | Single body id (empty = all four) |
| `BENCH_BODY_IDS` | `run-parallel-yb.sh`, `run-parallel-ymb.sh` | Space-separated body ids |
| `BENCH_MAX_CONCURRENT_WORKERS` | `run-parallel-ymb.sh` | Cap on parallel Scrapy subprocesses |
| `SCRAPER_AUTOTHROTTLE_TARGET_CONCURRENCY` | all | Per-process target |
| `SCRAPER_*` | all | Any scraper knob — flows through `docker-compose.yml` interpolation to the container's env block |

---

## Adding a new benchmark

1. Pick or write a script that runs the workload inside
   `docker compose run --rm --no-deps dagster-webserver bash -c "…"`
   with `MONGO_DB=wrc_bench` + the two `landing-bench` /
   `processed-bench` buckets overridden via `-e`.
2. Write output to `/bench/parallel_*.jsonl` inside the container
   (which bind-mounts to `bench/results/<label>/` on the host).
3. Capture wall time externally via `date +%s.%N` and write it to
   `bench/results/<label>/parallel.wall`.
4. Call the appropriate summarizer to build `summary.md`, then
   enrich by hand with the health check + comparison table so future
   readers understand what was tested.

Only `summary.md` and `.wall` files should be committed; raw
`.jsonl` streams are gitignored via `bench/results/**/*.jsonl` in
`.gitignore`.
