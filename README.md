# WRC Data Pipeline

Scrapy-based ingestion + BeautifulSoup transformation pipeline for
[workplacerelations.ie](https://www.workplacerelations.ie/en/search/?advance=true)
legal decisions. Metadata is stored in MongoDB, raw and processed
documents in MinIO. Dagster orchestrates ingest → transform with an
explicit partition dependency.

Target volume: 500–1000 documents (task spec); designed for 1000× headroom
per the scaling notes in [ARCHITECTURE.md](ARCHITECTURE.md).

---

## Quick start (Docker, recommended)

Prerequisites: Docker + Docker Compose.

```bash
docker compose up --build -d
```

That boots four containers: `mongo`, `minio`, `minio-init` (creates the
buckets, exits), and `app` (Dagit + code). When the containers are healthy:

- **Dagit UI** — [http://localhost:3000](http://localhost:3000)
- **MinIO console** — [http://localhost:9001](http://localhost:9001) (login: `minioadmin` / `minioadmin`)
- **Mongo** — `mongodb://localhost:27017`

Materialize a partition from Dagit:

1. Open the **Assets** tab.
2. Select both `landing_records` and `processed_records`.
3. Click **Materialize selected**.
4. Pick a partition (any month, e.g. `2026-01-01`) and confirm.

Dagster runs the scraper first, then the transform. All the pipeline's
own log lines are JSON — you can watch them in the run log or `docker
compose logs -f app`.

To stop everything and wipe state:

```bash
docker compose down -v
```

## Direct CLI (no Dagster)

Same pipeline, invoked directly. Useful for tight iteration cycles or
when the reviewer wants to script it.

Prerequisites: Python 3.11 + [`uv`](https://docs.astral.sh/uv/), then start
just the storage services from compose:

```bash
docker compose up -d mongo minio minio-init
uv sync
```

Then either step, independently:

```bash
# Ingest — spider writes to landing bucket + landing_metadata
uv run scrapy crawl wrc \
    -a start_date=2026-01-01 -a end_date=2026-01-31 \
    -a bodies=3

# Transform — cleans landing HTML → writes to processed bucket + processed_metadata
uv run python -m wrc_pipeline.transform \
    --start-date 2026-01-01 --end-date 2026-01-31 \
    --bodies 3
```

`bodies=` is optional; omit to scrape all four (Labour Court, WRC,
Equality Tribunal, EAT). Both commands are idempotent — re-running the
same range writes zero new bytes.

## Configuration

Everything is env-driven. Copy `.env.example` to `.env` and edit if you
need to. Real shell env vars override `.env`. Shipped defaults match our
fastest validated config (`bench/results/ymb_3yr_4bodies_c12t7`,
601 rec/min at aggregate ~84); the safer reference-implementation config
(aggregate ~16) is one env-var edit — `DAGSTER_MAX_CONCURRENT_RUNS=2`,
`SCRAPER_AUTOTHROTTLE_TARGET_CONCURRENCY=4.0` — if the site tightens
throttling. See `.env.example` for the full list; the ones you're most
likely to touch:

| Var | Default | Meaning |
|---|---|---|
| `SCRAPER_PARTITION_SIZE` | `monthly` | `monthly` / `weekly` / `daily` — spider's own partition granularity (independent of Dagster). |
| `SCRAPER_CONCURRENT_REQUESTS` | `32` | Global concurrency ceiling for Scrapy. |
| `SCRAPER_AUTOTHROTTLE_ENABLED` | `true` | Safety belt — auto-backs off on 5xx or slow responses. |
| `MONGO_URI` | `mongodb://localhost:27017` | Overridden to `mongodb://mongo:27017` inside the app container. |
| `MINIO_ENDPOINT` | `localhost:9000` | Overridden to `minio:9000` inside the app container. |

## Layout

```
src/wrc_pipeline/
├── scrapers/         # Scrapy spider, item, pipeline, dates + bodies utils
├── transform/        # BS4 cleaner, transform runner, CLI
├── orchestration/    # Dagster asset definitions
├── storage/          # Mongo + MinIO clients + hashing helpers
├── config/           # Typed env-driven settings singleton
└── logging_setup.py  # JSON root logger (task req 10)
```

Design decisions and their rationales live in `docs/decisions.md`;
task requirements in `docs/task.md`.

## Verification

End-to-end smoke test (Labour Court, January 2026 = 40 records):

```bash
docker compose up -d mongo minio minio-init
uv sync

uv run scrapy crawl wrc -a start_date=2026-01-01 -a end_date=2026-01-31 -a bodies=3
uv run python -m wrc_pipeline.transform --start-date 2026-01-01 --end-date 2026-01-31 --bodies 3

# Re-run: expect all 40 unchanged, zero MinIO puts
uv run scrapy crawl wrc -a start_date=2026-01-01 -a end_date=2026-01-31 -a bodies=3
uv run python -m wrc_pipeline.transform --start-date 2026-01-01 --end-date 2026-01-31 --bodies 3
```

Unit tests (25 tests, one per task requirement plus data-quality extras):

```bash
uv sync --group dev
uv run pytest tests/
```

## Benchmarks

Reproducible benchmark harness under `bench/`, isolated from real state
via a dedicated Mongo DB (`wrc_bench`) and MinIO buckets. Each run writes
JSON logs + a `summary.md` to `bench/results/<label>/` for committable
evidence. Full docs in `bench/README.md`. Milestone results in the repo:

| label | shape | records | wall | rec/min |
|---|---|---|---|---|
| `t8` | 5yr LC only, target=8 | 2171 | 7:26 | 292 |
| `par_t16` | 5yr LC only, target=16, 5 workers | 2170 | 3:22 | 644 |
| `ymb_3yr_4bodies_c12t7` | 3yr × 4 bodies × 12 months, 144 workers cap=12, target=7 | 9025 | 15:01 | 601 |

The `ymb_*` harness (`bench/run-parallel-ymb.sh`) is the mode that
matches Dagster's fanout most closely: one Scrapy subprocess per
`(year × month × body)`, joined by `wait` inside a single
`docker compose run` invocation, with a concurrency cap so aggregate
WRC load stays bounded regardless of partition count.

## Troubleshooting

- **Docker BuildKit / uv wheel errors** — the base image is `python:3.11-slim`; if the build stalls on `lxml`, delete `.venv/` and retry with `docker compose build --no-cache app`.
- **`Address already in use` on 3000 / 27017 / 9000 / 9001** — set `DAGSTER_PORT`, `MONGO_HOST_PORT`, `MINIO_API_PORT`, `MINIO_CONSOLE_PORT` in `.env`.
- **Dagit shows 0 partitions materialized after a run** — the run log usually has the actual error; look for a `record_failed` JSON line or a non-zero exit code from the `landing_records` op.
