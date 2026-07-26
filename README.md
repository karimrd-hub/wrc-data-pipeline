# WRC Data Pipeline

A Scrapy-based ingestion and BeautifulSoup transformation pipeline for
[workplacerelations.ie](https://www.workplacerelations.ie/en/search/?advance=true)
legal decisions. Metadata lands in MongoDB, raw and cleaned documents in
MinIO, orchestrated by Dagster. Every artifact and knob is reproducible
from `docker compose up`.

Target volume per the task spec: 500–1 000 documents. Designed for
1 000× that; see [ARCHITECTURE.md](ARCHITECTURE.md) for the scaling
levers and empirical validation.

---

## Highlights

- **Ingest** — Scrapy spider crawling all four WRC tribunals (Equality
  Tribunal, Employment Appeals Tribunal, Labour Court, WRC), partitioned
  by `(month × body)`. Downloads PDF/DOC as-is; HTML detail pages
  archived verbatim. Structured JSON logging with per-partition
  reconciliation (`found = scraped + failed + row_parse_failed`).
- **Transform** — Cleans HTML to relevant-content only, passes PDF
  through, renames to `identifier.ext`, writes to a separate bucket and
  collection. Ships three data-quality extras on top of the task spec:
  plain-text sibling for search/embedding, structured field extraction,
  pydantic validation gate with quarantine on failure.
- **Idempotency** — Canonicalised HTML + SHA-256 hash + Mongo unique
  index on `identifier`. Warm re-runs hit a source-hash fast path that
  skips MinIO + re-cleaning — measured **14× speedup** on the transform
  warm path.
- **Orchestration** — Dagster `MultiPartitionsDefinition({date × body})`
  with `QueuedRunCoordinator` fanning parallel Scrapy subprocesses.
  Shipped defaults sustain **~601 records/min** at aggregate ~84
  concurrent (validated: `bench/results/ymb_3yr_4bodies_c12t7`).
- **Config-driven** — Every numeric knob lives in `.env`; no hardcoded
  values in code. Ships fast, dials down easy.

---

## Project layout

```
wrc-data-pipeline/
├── src/wrc_pipeline/
│   ├── scrapers/               Scrapy project
│   │   ├── spiders/wrc.py      single-body WRC spider
│   │   ├── pipelines.py        canonicalize → hash → MinIO → Mongo
│   │   ├── items.py            WrcItem — every task-required field
│   │   └── utils/              date partitioning, body-id mapping
│   ├── transform/              landing → processed transformer
│   │   ├── cleaner.py          BS4 relevant-content extractor
│   │   ├── extractor.py        structured fields (chair, parties, acts, …)
│   │   ├── text.py             plain-text sibling (BS4 + pypdf)
│   │   ├── validation.py       pydantic gate + quarantine builder
│   │   ├── runner.py           batched-write runner, warm-path fast bypass
│   │   └── cli.py              `python -m wrc_pipeline.transform`
│   ├── orchestration/          Dagster assets + partitions
│   ├── storage/                Mongo + MinIO clients, hashing helpers
│   ├── config/settings.py      typed env-driven settings singleton
│   └── logging_setup.py        JSON root logger (task req 10)
├── tests/                      25 pytest tests, one per task req + extras
├── bench/                      benchmark harness + committed milestone results
│   ├── run.sh                  fixed-workload cold/warm benchmark
│   ├── run-parallel.sh         year-per-worker parallel harness
│   ├── run-parallel-yb.sh      (year × body)-per-worker harness
│   ├── run-parallel-ymb.sh     (year × month × body) with concurrency cap
│   ├── probe-pagesize.sh       checks WRC for a page-size override
│   ├── summarize-parallel*.py  markdown-summary generators
│   └── results/<label>/        summary.md + wall-time files
├── docker-compose.yml          mongo, minio, dagster (web + daemon), mongo-express
├── Dockerfile                  multi-stage python:3.11-slim + uv
├── dagster.yaml                QueuedRunCoordinator config
├── ARCHITECTURE.md             partition size · retries · dedup · 50+ sources
├── .env.example                every tunable + default
└── pyproject.toml              deps + pytest config
```

---

## Quick start — Docker (recommended)

Prerequisites: Docker Desktop or docker-compose v2. No `.env` editing
required for a first run.

```bash
docker compose up -d --build
```

Boots six containers:

| Container | Purpose |
|---|---|
| `wrc-mongo` | MongoDB — landing + processed + quarantine collections |
| `wrc-minio` | S3-compatible object storage |
| `wrc-minio-init` | One-shot bucket creation, exits |
| `wrc-dagster-webserver` | Dagit UI + asset execution |
| `wrc-dagster-daemon` | Dequeues + launches queued runs |
| `wrc-mongo-express` | Web browser for Mongo (dev convenience) |

Web UIs (all local):

| Service | URL | Credentials |
|---|---|---|
| Dagit (orchestrator) | http://localhost:3000 | — |
| MinIO Console | http://localhost:9001 | `minioadmin` / `minioadmin` |
| Mongo Express | http://localhost:8081 | `admin` / `admin` |

Stop and wipe all state:

```bash
docker compose down -v
```

---

## Alternative — uv (local development)

Prerequisites: Python 3.11 and [`uv`](https://docs.astral.sh/uv/).
Start only the backing services from compose and run the pipeline
directly with uv:

```bash
docker compose up -d mongo minio minio-init
uv sync
```

`SCRAPY_SETTINGS_MODULE=wrc_pipeline.scrapers.settings` is inferred
from `scrapy.cfg`; no shell exports needed.

---

## Triggering the pipeline

### Dagster UI (recommended)

1. Open http://localhost:3000
2. **Assets** tab → select `landing_records` and `processed_records`
3. Click **Materialize selected**
4. Pick a partition (e.g. `date=2024-01-01 / body=labour_court`) or a
   range, then confirm

Dagster runs the scraper subprocess first (`landing_records`), then the
transformer (`processed_records`) on the same partition — dependency
wired via `deps=[landing_records]`. Up to
`DAGSTER_MAX_CONCURRENT_RUNS=12` partitions materialize in parallel.
JSON log lines land in the Dagit run log and in
`docker compose logs -f dagster-webserver`.

### CLI (tight iteration)

Same code, invoked directly. Assumes storage services already up.

```bash
# Ingest — writes to landing_metadata + landing bucket
uv run scrapy crawl wrc \
    -a start_date=2024-01-01 -a end_date=2024-01-31 \
    -a bodies=3

# Transform — cleans HTML, extracts structured fields, quarantines invalid rows
uv run python -m wrc_pipeline.transform \
    --start-date 2024-01-01 --end-date 2024-01-31 \
    --bodies 3
```

`-a bodies=` / `--bodies` are optional; omit to scrape all four
tribunals. Both commands are idempotent — a warm re-run over the same
range writes zero new bytes.

---

## Configuration

Every tunable is env-driven. Copy `.env.example` to `.env` if you need
to override defaults; shell env wins over `.env` values.

Shipped defaults keep aggregate WRC-domain load near **~16 concurrent**
requests — the conservative posture our internal load testing settled
on after observing that more aggressive configurations (aggregate ~80+)
were faster on isolated benchmark runs but drew server-side blocking
on sustained crawls. Concretely: `AUTOTHROTTLE_TARGET_CONCURRENCY=4`
per process × `DAGSTER_MAX_CONCURRENT_RUNS=4` parallel subprocesses.
The faster configurations remain reproducible via the bench harness
for headroom testing; we simply don't ship them.

If the site tightens throttling further, roll back with:

```bash
DAGSTER_MAX_CONCURRENT_RUNS=2 SCRAPER_AUTOTHROTTLE_TARGET_CONCURRENCY=2.0 \
    docker compose up -d
```

Anti-fingerprint hygiene ships enabled by default: a
`RotatingUserAgentMiddleware` stamps every outgoing request with a
random UA from a pool of six realistic desktop-browser strings; a
browser-shaped set of default headers (`Accept-Language: en-IE`,
`Upgrade-Insecure-Requests`, the `Sec-Fetch-*` quartet, `DNT`) sits on
every request; and the retry list is extended to include 403 plus the
520–524 origin/edge-error family so WAF challenges are treated as
transient rather than terminal.

Most-touched vars:

| Var | Default | Meaning |
|---|---|---|
| `SCRAPER_PARTITION_SIZE` | `monthly` | `monthly` / `weekly` / `daily` |
| `SCRAPER_CONCURRENT_REQUESTS` | `8` | Hard ceiling on in-flight requests (2× above target — bounds AutoThrottle bursts) |
| `SCRAPER_CONCURRENT_REQUESTS_PER_DOMAIN` | `8` | Same, per-domain |
| `SCRAPER_AUTOTHROTTLE_TARGET_CONCURRENCY` | `4.0` | Per-process average concurrent requests |
| `DAGSTER_MAX_CONCURRENT_RUNS` | `4` | Parallel Scrapy subprocesses |
| `SCRAPER_USER_AGENT_POOL` | built-in 6-UA pool | Pipe-separated override for the UA rotation pool |
| `MONGO_URI` / `MINIO_ENDPOINT` | container defaults | Override to point at external services |

See [`.env.example`](.env.example) for the full annotated list.

---

## Tests

25 pytest tests. One group traces each numbered `docs/task.md`
requirement to a green/red signal; the other exercises the
data-quality extras (structured extraction, text sibling, pydantic
gate).

```bash
# via uv locally
uv sync --group dev
uv run pytest tests/

# or inside the runtime container
docker compose run --rm --no-deps \
    -v $(pwd)/tests:/app/tests -v $(pwd)/data:/app/data \
    dagster-webserver \
    bash -c "pip install pytest --quiet && cd /app && pytest tests/ -v"
```

---

## Benchmarks

Reproducible harness in [`bench/`](bench/) with committed milestone
results:

| label | shape | records | wall | rec/min |
|---|---|---|---|---|
| `t8` | 5-year Labour Court, target=8, 5 workers | 2 171 | 7:26 | 292 |
| `par_t16` | 5-year Labour Court, target=16, 5 workers | 2 170 | 3:22 | 644 |
| `ymb_3yr_4bodies_c12t7` | 3y × 4 bodies × 12 months, cap=12, target=7 | **9 025** | **15:01** | **601** |

These are historical throughput snapshots, not shipped configurations.
The `par_t16` and `ymb_3yr_4bodies_c12t7` postures produced our fastest
records/min numbers but sustained runs at aggregate ~80+ concurrent
drew server-side blocking from WRC, so we ship a much more
conservative default (aggregate ~16, see the *Configuration* section
above). Reproduce any of the bench rows by exporting the corresponding
env vars listed in each `bench/results/<label>/summary.md`.

Full commentary and conclusions: [`bench/README.md`](bench/README.md).

---

## Troubleshooting

- **`docker compose build` fails on `ghcr.io/astral-sh/uv` with a
  credential-helper error** — Docker Desktop on WSL is calling the
  Windows credential store from a non-interactive shell. Back up your
  Docker config and retry:
  ```bash
  mv ~/.docker/config.json ~/.docker/config.json.bak
  docker compose up -d --build
  ```
- **`Address already in use` on 3000 / 27017 / 9000 / 9001 / 8081** —
  set the corresponding `*_PORT` env var (see `.env.example`).
- **Dagit shows 0 partitions materialised after a run** — the run log
  has the reason; look for a `record_failed` JSON event or a non-zero
  exit code on the `landing_records` op.
- **Want to reduce load on WRC** — see the safer-dial-back preset above.
