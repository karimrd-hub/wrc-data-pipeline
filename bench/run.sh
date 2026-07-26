#!/usr/bin/env bash
# Fixed-workload benchmark for the WRC pipeline.
#
# Each phase runs inside the already-built ``dagster-webserver`` image via
# ``docker compose run --rm --no-deps`` — so we get Python + Scrapy + the
# venv from the container, and reach mongo/minio via the compose network.
# The bench output directory is bind-mounted at /bench so JSON logs and the
# summary land on the host and can be committed to git.
#
# Phases (all against a dedicated ``wrc_bench`` DB + ``landing-bench`` /
# ``processed-bench`` buckets so real data is untouched):
#
#   1. scraper_cold   — first crawl, empty DB
#   2. scraper_warm   — re-crawl, everything hashes as ``unchanged``
#   3. transform_cold — first transform, empty processed collection
#   4. transform_warm — re-transform, everything hashes as ``unchanged``
#
# Usage: bash bench/run.sh <label> [--force]

set -euo pipefail

LABEL=${1:-}
if [[ -z "$LABEL" ]]; then
    echo "usage: bash bench/run.sh <label> [--force]" >&2
    echo "  results written to bench/results/<label>/" >&2
    exit 2
fi

FORCE=""
if [[ "${2:-}" == "--force" ]]; then
    FORCE=1
fi

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
OUT_DIR="$REPO_ROOT/bench/results/$LABEL"

if [[ -d "$OUT_DIR" && -z "$FORCE" ]]; then
    echo "error: $OUT_DIR already exists; pass --force to overwrite" >&2
    exit 2
fi
mkdir -p "$OUT_DIR"
# Wipe stale files from a prior --force run so summarize.py doesn't see them.
rm -f "$OUT_DIR"/*.jsonl "$OUT_DIR"/summary.md

START_DATE=${BENCH_START_DATE:-2025-05-01}
END_DATE=${BENCH_END_DATE:-2025-05-31}
BODIES=${BENCH_BODIES:-}  # empty = all four

echo ">> wiping bench DB and buckets so 'cold' is really cold"
docker exec wrc-mongo mongosh --quiet --eval \
    'db.getSiblingDB("wrc_bench").dropDatabase()' >/dev/null
docker exec wrc-minio sh -c '
    mc alias set local http://localhost:9000 minioadmin minioadmin >/dev/null
    for b in landing-bench processed-bench; do
        mc rb --force "local/$b" >/dev/null 2>&1 || true
        mc mb "local/$b" >/dev/null
    done
'

# Base ``compose run`` invocation reused for every phase:
#  --rm         — auto-remove the container after each run
#  --no-deps    — don't start mongo/minio again, they're already up
#  -v …         — bind-mount host results dir so logs persist
#  -e …         — override MONGO_DB + bucket names for the bench
COMPOSE_RUN=(
    docker compose -f "$REPO_ROOT/docker-compose.yml" run --rm --no-deps
    -v "$OUT_DIR:/bench"
    -e MONGO_DB=wrc_bench
    -e MINIO_LANDING_BUCKET=landing-bench
    -e MINIO_PROCESSED_BUCKET=processed-bench
    dagster-webserver
)

BODIES_SCRAPY=""
BODIES_TRANSFORM=""
if [[ -n "$BODIES" ]]; then
    BODIES_SCRAPY="-a bodies=$BODIES"
    BODIES_TRANSFORM="--bodies $BODIES"
fi

run_phase () {
    local name="$1"
    local cmd="$2"
    echo ">> $name"
    "${COMPOSE_RUN[@]}" bash -c "$cmd > /bench/$name.jsonl 2>&1"
}

run_phase scraper_cold \
    "scrapy crawl wrc -a start_date=$START_DATE -a end_date=$END_DATE $BODIES_SCRAPY"
run_phase scraper_warm \
    "scrapy crawl wrc -a start_date=$START_DATE -a end_date=$END_DATE $BODIES_SCRAPY"
run_phase transform_cold \
    "python -m wrc_pipeline.transform --start-date $START_DATE --end-date $END_DATE $BODIES_TRANSFORM"
run_phase transform_warm \
    "python -m wrc_pipeline.transform --start-date $START_DATE --end-date $END_DATE $BODIES_TRANSFORM"

echo ">> writing summary"
python3 "$REPO_ROOT/bench/summarize.py" "$OUT_DIR" \
    "$START_DATE" "$END_DATE" "$BODIES" > "$OUT_DIR/summary.md"

echo
echo "done — see $OUT_DIR/summary.md"
