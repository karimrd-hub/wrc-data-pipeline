#!/usr/bin/env bash
# Parallelism benchmark: does the shipped architecture actually scale when
# you fan (year × body) partitions out into multiple Scrapy subprocesses?
#
# Runs the same single-body workload two ways against the dedicated
# wrc_bench DB / landing-bench + processed-bench buckets:
#
#   serial   — one Scrapy process crawls the whole range.
#   parallel — N Scrapy subprocesses (one per year) run in parallel inside a
#              single ``docker compose run`` invocation, joined by ``wait``.
#              Mirrors what Dagster's QueuedRunCoordinator would spawn at the
#              (year × body)-in-parallel level, without Dagster per-run overhead.
#
# Same per-process AutoThrottle target for both modes. Aggregate WRC load in
# parallel = target × N_workers. Wall time captured with ``date +%s.%N``.
#
# Usage: bash bench/run-parallel.sh <label> [--force]
#
# All tunables live in ``.env``:
#   BENCH_BODY_ID        which body to crawl (single body)
#   BENCH_YEARS          space-separated years (one worker per year)
#   SCRAPER_*            the same knobs the pipeline uses in production
#
# Shell env wins over .env. Example — sweep two targets sequentially:
#   SCRAPER_AUTOTHROTTLE_TARGET_CONCURRENCY=8  bash bench/run-parallel.sh t8  --force
#   SCRAPER_AUTOTHROTTLE_TARGET_CONCURRENCY=16 bash bench/run-parallel.sh t16 --force

set -euo pipefail

LABEL=${1:-}
if [[ -z "$LABEL" ]]; then
    echo "usage: bash bench/run-parallel.sh <label> [--force]" >&2
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
rm -f "$OUT_DIR"/*.jsonl "$OUT_DIR"/*.wall "$OUT_DIR"/summary.md

# Load .env for any var not already set in the shell (shell wins). Needed
# both for BENCH_* (read by this script) and for SCRAPER_* (docker compose
# will interpolate them into the container's environment block).
if [[ -f "$REPO_ROOT/.env" ]]; then
    while IFS= read -r line; do
        [[ "$line" =~ ^[[:space:]]*# ]] && continue
        [[ -z "$line" ]] && continue
        [[ "$line" != *=* ]] && continue
        key="${line%%=*}"
        key="${key//[[:space:]]/}"
        value="${line#*=}"
        if [[ -z "${!key+x}" ]]; then
            export "$key=$value"
        fi
    done < "$REPO_ROOT/.env"
fi

: "${BENCH_YEARS:?set BENCH_YEARS in .env or shell env}"
: "${SCRAPER_AUTOTHROTTLE_TARGET_CONCURRENCY:?set in .env or shell env}"
# BENCH_BODY_ID is optional — unset or empty means "all four bodies".
: "${BENCH_BODY_ID=}"

read -r -a YEARS <<< "$BENCH_YEARS"
START="${YEARS[0]}-01-01"
END="${YEARS[-1]}-12-31"
TARGET="$SCRAPER_AUTOTHROTTLE_TARGET_CONCURRENCY"
BODY_LABEL="${BENCH_BODY_ID:-all four}"

echo "workload:"
echo "  body_id=$BODY_LABEL  range=$START → $END  years=${#YEARS[@]} (${YEARS[*]})"
echo "  per-process autothrottle target=$TARGET"
echo

wipe_bench () {
    docker exec wrc-mongo mongosh --quiet --eval \
        'db.getSiblingDB("wrc_bench").dropDatabase()' >/dev/null
    docker exec wrc-minio sh -c '
        mc alias set local http://localhost:9000 minioadmin minioadmin >/dev/null
        for b in landing-bench processed-bench; do
            mc rb --force "local/$b" >/dev/null 2>&1 || true
            mc mb "local/$b" >/dev/null
        done
    '
}

# Only override the bench-specific DB/bucket names via -e. SCRAPER_* flows
# through compose's ${VAR:-default} interpolation from the exported shell env
# (which we just seeded from .env, with shell winning).
COMPOSE_RUN_BASE=(
    docker compose -f "$REPO_ROOT/docker-compose.yml" run --rm --no-deps
    -v "$OUT_DIR:/bench"
    -e MONGO_DB=wrc_bench
    -e MINIO_LANDING_BUCKET=landing-bench
    -e MINIO_PROCESSED_BUCKET=processed-bench
)

echo ">> parallel (${#YEARS[@]} processes)"
wipe_bench
BODIES_ARG=""
if [[ -n "$BENCH_BODY_ID" ]]; then
    BODIES_ARG="-a bodies=$BENCH_BODY_ID"
fi
par_cmd=""
for year in "${YEARS[@]}"; do
    par_cmd+="scrapy crawl wrc -a start_date=${year}-01-01 -a end_date=${year}-12-31 $BODIES_ARG > /bench/parallel_${year}.jsonl 2>&1 & "
done
par_cmd+="wait"

t_start=$(date +%s.%N)
"${COMPOSE_RUN_BASE[@]}" dagster-webserver bash -c "$par_cmd"
t_end=$(date +%s.%N)
awk "BEGIN{printf \"%.3f\", $t_end - $t_start}" > "$OUT_DIR/parallel.wall"
echo "   wall: $(cat "$OUT_DIR/parallel.wall") s"

echo ">> writing summary"
python3 "$REPO_ROOT/bench/summarize-parallel.py" "$OUT_DIR" \
    "$START" "$END" "$BODY_LABEL" "$TARGET" "${YEARS[@]}" \
    > "$OUT_DIR/summary.md"

echo
echo "done — see $OUT_DIR/summary.md"
