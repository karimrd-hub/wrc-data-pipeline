#!/usr/bin/env bash
# (year × body) parallelism benchmark. Same shape as ``run-parallel.sh``
# but fans out on both axes — one Scrapy subprocess per (year, body) pair.
#
# For 3 years × 4 bodies that's 12 workers, all launched at once inside a
# single ``docker compose run`` invocation and joined by ``wait``. Aggregate
# WRC load = 12 × SCRAPER_AUTOTHROTTLE_TARGET_CONCURRENCY, so keep the
# per-process target modest (target=7 gives aggregate ≈ 84 — close to t16).
#
# Usage: bash bench/run-parallel-yb.sh <label> [--force]
#
# All tunables in .env; shell env wins. Overrides worth knowing:
#   BENCH_YEARS      space-separated years
#   BENCH_BODY_IDS   space-separated body IDs
#   SCRAPER_AUTOTHROTTLE_TARGET_CONCURRENCY   per-process target

set -euo pipefail

LABEL=${1:-}
if [[ -z "$LABEL" ]]; then
    echo "usage: bash bench/run-parallel-yb.sh <label> [--force]" >&2
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

# Shell-wins .env load — same as run-parallel.sh.
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
: "${BENCH_BODY_IDS:?set BENCH_BODY_IDS in .env or shell env}"
: "${SCRAPER_AUTOTHROTTLE_TARGET_CONCURRENCY:?set in .env or shell env}"

read -r -a YEARS <<< "$BENCH_YEARS"
read -r -a BODY_IDS <<< "$BENCH_BODY_IDS"
START="${YEARS[0]}-01-01"
END="${YEARS[-1]}-12-31"
TARGET="$SCRAPER_AUTOTHROTTLE_TARGET_CONCURRENCY"
N_WORKERS=$(( ${#YEARS[@]} * ${#BODY_IDS[@]} ))
AGGREGATE=$(awk "BEGIN{printf \"%.1f\", $TARGET * $N_WORKERS}")

echo "workload:"
echo "  range=$START → $END"
echo "  years=${#YEARS[@]} (${YEARS[*]})"
echo "  bodies=${#BODY_IDS[@]} (${BODY_IDS[*]})"
echo "  workers=$N_WORKERS (year × body)"
echo "  per-process target=$TARGET  (aggregate ≈ $AGGREGATE)"
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

COMPOSE_RUN_BASE=(
    docker compose -f "$REPO_ROOT/docker-compose.yml" run --rm --no-deps
    -v "$OUT_DIR:/bench"
    -e MONGO_DB=wrc_bench
    -e MINIO_LANDING_BUCKET=landing-bench
    -e MINIO_PROCESSED_BUCKET=processed-bench
)

echo ">> parallel ($N_WORKERS processes, all launched at once)"
wipe_bench
par_cmd=""
for year in "${YEARS[@]}"; do
    for body_id in "${BODY_IDS[@]}"; do
        par_cmd+="scrapy crawl wrc -a start_date=${year}-01-01 -a end_date=${year}-12-31 -a bodies=$body_id > /bench/parallel_${year}_body${body_id}.jsonl 2>&1 & "
    done
done
par_cmd+="wait"

t_start=$(date +%s.%N)
"${COMPOSE_RUN_BASE[@]}" dagster-webserver bash -c "$par_cmd"
t_end=$(date +%s.%N)
awk "BEGIN{printf \"%.3f\", $t_end - $t_start}" > "$OUT_DIR/parallel.wall"
echo "   wall: $(cat "$OUT_DIR/parallel.wall") s"

echo ">> writing summary"
python3 "$REPO_ROOT/bench/summarize-parallel-yb.py" "$OUT_DIR" \
    "$START" "$END" "$TARGET" "$N_WORKERS" \
    > "$OUT_DIR/summary.md"

echo
echo "done — see $OUT_DIR/summary.md"
