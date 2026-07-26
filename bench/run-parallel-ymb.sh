#!/usr/bin/env bash
# (year × month × body) parallelism benchmark — finest partition grain.
#
# Total workers = years × 12 months × bodies. For 3 × 12 × 4 = 144 partitions.
# Can't launch 144 at once (would OOM + DoS WRC), so a bash job-control
# concurrency cap runs at most BENCH_MAX_CONCURRENT_WORKERS in parallel.
#
# Aggregate WRC load = cap × SCRAPER_AUTOTHROTTLE_TARGET_CONCURRENCY.
# Keep the product ≈ 80 (t16's proven-safe number).
#
# Usage: bash bench/run-parallel-ymb.sh <label> [--force]

set -euo pipefail

LABEL=${1:-}
if [[ -z "$LABEL" ]]; then
    echo "usage: bash bench/run-parallel-ymb.sh <label> [--force]" >&2
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
rm -f "$OUT_DIR"/*.jsonl "$OUT_DIR"/*.wall "$OUT_DIR"/summary.md "$OUT_DIR/.commands.txt"

# Shell-wins .env load — same pattern as the other run-parallel scripts.
if [[ -f "$REPO_ROOT/.env" ]]; then
    while IFS= read -r line; do
        [[ "$line" =~ ^[[:space:]]*# ]] && continue
        [[ -z "$line" ]] && continue
        [[ "$line" != *=* ]] && continue
        key="${line%%=*}"; key="${key//[[:space:]]/}"
        value="${line#*=}"
        [[ -z "${!key+x}" ]] && export "$key=$value"
    done < "$REPO_ROOT/.env"
fi

: "${BENCH_YEARS:?set BENCH_YEARS in .env or shell env}"
: "${BENCH_BODY_IDS:?set BENCH_BODY_IDS in .env or shell env}"
: "${BENCH_MAX_CONCURRENT_WORKERS:?set BENCH_MAX_CONCURRENT_WORKERS in .env or shell env}"
: "${SCRAPER_AUTOTHROTTLE_TARGET_CONCURRENCY:?set in .env or shell env}"

read -r -a YEARS <<< "$BENCH_YEARS"
read -r -a BODY_IDS <<< "$BENCH_BODY_IDS"
TARGET="$SCRAPER_AUTOTHROTTLE_TARGET_CONCURRENCY"
CAP="$BENCH_MAX_CONCURRENT_WORKERS"
N_TOTAL=$(( ${#YEARS[@]} * 12 * ${#BODY_IDS[@]} ))
AGGREGATE=$(awk "BEGIN{printf \"%.1f\", $TARGET * $CAP}")

echo "workload:"
echo "  years=${#YEARS[@]} (${YEARS[*]})  months=12  bodies=${#BODY_IDS[@]} (${BODY_IDS[*]})"
echo "  total partitions=$N_TOTAL  concurrency cap=$CAP"
echo "  per-process target=$TARGET  capped aggregate ≈ $AGGREGATE"
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

wipe_bench

# Generate a command per (year, month, body). The runner inside the
# container iterates this file with a bash job-control cap so at most CAP
# Scrapy subprocesses are alive at once.
CMD_FILE="$OUT_DIR/.commands.txt"
: > "$CMD_FILE"
for year in "${YEARS[@]}"; do
    for m in 01 02 03 04 05 06 07 08 09 10 11 12; do
        last=$(date -d "${year}-${m}-01 +1 month -1 day" +%d)
        for body_id in "${BODY_IDS[@]}"; do
            log="parallel_${year}-${m}_body${body_id}.jsonl"
            echo "scrapy crawl wrc -a start_date=${year}-${m}-01 -a end_date=${year}-${m}-${last} -a bodies=${body_id} > /bench/${log} 2>&1" >> "$CMD_FILE"
        done
    done
done

echo ">> launching $N_TOTAL workers (cap=$CAP concurrent)"
t_start=$(date +%s.%N)
docker compose -f "$REPO_ROOT/docker-compose.yml" run --rm --no-deps \
    -v "$OUT_DIR:/bench" \
    -e MONGO_DB=wrc_bench \
    -e MINIO_LANDING_BUCKET=landing-bench \
    -e MINIO_PROCESSED_BUCKET=processed-bench \
    dagster-webserver bash -c "
max=$CAP
while IFS= read -r cmd; do
    while (( \$(jobs -pr | wc -l) >= \$max )); do
        wait -n
    done
    eval \"\$cmd\" &
done < /bench/.commands.txt
wait
"
t_end=$(date +%s.%N)
awk "BEGIN{printf \"%.3f\", $t_end - $t_start}" > "$OUT_DIR/parallel.wall"
echo "   wall: $(cat "$OUT_DIR/parallel.wall") s"
rm -f "$CMD_FILE"

echo ">> writing summary"
START="${YEARS[0]}-01-01"
END="${YEARS[-1]}-12-31"
python3 "$REPO_ROOT/bench/summarize-parallel-yb.py" "$OUT_DIR" \
    "$START" "$END" "$TARGET" "$N_TOTAL" \
    > "$OUT_DIR/summary.md"

echo
echo "done — see $OUT_DIR/summary.md"
