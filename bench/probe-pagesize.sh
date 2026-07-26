#!/usr/bin/env bash
# Probe whether WRC's search page honours a page-size override, and if so,
# which query-parameter name unlocks it. Baseline is 10 items per page; if
# any of these params returns more, we found the knob and can trim
# search-page GETs from ~250 to ~25 across a 5-year run.
#
# Usage: bash bench/probe-pagesize.sh

set -euo pipefail

# Fixed URL: Labour Court, full 2020 — big enough that a wider page size
# would show up as substantially more items in the response.
BASE="https://www.workplacerelations.ie/en/search/?decisions=1&body=3&pageNumber=1&from=2020-01-01&to=2020-12-31"

count_items () {
    # Count matches of ``<li class="each-item">`` — each row on the search
    # page. Tolerant to whitespace and attribute reordering.
    grep -cE '<li[^>]*class="[^"]*each-item[^"]*"' || true
}

echo "baseline (no override):"
n=$(curl -sSL "$BASE" | count_items)
echo "  $n items"
echo

for param in pageSize size perPage page_size pagesize pageCount take limit; do
    url="${BASE}&${param}=100"
    n=$(curl -sSL "$url" | count_items)
    marker=""
    if [[ "$n" -gt 10 ]]; then
        marker="  ← accepts override"
    fi
    printf '  %-12s : %3d items%s\n' "$param=100" "$n" "$marker"
done

echo
echo "if any row above shows >10, that's the param name to wire in."
