#!/usr/bin/env python3
"""Summarize a parallel-mode benchmark into a Markdown table.

Reads the two ``.wall`` files (external timings written by ``run-parallel.sh``)
plus the ``serial.jsonl`` / ``parallel_YYYY.jsonl`` logs to aggregate record
counts from every ``partition_summary`` event across the workers.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path


def _iter_events(path: Path):
    if not path.exists():
        return
    for line in path.read_text(errors="replace").splitlines():
        line = line.strip()
        if not line.startswith("{"):
            continue
        try:
            yield json.loads(line)
        except json.JSONDecodeError:
            continue


def summed_counts(paths: list[Path]) -> dict[str, int]:
    totals: dict[str, int] = {}
    for path in paths:
        for obj in _iter_events(path):
            if obj.get("event") != "partition_summary":
                continue
            for k, v in obj.items():
                if isinstance(v, bool) or k == "body_id":
                    continue
                if isinstance(v, (int, float)):
                    totals[k] = totals.get(k, 0) + int(v)
    return totals


def _read_wall_seconds(path: Path) -> float | None:
    if not path.exists():
        return None
    try:
        return float(path.read_text().strip())
    except ValueError:
        return None


def _fmt_wall(seconds: float | None) -> str:
    if seconds is None:
        return "?"
    m, s = divmod(seconds, 60)
    return f"{int(m):02d}:{s:05.2f}"


def _counts_cell(counts: dict[str, int]) -> str:
    if not counts:
        return "—"
    return ", ".join(f"{k}={v}" for k, v in sorted(counts.items()) if v)


def main() -> int:
    out_dir = Path(sys.argv[1])
    start = sys.argv[2]
    end = sys.argv[3]
    body_id = sys.argv[4]
    target = sys.argv[5]
    years = sys.argv[6:]
    par_agg = float(target) * len(years) if years else float(target)

    print(f"# Parallel benchmark — `{out_dir.name}`")
    print()
    print(f"- Range: `{start}` → `{end}`")
    print(f"- Body id: `{body_id}` (single body)")
    print(f"- Parallel workers: `{len(years)}` (one per year: {', '.join(years)})")
    print(
        f"- Per-process AutoThrottle target: `{target}` "
        f"(parallel aggregate ≈ `{par_agg:.1f}`)"
    )
    print()
    par_wall = _read_wall_seconds(out_dir / "parallel.wall")
    par_paths = [out_dir / f"parallel_{y}.jsonl" for y in years]
    par_counts = summed_counts(par_paths)

    print("| mode | wall (mm:ss.ms) | records | records/min |")
    print("|---|---|---|---|")
    rate = "?"
    if par_wall and par_wall > 0 and par_counts.get("scraped"):
        rate = f"{par_counts['scraped'] * 60.0 / par_wall:.1f}"
    print(
        f"| parallel ({len(years)} processes) | {_fmt_wall(par_wall)} | "
        f"{_counts_cell(par_counts)} | {rate} |"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
