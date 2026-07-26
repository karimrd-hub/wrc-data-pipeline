#!/usr/bin/env python3
"""Turn a bench/results/<label>/ directory into a Markdown summary table.

Wall time is computed from the first vs. last ``ts`` field in each
``<phase>.jsonl`` — those come from the pipeline's own JSON root logger, so
we don't need /usr/bin/time and the benchmark works inside a slim container.

Record counts are summed across every ``partition_summary`` event.
"""

from __future__ import annotations

import json
import sys
from datetime import datetime
from pathlib import Path

PHASES = ("scraper_cold", "scraper_warm", "transform_cold", "transform_warm")


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


def wall_seconds(path: Path) -> float | None:
    """First ts to last ts in ``path``. Returns None if unparseable."""
    first_ts = last_ts = None
    for obj in _iter_events(path):
        ts = obj.get("ts")
        if not ts:
            continue
        if first_ts is None:
            first_ts = ts
        last_ts = ts
    if first_ts is None or last_ts is None:
        return None
    try:
        a = datetime.fromisoformat(first_ts)
        b = datetime.fromisoformat(last_ts)
    except (ValueError, TypeError):
        return None
    return (b - a).total_seconds()


def _fmt_wall(seconds: float | None) -> str:
    if seconds is None:
        return "?"
    m, s = divmod(seconds, 60)
    return f"{int(m):02d}:{s:05.2f}"


def summed_counts(path: Path) -> dict[str, int]:
    """Sum every numeric field of every ``partition_summary`` event."""
    totals: dict[str, int] = {}
    for obj in _iter_events(path):
        if obj.get("event") != "partition_summary":
            continue
        for k, v in obj.items():
            # body_id is numeric but not a counter — skip it explicitly.
            if isinstance(v, bool) or k == "body_id":
                continue
            if isinstance(v, (int, float)):
                totals[k] = totals.get(k, 0) + int(v)
    return totals


def _counts_cell(counts: dict[str, int]) -> str:
    if not counts:
        return "—"
    return ", ".join(f"{k}={v}" for k, v in sorted(counts.items()) if v)


def main() -> int:
    out_dir = Path(sys.argv[1])
    start = sys.argv[2] if len(sys.argv) > 2 else "?"
    end = sys.argv[3] if len(sys.argv) > 3 else "?"
    bodies = sys.argv[4] if len(sys.argv) > 4 else ""

    print(f"# Benchmark — `{out_dir.name}`")
    print()
    print(f"- Range: `{start}` → `{end}`")
    print(f"- Bodies: `{bodies or 'all four'}`")
    print()
    print("| phase | wall (mm:ss.ms) | records |")
    print("|---|---|---|")
    for phase in PHASES:
        log_path = out_dir / f"{phase}.jsonl"
        wall = _fmt_wall(wall_seconds(log_path))
        counts = summed_counts(log_path)
        print(f"| `{phase}` | {wall} | {_counts_cell(counts)} |")
    return 0


if __name__ == "__main__":
    sys.exit(main())
