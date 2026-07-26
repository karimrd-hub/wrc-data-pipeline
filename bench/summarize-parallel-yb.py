#!/usr/bin/env python3
"""Summarize a (year × body) benchmark run.

Globs every ``parallel_*.jsonl`` under the label dir and aggregates the
``partition_summary`` events + wall time recorded by the runner script.
Also breaks out per-body totals so a reviewer can see how the load split
across the tribunals.
"""

from __future__ import annotations

import collections
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


def aggregate(paths):
    totals: dict[str, int] = {}
    per_body: dict[str, int] = collections.Counter()
    for path in paths:
        for obj in _iter_events(path):
            ev = obj.get("event")
            if ev == "partition_summary":
                for k, v in obj.items():
                    if isinstance(v, bool) or k == "body_id":
                        continue
                    if isinstance(v, (int, float)):
                        totals[k] = totals.get(k, 0) + int(v)
            elif ev == "record_stored":
                body = obj.get("body", "?")
                per_body[body] += 1
    return totals, per_body


def _read_wall_seconds(path: Path):
    if not path.exists():
        return None
    try:
        return float(path.read_text().strip())
    except ValueError:
        return None


def _fmt_wall(seconds):
    if seconds is None:
        return "?"
    m, s = divmod(seconds, 60)
    return f"{int(m):02d}:{s:05.2f}"


def _counts_cell(counts):
    if not counts:
        return "—"
    return ", ".join(f"{k}={v}" for k, v in sorted(counts.items()) if v)


def main() -> int:
    out_dir = Path(sys.argv[1])
    start = sys.argv[2]
    end = sys.argv[3]
    target = sys.argv[4]
    n_workers = sys.argv[5]

    paths = sorted(out_dir.glob("parallel_*.jsonl"))
    wall = _read_wall_seconds(out_dir / "parallel.wall")
    totals, per_body = aggregate(paths)

    print(f"# (year × body) benchmark — `{out_dir.name}`")
    print()
    print(f"- Range: `{start}` → `{end}`")
    print(f"- Workers: `{n_workers}` (one Scrapy subprocess per year × body)")
    print(f"- Files aggregated: `{len(paths)}`")
    print(f"- Per-process AutoThrottle target: `{target}`  "
          f"(aggregate ≈ `{float(target) * int(n_workers):.1f}`)")
    print()
    print("| wall (mm:ss.ms) | records | records/min |")
    print("|---|---|---|")
    rate = "?"
    if wall and wall > 0 and totals.get("scraped"):
        rate = f"{totals['scraped'] * 60.0 / wall:.1f}"
    print(f"| {_fmt_wall(wall)} | {_counts_cell(totals)} | {rate} |")

    if per_body:
        print()
        print("## Per-body record counts")
        print()
        print("| body | records |")
        print("|---|---|")
        for body, n in sorted(per_body.items(), key=lambda x: -x[1]):
            print(f"| {body} | {n} |")
    return 0


if __name__ == "__main__":
    sys.exit(main())
