"""Command-line entry for the transformation step.

Usage:

    python -m wrc_pipeline.transform \
        --start-date 2026-01-01 \
        --end-date 2026-01-31 \
        [--bodies 3,15376]

All connection strings, buckets, and collection names come from the shared
``config.settings`` module — this script has no configuration of its own.
Exit code is 0 on completion (even with per-record failures, which are
logged as ``record_failed`` JSON events and counted in ``transform_summary``)
and non-zero only on argument-parsing errors or unrecoverable startup
failures.
"""

from __future__ import annotations

import argparse
import sys
from datetime import date

from wrc_pipeline.logging_setup import install_json_root_logging
from wrc_pipeline.transform.runner import TransformRunner


def _parse_bodies(raw: str | None) -> list[int] | None:
    if not raw:
        return None
    return [int(b) for b in raw.split(",") if b.strip()]


def _parse_iso(s: str) -> date:
    try:
        return date.fromisoformat(s)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(str(exc)) from exc


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="wrc-transform",
        description="Clean and re-materialize scraped WRC decisions from the landing zone.",
    )
    parser.add_argument(
        "--start-date", required=True, type=_parse_iso,
        help="Inclusive start of the partition_date range (YYYY-MM-DD).",
    )
    parser.add_argument(
        "--end-date", required=True, type=_parse_iso,
        help="Inclusive end of the partition_date range (YYYY-MM-DD).",
    )
    parser.add_argument(
        "--bodies", default=None,
        help="Optional comma-separated list of body IDs to filter by (default: all).",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    install_json_root_logging()
    args = build_parser().parse_args(argv)
    body_ids = _parse_bodies(args.bodies)
    runner = TransformRunner()
    runner.run(args.start_date, args.end_date, body_ids=body_ids)
    return 0


if __name__ == "__main__":
    sys.exit(main())
