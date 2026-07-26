"""Dagster orchestration for the WRC pipeline (task infra bullet).

Two software-defined assets, both partitioned on **two dimensions**:

* ``date`` — one calendar month (start 2020-01).
* ``body`` — which tribunal to scrape. Dropdown values are human-readable
  slugs (``labour_court``, ``workplace_relations_commission``, ...); the
  numeric body id is resolved at submit time via ``BODY_SLUGS``.

Multi-partitioning is the speed lever. With the QueuedRunCoordinator wired
in ``dagster.yaml`` and ``DAGSTER_MAX_CONCURRENT_RUNS`` > 1, a backfill fans
each ``(month × body)`` combination out into its own Scrapy subprocess —
bodies no longer share a single reactor, they crawl in parallel processes.

    landing_records  →  processed_records

``processed_records`` has ``deps=[landing_records]`` on the same partition
key, so Dagster refuses to materialize transform for a partition whose
ingest hasn't succeeded — the task's "separate tasks with proper dependency
handling" contract.

Scrapy is invoked as a **subprocess**, not embedded, on purpose: Scrapy
drives Twisted's reactor which is not restartable within one Python
process. Subprocessing sidesteps the whole class of "reactor already
running" bugs and lets Dagster stream the spider's JSON log lines through
its own log view unchanged.
"""

from __future__ import annotations

import os
import subprocess
import sys
import threading
from calendar import monthrange
from datetime import date
from pathlib import Path

import dagster as dg

from wrc_pipeline.config.settings import settings
from wrc_pipeline.scrapers.utils.bodies import BODIES, body_slug
from wrc_pipeline.transform.runner import TransformRunner


# Subprocess wall-clock timeout lives in .env
# (``DAGSTER_SUBPROCESS_TIMEOUT_SEC``); default one hour is well beyond any
# observed partition runtime.


# Slug -> numeric body id. Slugs are the human-readable dropdown values in
# Dagit; the numeric id is what the spider and the site's search URL expect.
BODY_SLUGS: dict[str, int] = {body_slug(name): bid for bid, name in BODIES.items()}


# Monthly partitions. Weekly was tried but per-subprocess startup overhead
# ate the parallelism benefit at the 500-1000-doc test volume; monthly
# amortizes subprocess startup across more per-partition work.
_MONTHLY = dg.MonthlyPartitionsDefinition(start_date=settings.dagster.partition_start_date)
_BODIES = dg.StaticPartitionsDefinition(sorted(BODY_SLUGS))
_PARTITIONS = dg.MultiPartitionsDefinition({"date": _MONTHLY, "body": _BODIES})

# Repo root — the folder containing ``scrapy.cfg``. Dagster may launch this
# code from anywhere (its own workspace, a container WORKDIR), so we resolve
# the path relative to *this* file rather than trusting the cwd.
_REPO_ROOT = Path(__file__).resolve().parents[3]


def _resolve(context) -> tuple[date, date, str, int]:
    """(start_date, end_date, body_slug, body_id) for the current partition."""
    dims = context.partition_key.keys_by_dimension
    start = date.fromisoformat(dims["date"])
    last_day = monthrange(start.year, start.month)[1]
    end = date(start.year, start.month, last_day)
    slug = dims["body"]
    return start, end, slug, BODY_SLUGS[slug]


@dg.asset(
    partitions_def=_PARTITIONS,
    retry_policy=dg.RetryPolicy(
        max_retries=settings.dagster.landing_max_retries,
        delay=settings.dagster.landing_retry_delay_sec,
    ),
    group_name="wrc",
    description="Raw WRC decisions for one (month × body) partition.",
)
def landing_records(context) -> dg.MaterializeResult:
    """Run the Scrapy spider for one (month × body) partition.

    Emits ``record_stored`` / ``record_unchanged`` / ``partition_summary``
    JSON events on stdout — Dagster captures them into the run log so a
    reviewer sees the same trail as the local CLI.
    """
    start_d, end_d, slug, body_id = _resolve(context)

    cmd = [
        sys.executable, "-m", "scrapy", "crawl", "wrc",
        "-a", f"start_date={start_d.isoformat()}",
        "-a", f"end_date={end_d.isoformat()}",
        "-a", f"bodies={body_id}",
    ]
    context.log.info(f"launching: {' '.join(cmd)} (cwd={_REPO_ROOT})")

    # Line-buffered Popen + live drain so Dagit shows crawl progress as it
    # happens, not only after the subprocess exits. Each line is already a
    # JSON event (see ``logging_setup.install_json_root_logging``).
    proc = subprocess.Popen(
        cmd,
        cwd=str(_REPO_ROOT),
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
        env={**os.environ, "SCRAPY_SETTINGS_MODULE": "wrc_pipeline.scrapers.settings"},
    )

    # Read stdout on a worker thread so the main thread can enforce a
    # wall-clock timeout with ``proc.wait(timeout=...)``. A blocking ``for
    # line in proc.stdout`` on the main thread would swallow the timeout.
    reader = threading.Thread(
        target=_drain_stdout, args=(proc, context), daemon=True,
    )
    reader.start()

    subprocess_timeout = settings.dagster.subprocess_timeout_sec
    try:
        returncode = proc.wait(timeout=subprocess_timeout)
    except subprocess.TimeoutExpired:
        proc.terminate()
        try:
            proc.wait(timeout=15)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait()
        reader.join(timeout=5)
        raise dg.Failure(
            description=(
                f"scrapy timed out after {subprocess_timeout}s for "
                f"partition {context.partition_key}"
            ),
        )

    reader.join(timeout=5)

    if returncode != 0:
        raise dg.Failure(
            description=(
                f"scrapy exited with code {returncode} for partition "
                f"{context.partition_key} — see run log for record_failed events"
            ),
        )

    return dg.MaterializeResult(
        metadata={
            "partition_start": str(start_d),
            "partition_end": str(end_d),
            "body": slug,
            "body_id": body_id,
        }
    )


@dg.asset(
    partitions_def=_PARTITIONS,
    deps=[landing_records],
    retry_policy=dg.RetryPolicy(
        max_retries=settings.dagster.processed_max_retries,
        delay=settings.dagster.processed_retry_delay_sec,
    ),
    group_name="wrc",
    description="Cleaned + renamed WRC decisions for one (month × body) partition.",
)
def processed_records(context) -> dg.MaterializeResult:
    """Run the transform for one (month × body) partition.

    Called in-process because the runner is plain Python — no reactor
    conflict, no need to subprocess.
    """
    start_d, end_d, slug, body_id = _resolve(context)
    context.log.info(
        f"transforming {start_d.isoformat()} → {end_d.isoformat()} body={slug}"
    )

    runner = TransformRunner()
    stats = runner.run(start_d, end_d, body_ids=[body_id])

    if stats.failed > 0:
        # Per-record failures are already logged as ``record_failed`` events
        # with precise reasons; raise Failure so the asset materialization
        # reflects the incomplete run and the retry policy gets a chance.
        raise dg.Failure(
            description=(
                f"transform completed with {stats.failed} failed record(s) "
                f"for partition {context.partition_key} — see run log for "
                f"record_failed events"
            ),
            metadata={
                "transformed": stats.transformed,
                "unchanged": stats.unchanged,
                "passthrough": stats.passthrough,
                "failed": stats.failed,
                "body": slug,
            },
        )

    return dg.MaterializeResult(
        metadata={
            "transformed": stats.transformed,
            "unchanged": stats.unchanged,
            "passthrough": stats.passthrough,
            "failed": stats.failed,
            "body": slug,
        }
    )


def _drain_stdout(proc: subprocess.Popen, context) -> None:
    """Forward every subprocess stdout line to Dagit's run log."""
    assert proc.stdout is not None
    for line in proc.stdout:
        context.log.info(line.rstrip())


defs = dg.Definitions(assets=[landing_records, processed_records])
