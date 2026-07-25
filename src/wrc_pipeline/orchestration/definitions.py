"""Dagster orchestration for the WRC pipeline (task infra bullet).

Two monthly-partitioned assets wire the ingest step to the transform step
with an explicit dependency:

    landing_records  →  processed_records

`landing_records` runs the Scrapy spider for the selected month × all
bodies; `processed_records` runs the transform for the same month. Because
the second asset ``deps=[landing_records]``, Dagster refuses to materialize
transform for a partition whose landing step has not yet completed
successfully in that partition — matches the task's "separate tasks with
proper dependency handling" requirement.

Monthly partitions were the natural choice: the spider already partitions
by month internally (recon §7.3 gave us month-level volumes; ``partition_date``
is stored on every record). Weekly / daily are available at the CLI layer
via ``SCRAPER_PARTITION_SIZE`` for people who need finer granularity — the
orchestrator layer uses months so one Dagit partition == one calendar
month == one row in the Mongo ``partition_date`` axis.

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
from calendar import monthrange
from datetime import date
from pathlib import Path

import dagster as dg

from wrc_pipeline.scrapers.utils.bodies import BODIES
from wrc_pipeline.transform.runner import TransformRunner


# Partition range must start no later than the earliest data on the site.
# 2020-01 is well before the first published Labour Court decisions we care
# about, so materializing any real month is possible.
_MONTHLY = dg.MonthlyPartitionsDefinition(start_date="2020-01-01")

# Repo root — the folder containing ``scrapy.cfg``. Dagster may launch this
# code from anywhere (its own workspace, a container WORKDIR), so we resolve
# the path relative to *this* file rather than trusting the cwd.
_REPO_ROOT = Path(__file__).resolve().parents[3]


def _month_bounds(partition_key: str) -> tuple[date, date]:
    start = date.fromisoformat(partition_key)
    last_day = monthrange(start.year, start.month)[1]
    end = date(start.year, start.month, last_day)
    return start, end


@dg.asset(
    partitions_def=_MONTHLY,
    group_name="wrc",
    description="Raw WRC decisions for the partition month across all bodies.",
)
def landing_records(context) -> dg.MaterializeResult:
    """Run the Scrapy spider for one calendar month.

    Emits ``record_stored`` / ``record_unchanged`` / ``partition_summary``
    JSON events on stdout — Dagster captures them into the run log so a
    reviewer sees the same trail as the local CLI.
    """
    start_d, end_d = _month_bounds(context.partition_key)
    bodies_arg = ",".join(str(b) for b in BODIES)

    cmd = [
        sys.executable, "-m", "scrapy", "crawl", "wrc",
        "-a", f"start_date={start_d.isoformat()}",
        "-a", f"end_date={end_d.isoformat()}",
        "-a", f"bodies={bodies_arg}",
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
    assert proc.stdout is not None
    for line in proc.stdout:
        context.log.info(line.rstrip())
    returncode = proc.wait()

    if returncode != 0:
        raise dg.Failure(
            description=f"scrapy exited with code {returncode} for partition {context.partition_key}",
        )

    return dg.MaterializeResult(
        metadata={"partition_start": str(start_d), "partition_end": str(end_d)}
    )


@dg.asset(
    partitions_def=_MONTHLY,
    deps=[landing_records],
    group_name="wrc",
    description="Cleaned + renamed WRC decisions for the partition month.",
)
def processed_records(context) -> dg.MaterializeResult:
    """Run the transform for one calendar month.

    Called in-process because the runner is plain Python — no reactor
    conflict, no need to subprocess.
    """
    start_d, end_d = _month_bounds(context.partition_key)
    context.log.info(f"transforming {start_d.isoformat()} → {end_d.isoformat()}")

    runner = TransformRunner()
    stats = runner.run(start_d, end_d)

    return dg.MaterializeResult(
        metadata={
            "transformed": stats.transformed,
            "unchanged": stats.unchanged,
            "passthrough": stats.passthrough,
            "failed": stats.failed,
        }
    )


defs = dg.Definitions(assets=[landing_records, processed_records])
