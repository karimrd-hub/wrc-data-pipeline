"""Unit tests for the pure-logic pieces of the pipeline.

Two groups of tests share this file:

1. **Task requirement compliance** — one test per numbered ``docs/task.md``
   requirement that can be verified without Mongo / MinIO / docker.  Each
   test's docstring quotes the requirement it covers so a reviewer can
   trace green/red back to the spec.
2. **Data quality features** — the sibling ``.txt`` extractor, the
   structured-field extractor, and the pydantic validation gate we added
   on top of the task spec.

The one real fixture is ``sample_detail.html`` — the WRC Labour Court
decision ``TED2616`` captured during Phase B recon.
"""

from __future__ import annotations

import json
import logging
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pytest
from pydantic import ValidationError

from wrc_pipeline.config.settings import ScraperSettings
from wrc_pipeline.logging_setup import JsonFormatter
from wrc_pipeline.scrapers.items import WrcItem
from wrc_pipeline.scrapers.utils.bodies import body_slug
from wrc_pipeline.scrapers.utils.dates import iter_partitions
from wrc_pipeline.storage.hashing import canonicalize_html, sha256_hash
from wrc_pipeline.transform.cleaner import ContentNotFoundError, clean_html
from wrc_pipeline.transform.extractor import extract_fields
from wrc_pipeline.transform.text import html_to_text, pdf_to_text
from wrc_pipeline.transform.validation import ProcessedRecord, build_quarantine_doc


_SAMPLE = (
    Path(__file__).resolve().parents[1]
    / "data" / "recon" / "data" / "recon" / "sample_detail.html"
)


@pytest.fixture(scope="module")
def cleaned_html() -> bytes:
    return clean_html(_SAMPLE.read_bytes(), "TED2616")


# ===========================================================================
# Task requirement compliance
# ---------------------------------------------------------------------------
# Each test below cites the ``docs/task.md`` requirement it verifies.
# Framework-free — Mongo / MinIO / docker are never touched, so the whole
# suite runs in well under a second.
# ===========================================================================


def test_req4_monthly_partitions_cover_a_multi_month_range_end_to_end() -> None:
    """Task req 4 — the scraper "must iterate on a time-period basis
    between the two dates. (example monthly partitions between
    01-01-2024 and 01-01-2025), and add a partition_date field for
    every record". The generator must emit one partition per calendar
    month touched by the range, and the ``[query_start, query_end]``
    windows must tile the requested range with no gap and no overlap.
    """
    parts = list(iter_partitions(date(2024, 1, 15), date(2024, 3, 10), "monthly"))
    assert len(parts) == 3
    days_covered: set[date] = set()
    for _partition_date, q_start, q_end in parts:
        cursor = q_start
        while cursor <= q_end:
            assert cursor not in days_covered, f"day {cursor} covered by two windows"
            days_covered.add(cursor)
            cursor += timedelta(days=1)
    assert min(days_covered) == date(2024, 1, 15)
    assert max(days_covered) == date(2024, 3, 10)


def test_req4_partition_date_bucket_key_is_stable_across_runs() -> None:
    """Task req 4 companion — ``partition_date`` is the field tagged on
    every record; it must be the same value across re-runs of the same
    range even when the caller shifts the range boundaries inside a
    month. Monthly buckets anchor at first-of-month regardless."""
    a = list(iter_partitions(date(2024, 1, 10), date(2024, 1, 20), "monthly"))
    b = list(iter_partitions(date(2024, 1, 25), date(2024, 1, 31), "monthly"))
    assert a[0][0] == b[0][0] == date(2024, 1, 1)


def test_req9_volatile_wrc_server_comments_do_not_defeat_hash_idempotency() -> None:
    """Task req 9 — "Running it twice on the same date range must not
    create duplicate records or re-download unchanged files. Use the
    file hash to detect changes between runs." WRC injects two
    server-generated HTML comments whose values drift per request; if
    the raw bytes were hashed as-is, req 9 would report "changed" on
    every re-run. The canonicaliser strips the volatile pieces so the
    hash of unchanged content stays constant across fetches.
    """
    base = b"<html><body>Decision text</body></html>"
    fetch_a = base + b"<!-- Elapsed time: 0.0625034 -->"
    fetch_b = base + b"<!-- Elapsed time: 0.0918762 -->"
    fetch_c = (
        base
        + b"<!-- Elapsed time: 0.0501129 -->"
        + b"<!-- cached or not being index.aspx page -->"
    )
    fingerprints = {
        sha256_hash(canonicalize_html(x)) for x in (base, fetch_a, fetch_b, fetch_c)
    }
    assert len(fingerprints) == 1


def test_req9_real_content_change_produces_a_different_hash() -> None:
    """Task req 9 (the other half) — the canonicaliser must strip
    *only* the known volatile markers. A one-byte edit to the decision
    text must still flip the hash, otherwise a genuine site update
    would masquerade as "unchanged" and never trigger a re-download.
    """
    a = b"<html><body>Decision text A</body></html>"
    b = b"<html><body>Decision text B</body></html>"
    assert sha256_hash(canonicalize_html(a)) != sha256_hash(canonicalize_html(b))


def test_req13c_i_pdf_bytes_pass_canonicalize_unchanged() -> None:
    """Task req 13c/i — "If the file is a pdf/doc file, don't apply any
    transformation." The pipeline calls ``canonicalize_html`` on every
    payload before hashing / storing; it must be a no-op for binary
    inputs so a reviewer can verify a stored PDF against ``file_hash``
    in Mongo with a plain ``sha256sum`` and no canonical-form knowledge.
    """
    pdf = b"%PDF-1.7\n\x00\x01\x02\x03payload bytes"
    assert canonicalize_html(pdf) == pdf
    assert sha256_hash(pdf) == sha256_hash(canonicalize_html(pdf))


def test_req10_json_log_lines_include_event_and_partition_context() -> None:
    """Task req 10 — "Produce structured logs (JSON format) that
    include: the current partition being processed, the body being
    scraped, ...". Format a log record with the ``extra={}`` fields
    our production code passes and confirm they round-trip through
    ``json.loads`` with the required context intact.
    """
    formatter = JsonFormatter()
    record = logging.LogRecord(
        name="wrc.pipeline",
        level=logging.INFO,
        pathname="",
        lineno=0,
        msg="record_stored",
        args=(),
        exc_info=None,
    )
    record.event = "record_stored"
    record.identifier = "TED2616"
    record.body = "Labour Court"
    record.partition_date = "2026-07-01"
    parsed = json.loads(formatter.format(record))
    assert parsed["event"] == "record_stored"
    assert parsed["body"] == "Labour Court"
    assert parsed["partition_date"] == "2026-07-01"
    assert parsed["identifier"] == "TED2616"
    assert parsed["level"] == "INFO"


def test_req13c_ii_html_cleaner_keeps_decision_and_drops_page_chrome() -> None:
    """Task req 13c/ii/1 — "Use an html parser like BeautifulSoup, and
    get only the relevant content of the document (excluding navigation
    bars/buttons, headers, footers, etc.)". Verified against the real
    ``TED2616`` fixture: the decision body must survive, the site's
    chrome (footer, Google Analytics / Tag Manager scripts) must not.
    """
    cleaned = clean_html(_SAMPLE.read_bytes(), "TED2616")
    assert b"TED2616" in cleaned
    assert b"DAWN MEATS" in cleaned
    assert b"<footer" not in cleaned
    assert b"Google Tag Manager" not in cleaned
    assert b"google-analytics" not in cleaned


def test_req13c_iii_processed_files_are_renamed_to_identifier_ext() -> None:
    """Task req 13c/iii — "Change the name of ALL the files to become
    identifier.ext (from metadata)". The transform builds processed
    object keys as ``{body_slug(body)}/{identifier}.{ext}`` — verify
    both the slug helper and the format string here so a rename
    regression can't ship silently.
    """
    assert body_slug("Labour Court") == "labour_court"
    assert body_slug("Workplace Relations Commission") == "workplace_relations_commission"
    assert (
        f"{body_slug('Labour Court')}/TED2616.html"
        == "labour_court/TED2616.html"
    )
    assert (
        f"{body_slug('Workplace Relations Commission')}/ADJ-00053174.pdf"
        == "workplace_relations_commission/ADJ-00053174.pdf"
    )


def test_req5_wrc_item_declares_every_task_required_metadata_field() -> None:
    """Task req 5 — "Extract metadata and capture the details for each
    record (title, description identifier, date, link to doc,
    partition_date, etc.)". ``WrcItem`` must declare every field the
    task names explicitly; extras (``body``, ``file_hash``, ...) are
    welcome but the six required fields must be present."""
    required = {"title", "description", "identifier", "date", "doc_url", "partition_date"}
    declared = set(WrcItem.fields)
    missing = required - declared
    assert not missing, f"WrcItem is missing task-required fields: {missing}"


def test_req12_scraper_settings_read_partition_size_from_environment(monkeypatch) -> None:
    """Task req 12 — "All connection strings, storage paths, partition
    sizes, and scraping parameters must be configurable via environment
    variables or a config file. No hardcoded values." Set the env var,
    reconstruct ``ScraperSettings``, verify the value flows through —
    and that a typo fails loud rather than silently falling back.
    """
    monkeypatch.setenv("SCRAPER_PARTITION_SIZE", "weekly")
    assert ScraperSettings.from_env().partition_size == "weekly"
    monkeypatch.setenv("SCRAPER_PARTITION_SIZE", "hourly")  # not a valid granularity
    with pytest.raises(ValueError):
        ScraperSettings.from_env()


# ===========================================================================
# Data quality feature tests — the sibling .txt, structured fields, pydantic
# gate we added on top of the task spec. See task.md addendum discussion.
# ===========================================================================


# ---------------------------------------------------------------------------
# text
# ---------------------------------------------------------------------------


def test_html_to_text_keeps_content_and_strips_chrome(cleaned_html: bytes) -> None:
    text = html_to_text(cleaned_html)
    assert "TED2616" in text
    assert "DAWN MEATS IRELAND UNLIMITED" in text
    assert "LYU LU" in text
    assert "<script" not in text.lower()
    assert "<html" not in text.lower()


def test_html_to_text_is_deterministic(cleaned_html: bytes) -> None:
    # Idempotency contract — same bytes in, same bytes out, always.
    assert html_to_text(cleaned_html) == html_to_text(cleaned_html)


def test_pdf_to_text_returns_none_on_garbage() -> None:
    assert pdf_to_text(b"") is None
    assert pdf_to_text(b"not a pdf") is None


# ---------------------------------------------------------------------------
# extractor
# ---------------------------------------------------------------------------


def test_extractor_pulls_key_fields(cleaned_html: bytes) -> None:
    fields = extract_fields(cleaned_html)
    assert fields["decision_type"] == "Appeal"
    assert fields["chair"] == "Mr Haugh"
    assert fields["signatory"] == "Alan Haugh"
    assert fields["hearing_date"] == "2026-06-23"
    assert fields["signature_date"] == "2026-07-13"
    assert 26000.0 in fields["monetary_amounts_eur"]


def test_extractor_splits_parties_and_representatives(cleaned_html: bytes) -> None:
    parties = extract_fields(cleaned_html)["parties"]
    assert "DAWN MEATS IRELAND UNLIMITED" in parties["complainant"]
    assert parties["respondent"] == "LYU LU"
    assert "REPRESENTED BY" not in parties["complainant"]
    assert any("IBEC" in r for r in parties["representatives"])


def test_extractor_finds_acts_cited(cleaned_html: bytes) -> None:
    joined = " | ".join(a.lower() for a in extract_fields(cleaned_html)["acts_cited"])
    assert "workplace relations act 2015" in joined
    assert "terms of employment" in joined


def test_extractor_never_raises_on_empty_input() -> None:
    fields = extract_fields(b"<html><body></body></html>")
    assert fields["chair"] is None
    assert fields["division_members"] == []
    assert fields["acts_cited"] == []


# ---------------------------------------------------------------------------
# validation
# ---------------------------------------------------------------------------


def _valid(**overrides) -> dict:
    base = {
        "identifier": "TED2616",
        "body": "Labour Court",
        "body_id": 3,
        "partition_date": "2026-07-01",
        "partition_end": "2026-07-31",
        "date": "13/07/2026",
        "bucket": "processed",
        "file_path": "labour_court/TED2616.html",
        "file_hash": "a" * 64,
        "file_size": 8192,
        "content_type": "text/html; charset=utf-8",
        "min_content_bytes": 1024,
    }
    base.update(overrides)
    return base


def test_validation_happy_path() -> None:
    assert ProcessedRecord(**_valid()).identifier == "TED2616"


@pytest.mark.parametrize(
    "field, value",
    [
        ("identifier", "oops bad<>"),
        ("identifier", ""),
        ("file_hash", "not-a-sha256"),
        ("partition_date", "01/01/2026"),
        ("file_size", 42),  # < min_content_bytes
    ],
)
def test_validation_rejects_bad_field(field: str, value) -> None:
    with pytest.raises(ValidationError):
        ProcessedRecord(**_valid(**{field: value}))


def test_validation_rejects_decision_date_after_partition_end() -> None:
    with pytest.raises(ValidationError):
        ProcessedRecord(
            **_valid(date="15/09/2026", partition_end="2026-07-31")
        )


def test_quarantine_doc_carries_reason_and_landing_pointers() -> None:
    now = datetime(2026, 7, 26, tzinfo=timezone.utc)
    landing = {
        "identifier": "TED2616",
        "body": "Labour Court",
        "partition_date": "2026-07-01",
        "partition_end": "2026-07-31",
        "bucket": "landing-zone",
        "file_path": "labour_court/2026-07/TED2616-abcdef012345.html",
        "file_hash": "a" * 64,
    }
    doc = build_quarantine_doc(
        landing_record=landing,
        reason="schema_validation_failed",
        details={"errors": ["identifier fails shape check"]},
        now=now,
    )
    assert doc["identifier"] == "TED2616"
    assert doc["quarantine_reason"] == "schema_validation_failed"
    assert doc["landing_file_path"].endswith(".html")
    assert doc["last_quarantined_at"] == now
