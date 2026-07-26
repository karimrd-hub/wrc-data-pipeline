r"""Pydantic-backed schema validation + quarantine routing.

Every candidate processed record is validated against ``ProcessedRecord``
before the upsert. On failure the record is routed to the quarantine
collection with the reason attached, instead of silently propagating a
half-baked row into ``processed_metadata``. Reviewers can inspect
quarantined rows to see what went wrong — schema drift, error-page
content, malformed dates, etc.

Validation rules (task-tip: every non-scraped record must be logged with
a reason):

* ``identifier`` — non-empty, matches ``^[A-Z0-9][A-Z0-9\-/.]{1,60}$``.
  We do NOT enforce per-body identifier regexes because the site emits a
  larger identifier zoo than public docs let on (TED, EDA, PWD, UDD, ADJ,
  DEC-E, EE, UD/NN, …); a lenient upper-alnum-plus-punctuation rule
  catches obvious garbage without false-positiving on rare shapes.
* ``file_hash``  — SHA-256 hex (64 hex chars).
* ``file_size``  — >= ``TRANSFORM_MIN_CONTENT_BYTES`` (default 1024).
  Below that the candidate is almost certainly an error page /
  session-timeout stub — real decisions in the recon set are 5 KB+.
* ``date <= partition_end`` — a decision dated after its partition ended
  is a shape/parse error somewhere upstream.

Quarantine contract:

* Same unique ``identifier`` index as the other collections.
* If a previously-quarantined record later validates, the caller deletes
  it from quarantine before upserting into processed — so a given
  identifier can be in exactly one of {processed, quarantine, neither}.
"""

from __future__ import annotations

import re
from datetime import date, datetime
from typing import Optional

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


_IDENTIFIER_RE = re.compile(r"^[A-Z0-9][A-Z0-9\-/.]{1,60}$")
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
# The site emits DD/MM/YYYY on decisions and ISO on partition keys — accept
# both when normalising the decision date.
_ISO_DATE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_SITE_DATE = re.compile(r"^(\d{1,2})/(\d{1,2})/(\d{4})$")


class Parties(BaseModel):
    complainant: Optional[str] = None
    respondent: Optional[str] = None
    representatives: list[str] = Field(default_factory=list)


class StructuredFields(BaseModel):
    """Best-effort structured extraction — all fields nullable.

    The validator's job is to *type-check* what the extractor produced;
    a decision that legitimately has no PARTIES section (rare, e.g.
    procedural rulings) is not an error.
    """

    model_config = ConfigDict(extra="allow")

    decision_type: Optional[str] = None
    chair: Optional[str] = None
    adjudicating_officer: Optional[str] = None
    division_members: list[dict] = Field(default_factory=list)
    parties: Parties = Field(default_factory=Parties)
    hearing_date: Optional[str] = None
    signature_date: Optional[str] = None
    signatory: Optional[str] = None
    acts_cited: list[str] = Field(default_factory=list)
    monetary_amounts_eur: list[float] = Field(default_factory=list)


class ProcessedRecord(BaseModel):
    """The shape we require of every row before it lands in
    ``processed_metadata``. Fields that vary between HTML and PDF branches
    (``text_file_*``) are optional — the caller decides."""

    model_config = ConfigDict(extra="allow")

    identifier: str
    body: str
    body_id: int
    partition_date: str
    partition_end: str
    date: Optional[str] = None
    bucket: str
    file_path: str
    file_hash: str
    file_size: int
    content_type: str
    text_file_path: Optional[str] = None
    text_file_hash: Optional[str] = None
    text_size: Optional[int] = None
    structured: Optional[StructuredFields] = None
    min_content_bytes: int  # threshold used to validate this record

    @field_validator("identifier")
    @classmethod
    def _identifier_shape(cls, v: str) -> str:
        if not _IDENTIFIER_RE.match(v):
            raise ValueError(f"identifier {v!r} fails shape check")
        return v

    @field_validator("file_hash")
    @classmethod
    def _hash_shape(cls, v: str) -> str:
        if not _SHA256_RE.match(v):
            raise ValueError("file_hash is not a 64-char SHA-256 hex")
        return v

    @field_validator("partition_date", "partition_end")
    @classmethod
    def _partition_iso(cls, v: str) -> str:
        if not _ISO_DATE.match(v):
            raise ValueError(f"partition date {v!r} not ISO YYYY-MM-DD")
        return v

    @model_validator(mode="after")
    def _min_size(self) -> "ProcessedRecord":
        if self.file_size < self.min_content_bytes:
            raise ValueError(
                f"file_size={self.file_size} < min_content_bytes={self.min_content_bytes} "
                f"(likely error page)"
            )
        return self

    @model_validator(mode="after")
    def _date_within_partition(self) -> "ProcessedRecord":
        decision = _coerce_decision_date(self.date)
        if decision is None:
            return self
        try:
            end = date.fromisoformat(self.partition_end)
        except ValueError:
            return self
        if decision > end:
            raise ValueError(
                f"decision date {decision.isoformat()} > partition_end {end.isoformat()}"
            )
        return self


def _coerce_decision_date(value: Optional[str]) -> Optional[date]:
    """Return an ISO date if we can parse ``value`` (DD/MM/YYYY or ISO),
    else ``None`` (missing dates aren't validated — that's a soft field)."""
    if not value:
        return None
    m = _SITE_DATE.match(value)
    if m:
        d, mo, y = (int(x) for x in m.groups())
        try:
            return date(y, mo, d)
        except ValueError:
            return None
    try:
        return date.fromisoformat(value)
    except ValueError:
        return None


def build_quarantine_doc(
    landing_record: dict,
    reason: str,
    details: dict,
    now: datetime,
) -> dict:
    """Freeze the landing snapshot alongside the failure reason.

    ``details`` carries the machine-readable specifics (validation error
    list, error class name, sizes). ``landing_record`` is the raw
    ``landing_metadata`` document — copied verbatim so a reviewer can
    replay the transform against the same bytes.
    """
    doc = {
        "identifier": landing_record.get("identifier"),
        "body": landing_record.get("body"),
        "body_id": landing_record.get("body_id"),
        "partition_date": landing_record.get("partition_date"),
        "partition_end": landing_record.get("partition_end"),
        "landing_bucket": landing_record.get("bucket"),
        "landing_file_path": landing_record.get("file_path"),
        "landing_file_hash": landing_record.get("file_hash"),
        "content_type": landing_record.get("content_type"),
        "quarantine_reason": reason,
        "quarantine_details": details,
        "last_quarantined_at": now,
    }
    return doc
