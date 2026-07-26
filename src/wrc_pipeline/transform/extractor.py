"""Structured field extraction from cleaned decision HTML.

WRC decisions follow a labelled-heading template (verified against Labour
Court ``TED2616``, 2026-07): a ``div.content`` subtree with headings named
``PARTIES``, ``DIVISION``, ``SUBJECT``, ``BACKGROUND``, ``DECISION``,
followed by a signature block. WRC ADJ pages use "Adjudication Officer"
instead of "Chair"; every extractor is best-effort — missing pieces come
back as ``None`` (or ``[]``) rather than raising.

Fields returned (see ``extract_fields``):

* ``decision_type``          Appeal / Complaint / Enforcement / Industrial Relations Referral
* ``chair``                  Labour Court chair (from DIVISION table row "Chairman:")
* ``adjudicating_officer``   WRC officer (from body text signed / adjudicated by)
* ``division_members``       ``[{"role": "Chairman", "name": "Mr Haugh"}, ...]``
* ``parties.complainant``    text before the "AND" divider inside PARTIES
* ``parties.respondent``     text after the "AND" divider
* ``parties.representatives`` ``["(REPRESENTED BY IBEC)", ...]``
* ``hearing_date``           ISO date parsed from body text
* ``signature_date``         ISO date parsed from signature block
* ``signatory``              name from signature block
* ``acts_cited``             Irish statutes named in body text (de-duped, ordered)
* ``monetary_amounts_eur``   distinct € amounts named anywhere in body
"""

from __future__ import annotations

import re
from datetime import date, datetime
from typing import Iterable

from bs4 import BeautifulSoup, Tag


_KNOWN_LABELS: tuple[str, ...] = ("PARTIES", "DIVISION", "SUBJECT", "BACKGROUND", "DECISION")

_MONTHS = {
    "january": 1, "february": 2, "march": 3, "april": 4, "may": 5, "june": 6,
    "july": 7, "august": 8, "september": 9, "october": 10, "november": 11, "december": 12,
}
# "23 June 2026", "23rd June 2026", "1st of January 2025"
_LONG_DATE = re.compile(
    r"\b(\d{1,2})(?:st|nd|rd|th)?\s+(?:of\s+)?"
    r"(january|february|march|april|may|june|july|august|september|october|november|december)"
    r"\s+(\d{4})\b",
    re.IGNORECASE,
)
# DD/MM/YYYY, DD-MM-YYYY (used in signature blocks)
_SHORT_DATE = re.compile(r"\b(\d{1,2})[/\-](\d{1,2})[/\-](\d{4})\b")
# "Court hearing took place on ...", "Adjudication hearing was held on ..."
_HEARING_CONTEXT = re.compile(
    r"(?:hearing\s+(?:took\s+place|was\s+held|convened)|held\s+a\s+hearing)"
    r"[^.]{0,80}?on\s+(?:the\s+)?",
    re.IGNORECASE,
)
# Irish statutes: "<Words> Act[s] YYYY[ to YYYY]"
_ACT = re.compile(
    r"([A-Z][A-Za-z'()\- ,&]{2,80}?Acts?\s+(?:19|20)\d{2}"
    r"(?:\s+to\s+(?:19|20)\d{2})?)",
)
# €1,234, €1,234.56, € 1234
_MONEY = re.compile(r"€\s*([\d]{1,3}(?:[,\s]\d{3})*(?:\.\d+)?)")
# "(REPRESENTED BY ...)" — case-insensitive; parentheses required.
_REPRESENTED = re.compile(r"\(\s*REPRESENTED\s+BY\s+([^)]+?)\s*\)", re.IGNORECASE)

_DECISION_KEYWORDS: tuple[tuple[str, str], ...] = (
    ("appeal", "Appeal"),
    ("enforcement", "Enforcement"),
    ("industrial relations", "Industrial Relations Referral"),
    ("complaint", "Complaint"),
)


def extract_fields(cleaned_html: bytes) -> dict:
    """Return best-effort structured fields from cleaned decision HTML.

    Never raises: unknown-shape pages yield a mostly-``None`` dict with
    empty lists in the array slots.
    """
    soup = BeautifulSoup(cleaned_html, "lxml")

    labels = _find_labels(soup)
    body_text = _visible_text(soup)

    parties = _parties(labels.get("PARTIES"))
    division_members = _division_members(labels.get("DIVISION"))
    chair = _chair_from_division(division_members)
    subject_text = _text_after(labels.get("SUBJECT"))

    return {
        "decision_type": _decision_type(subject_text, body_text),
        "chair": chair,
        "adjudicating_officer": _adjudicating_officer(body_text),
        "division_members": division_members,
        "parties": parties,
        "hearing_date": _hearing_date(body_text),
        "signature_date": _signature_date(soup),
        "signatory": _signatory(soup),
        "acts_cited": _acts_cited(body_text),
        "monetary_amounts_eur": _monetary_amounts(body_text),
    }


# ---------------------------------------------------------------------------
# Section-locator helpers
# ---------------------------------------------------------------------------


def _find_labels(soup: BeautifulSoup) -> dict[str, Tag]:
    """Map ``"PARTIES"`` / ``"DIVISION"`` / ... to the ``<p>`` that hosts it.

    Labels appear as ``<span class="c3">PARTIES:</span>`` inside a
    ``<strong>`` inside a ``<p>``. Non-label ``.c3`` spans (used as row
    labels inside the DIVISION table, e.g. "Chairman:") are filtered out.
    """
    labels: dict[str, Tag] = {}
    for span in soup.select("span.c3"):
        text = span.get_text(" ", strip=True).upper().rstrip(":")
        text = text.strip()
        if text not in _KNOWN_LABELS or text in labels:
            continue
        # Walk up to the enclosing <p> — the label's *content* lives in
        # that <p>'s next block-level sibling.
        host = span
        for _ in range(4):
            if host.parent is None or host.parent.name in ("p", "div", "td"):
                host = host.parent
                break
            host = host.parent
        if host is not None:
            labels[text] = host  # type: ignore[assignment]
    return labels


def _text_after(label_host: Tag | None) -> str:
    """Text of the block-level sibling(s) that follow ``label_host`` up to
    the next known label. Empty string when the label isn't found."""
    if label_host is None:
        return ""
    parts: list[str] = []
    for sib in label_host.find_next_siblings():
        if _hosts_label(sib):
            break
        text = sib.get_text(" ", strip=True)
        if text:
            parts.append(text)
    return " ".join(parts)


def _hosts_label(tag: Tag) -> bool:
    for span in tag.select("span.c3") if hasattr(tag, "select") else []:
        text = span.get_text(" ", strip=True).upper().rstrip(":").strip()
        if text in _KNOWN_LABELS:
            return True
    return False


def _visible_text(soup: BeautifulSoup) -> str:
    return soup.get_text(" ", strip=True)


# ---------------------------------------------------------------------------
# Field extractors
# ---------------------------------------------------------------------------


def _parties(host: Tag | None) -> dict:
    """Split the PARTIES block on the ``AND`` divider paragraph.

    Bold paragraphs before ``AND`` = complainant lines, after = respondent
    lines. ``(REPRESENTED BY …)`` fragments are stripped from names and
    collected into ``representatives``.
    """
    empty: dict = {"complainant": None, "respondent": None, "representatives": []}
    if host is None:
        return empty
    # Content div sits as the next sibling of the label paragraph.
    content = host.find_next_sibling(["div", "p", "table"])
    if content is None:
        return empty
    lines: list[str] = []
    for tag in content.find_all(["p", "b", "strong"], recursive=True):
        text = tag.get_text(" ", strip=True)
        if text and text not in lines:
            lines.append(text)
    return _split_parties(lines)


def _split_parties(lines: list[str]) -> dict:
    complainant: list[str] = []
    respondent: list[str] = []
    reps: list[str] = []
    seen_divider = False
    for line in lines:
        upper = line.upper().strip()
        if upper == "AND" or upper.startswith("AND "):
            seen_divider = True
            continue
        reps.extend(m.group(1).strip() for m in _REPRESENTED.finditer(line))
        cleaned = _REPRESENTED.sub("", line).strip(" ()")
        if not cleaned:
            continue
        (respondent if seen_divider else complainant).append(cleaned)
    return {
        "complainant": " ".join(complainant) or None,
        "respondent": " ".join(respondent) or None,
        "representatives": list(dict.fromkeys(reps)),
    }


def _division_members(host: Tag | None) -> list[dict]:
    """Rows of the DIVISION table: ``[{role, name}, ...]``. Labour Court
    only — WRC ADJ pages have no such block and return ``[]``."""
    if host is None:
        return []
    table = host.find_next_sibling("table")
    if table is None:
        return []
    members: list[dict] = []
    for row in table.find_all("tr"):
        cells = row.find_all("td")
        if len(cells) < 2:
            continue
        role = cells[0].get_text(" ", strip=True).rstrip(":").strip()
        name = cells[1].get_text(" ", strip=True).strip()
        if role and name:
            members.append({"role": role, "name": name})
    return members


def _chair_from_division(members: list[dict]) -> str | None:
    for m in members:
        if m["role"].lower() in ("chairman", "chairperson", "chair", "deputy chairman"):
            return m["name"]
    return None


def _adjudicating_officer(body_text: str) -> str | None:
    """WRC ADJ pages: the officer is signed at the bottom as
    ``Adjudication Officer: <Name>`` (or "Adjudicator:" on older layouts)."""
    for pattern in (
        r"Adjudication\s+Officer\s*[:\-]\s*([A-Z][A-Za-z' .\-]{2,80}?)(?=[.\s]|$)",
        r"Adjudicator\s*[:\-]\s*([A-Z][A-Za-z' .\-]{2,80}?)(?=[.\s]|$)",
    ):
        m = re.search(pattern, body_text)
        if m:
            return m.group(1).strip(" .,")
    return None


def _decision_type(subject_text: str, body_text: str) -> str | None:
    """First keyword hit wins — searched in SUBJECT first (higher signal),
    then the full body as a fallback."""
    haystacks = [subject_text.lower(), body_text.lower()]
    for haystack in haystacks:
        if not haystack:
            continue
        for keyword, label in _DECISION_KEYWORDS:
            if keyword in haystack:
                return label
    return None


def _hearing_date(body_text: str) -> str | None:
    """Parse the hearing date out of "hearing took place on ..." style
    phrasings. Returns ISO ``YYYY-MM-DD`` on hit, ``None`` otherwise."""
    for m in _HEARING_CONTEXT.finditer(body_text):
        tail = body_text[m.end() : m.end() + 60]
        parsed = _parse_any_date(tail)
        if parsed:
            return parsed.isoformat()
    return None


def _signature_date(soup: BeautifulSoup) -> str | None:
    """Signature blocks are the last table on the page; the date lives in
    a cell that matches ``DD/MM/YYYY``. Falls back to the last DD/MM/YYYY
    anywhere on the page."""
    tables = soup.find_all("table")
    for table in reversed(tables):
        text = table.get_text(" ", strip=True)
        parsed = _last_short_date(text)
        if parsed:
            return parsed.isoformat()
    return None


def _signatory(soup: BeautifulSoup) -> str | None:
    """Signatory name = bold text between "Signed on behalf" and the
    date/role footer inside the signature table."""
    for table in reversed(soup.find_all("table")):
        text = table.get_text(" ", strip=True)
        if "signed on behalf" not in text.lower():
            continue
        for b in table.find_all(["b", "strong"]):
            candidate = b.get_text(" ", strip=True)
            # Skip the label rows ("CC", "Deputy Chairman", underscored blanks).
            if not candidate or len(candidate) < 3:
                continue
            if "_" in candidate:
                continue
            if candidate.lower() in ("cc", "deputy chairman", "chairman", "signed on behalf of the labour court"):
                continue
            return candidate
    return None


def _acts_cited(body_text: str) -> list[str]:
    """De-duped list of "<Words> Act[s] YYYY[ to YYYY]" matches, preserving
    first-encounter order.  Long-form phrasing is preferred (Mongo queries
    can normalise further downstream)."""
    hits: list[str] = []
    seen: set[str] = set()
    for m in _ACT.finditer(body_text):
        cleaned = re.sub(r"\s+", " ", m.group(1)).strip(" ,;:")
        # Drop lead-in artifacts like "the " from "the Terms of Employment Act 1994".
        cleaned = re.sub(r"^(the\s+|of\s+the\s+|under\s+the\s+)", "", cleaned, flags=re.IGNORECASE)
        key = cleaned.lower()
        if key not in seen and len(cleaned) >= 8:
            seen.add(key)
            hits.append(cleaned)
    return hits


def _monetary_amounts(body_text: str) -> list[float]:
    """Distinct € amounts named anywhere in the body, first-encounter
    order.  Not necessarily *awards* — the caller must consult surrounding
    text for that (e.g. a €26,000 recruitment fee is claimed, not awarded)."""
    amounts: list[float] = []
    seen: set[float] = set()
    for m in _MONEY.finditer(body_text):
        raw = m.group(1).replace(",", "").replace(" ", "")
        try:
            value = float(raw)
        except ValueError:
            continue
        if value in seen:
            continue
        seen.add(value)
        amounts.append(value)
    return amounts


# ---------------------------------------------------------------------------
# Date parsing
# ---------------------------------------------------------------------------


def _parse_any_date(text: str) -> date | None:
    m = _LONG_DATE.search(text)
    if m:
        day, month_name, year = m.groups()
        month = _MONTHS[month_name.lower()]
        try:
            return date(int(year), month, int(day))
        except ValueError:
            return None
    return _parse_short_date(text)


def _parse_short_date(text: str) -> date | None:
    m = _SHORT_DATE.search(text)
    return _short_date_from_match(m)


def _last_short_date(text: str) -> date | None:
    last: date | None = None
    for m in _SHORT_DATE.finditer(text):
        parsed = _short_date_from_match(m)
        if parsed:
            last = parsed
    return last


def _short_date_from_match(m: re.Match | None) -> date | None:
    if m is None:
        return None
    day, month, year = m.groups()
    try:
        return date(int(year), int(month), int(day))
    except ValueError:
        return None
