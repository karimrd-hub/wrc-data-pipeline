"""Plain-text extraction for the sibling ``{identifier}.txt`` artifact.

Every processed document gets a ``.txt`` sibling in the processed bucket
next to the (untouched) ``{identifier}.{html,pdf,doc,docx}``. The task-rule
"don't apply any transformation" for PDFs still holds — the PDF stays
byte-identical to the landing object — but a searchable-text sibling gets
produced next to it so downstream consumers (search, embeddings, LLMs)
don't need to lug an HTML/PDF parser around.

HTML  -> BS4 ``get_text("\n", strip=True)`` on the *already-cleaned* payload,
         then NFC + whitespace normalisation.
PDF   -> ``pypdf`` page-by-page. Returns ``None`` on malformed input; the
         caller logs the miss and still stores the archived PDF.
DOC   -> not extracted (no msword parser in the dep tree; ``None`` is
         returned and the caller skips the sibling).
"""

from __future__ import annotations

import io
import re
import unicodedata

from bs4 import BeautifulSoup
from pypdf import PdfReader
from pypdf.errors import PdfReadError

# Collapse runs of horizontal whitespace + blank-line runs; keeps paragraph
# breaks readable without exploding the .txt with the site's ``&nbsp;``-heavy
# spacing.
_WS_RUN = re.compile(r"[ \t]+")
_BLANK_LINE_RUN = re.compile(r"\n{3,}")


def html_to_text(payload: bytes) -> str:
    """Extract plain text from *cleaned* decision HTML.

    Caller must pass the output of ``transform.cleaner.clean_html`` (chrome
    already stripped) — we do not attempt to strip navigation here.
    """
    soup = BeautifulSoup(payload, "lxml")
    raw = soup.get_text("\n", strip=True)
    normalized = unicodedata.normalize("NFC", raw)
    normalized = _WS_RUN.sub(" ", normalized)
    normalized = _BLANK_LINE_RUN.sub("\n\n", normalized)
    return normalized.strip()


def pdf_to_text(payload: bytes) -> str | None:
    """Extract page-joined text from a PDF payload.

    Returns ``None`` if pypdf can't parse it (encrypted, malformed, empty).
    Every text page is separated by a blank line so paragraphs from
    different pages don't run together.
    """
    try:
        reader = PdfReader(io.BytesIO(payload))
    except (PdfReadError, ValueError, TypeError, KeyError, IndexError, OSError):
        # pypdf raises a small zoo of exceptions on malformed input; catch
        # them so one weird PDF doesn't tank the whole transform run.
        return None

    pages: list[str] = []
    for page in reader.pages:
        try:
            text = page.extract_text() or ""
        except (PdfReadError, ValueError, KeyError, IndexError, TypeError, OSError):
            continue
        if text.strip():
            pages.append(text)
    if not pages:
        return None
    joined = "\n\n".join(pages)
    return unicodedata.normalize("NFC", joined).strip() or None
