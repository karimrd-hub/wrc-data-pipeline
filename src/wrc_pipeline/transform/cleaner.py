"""HTML "relevant content" extractor (task req transform/1/c/ii/1).

The transformation step must reduce a full WRC decision page to just the case
material — the identifier heading, the "content" block (parties, division,
subject, background, decision body, signatures) — and drop everything the
screenshot in ``docs/task.md`` marks as chrome (language toggle, font-size
widget, header banner, breadcrumbs, "Return to Search" link, cookie bar,
footer, scripts, social banner).

Selectors (verified against a real Labour Court decision page,
``PWD2642`` 2026-01):

* ``div.col-sm-9`` — outer wrapper of the case content. Sibling ``col-sm-3``
  is the "Return to Search" side-column, so grabbing ``col-sm-9`` cleanly
  excludes it.
* ``h1.page-title`` — identifier heading inside ``col-sm-9``.
* ``div.content`` — case body (tables, paragraphs, signatures) inside
  ``col-sm-9``.

Anything else on the page (``<header>``, ``<footer>``, ``<script>``,
``.top-header``, ``.searchbanner``, ``.social-banner``, cookie bar, skip
link) is chrome by construction — never reached because we only serialize
the extracted ``col-sm-9`` subtree.

Fallback: if ``col-sm-9`` is missing (site layout change), we fall back to
``div.content`` alone. If that too is missing we raise
``ContentNotFoundError`` so the runner can log ``record_failed`` and skip
the object rather than write empty output.

Determinism: given identical input bytes and the same cleaner version, the
output bytes are identical — the caller relies on this for idempotency.
"""

from __future__ import annotations

from bs4 import BeautifulSoup, Comment, Tag
from bs4.builder import ParserRejectedMarkup


class ContentNotFoundError(Exception):
    """Raised when the payload can't yield a usable content subtree —
    either lxml rejected it as malformed markup, or it parsed cleanly but
    neither ``col-sm-9`` nor ``div.content`` was present."""


# Tags we drop wholesale from within the extracted subtree. All are chrome
# or executable content that carries no legal information.
_STRIP_TAGS = ("script", "style", "noscript")

# CSS selectors of chrome elements that can appear inside the extracted
# subtree (e.g. the "Return to Search" back-link lives inside ``col-sm-9``
# on some pages).
_STRIP_SELECTORS = ("a.return-to-search",)

_DOCUMENT_TEMPLATE = (
    "<!DOCTYPE html>\n"
    '<html lang="en"><head><meta charset="utf-8">'
    "<title>{title}</title></head><body>{content}</body></html>"
)


def clean_html(payload: bytes, identifier: str) -> bytes:
    """Return a minimal standalone HTML document containing only the
    relevant content from ``payload``.

    Parameters
    ----------
    payload
        Raw HTML bytes from the Landing Zone (as scraped by the spider).
    identifier
        Canonical record identifier — used as the document ``<title>``.
    """
    try:
        soup = BeautifulSoup(payload, "lxml")
    except ParserRejectedMarkup as exc:
        raise ContentNotFoundError(
            f"lxml rejected {len(payload)} bytes as malformed markup"
        ) from exc

    extracted = _extract_relevant(soup)
    if extracted is None:
        raise ContentNotFoundError(
            "neither div.col-sm-9 nor div.content was present in the payload"
        )

    _prune(extracted)

    document = _DOCUMENT_TEMPLATE.format(
        title=_escape(identifier),
        content=extracted.decode_contents(),
    )
    return document.encode("utf-8")


def _extract_relevant(soup: BeautifulSoup) -> Tag | None:
    # Prefer the outer wrapper (contains h1.page-title *and* .content) so the
    # identifier heading is preserved. Fall back to .content on layout drift.
    return soup.select_one("div.col-sm-9") or soup.select_one("div.content")


def _prune(root: Tag) -> None:
    for tag in root.find_all(_STRIP_TAGS):
        tag.decompose()
    for selector in _STRIP_SELECTORS:
        for tag in root.select(selector):
            tag.decompose()
    for comment in root.find_all(string=lambda s: isinstance(s, Comment)):
        comment.extract()
    for p in root.find_all("p"):
        # Empty paragraph pattern seen in the source (``<p></p>`` between
        # sections). Zero information, so removing tightens the diff and
        # keeps the cleaned document readable.
        if not p.get_text(strip=True) and not p.find(True):
            p.decompose()


def _escape(text: str) -> str:
    # Minimal HTML escape for the identifier landing in <title>. Identifiers
    # in the wild (``TED2616``, ``ADJ-00027821``) never contain these chars,
    # but escaping is defensive.
    return (
        text.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
    )
