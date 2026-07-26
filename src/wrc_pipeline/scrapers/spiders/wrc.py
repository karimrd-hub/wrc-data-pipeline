import math
import re
from datetime import datetime, timezone
from urllib.parse import urlencode

import scrapy

from wrc_pipeline.config.settings import settings
from wrc_pipeline.scrapers.items import WrcItem
from wrc_pipeline.scrapers.utils.bodies import BODIES
from wrc_pipeline.scrapers.utils.dates import iter_partitions, parse_iso, to_site_date


SEARCH_PATH = "/en/search/"
RESULTS_PER_PAGE = 10  # site pagination window
TOTAL_RE = re.compile(r"of\s+(\d+)\s+results", re.IGNORECASE)

# Extensions we treat as "the actual document" when a search-result page
# opens onto an HTML wrapper that embeds a link (task req 6a). Matched
# against the URL path only — query strings/fragments are ignored.
_DOCUMENT_EXTS: tuple[str, ...] = (".pdf", ".doc", ".docx")

# Scope for the anchor scan: keep it inside the decision content subtree so
# we don't chase site-wide chrome links (footer "annual report" PDFs, etc.).
# Same selectors as ``transform/cleaner.py``.
_DOC_LINK_SCOPE = "div.col-sm-9, div.content"


class WrcSpider(scrapy.Spider):
    """Scrapes workplacerelations.ie decision search results across
    (body × partition) slices between ``start_date`` and ``end_date``
    (inclusive). Partition granularity is env-driven — see
    ``SCRAPER_PARTITION_SIZE`` (monthly / weekly / daily)."""

    name = "wrc"
    allowed_domains = ["workplacerelations.ie"]

    def __init__(
        self,
        start_date: str | None = None,
        end_date: str | None = None,
        bodies: str | None = None,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        if not start_date or not end_date:
            raise ValueError("start_date and end_date are required (YYYY-MM-DD)")
        self.range_start = parse_iso(start_date)
        self.range_end = parse_iso(end_date)
        if bodies:
            self.body_ids = [int(b) for b in bodies.split(",") if b.strip()]
        else:
            self.body_ids = list(BODIES.keys())
        for bid in self.body_ids:
            if bid not in BODIES:
                raise ValueError(f"Unknown body id: {bid} (known: {list(BODIES)})")
        # (body_name, partition_date_iso) -> total records reported by search page.
        # Read by StoragePipeline.close_spider to emit partition_summary events.
        self.partition_totals: dict[tuple[str, str], int] = {}
        # (body_name, partition_date_iso) -> count of rows we couldn't parse
        # at the spider stage. Merged into partition_summary["row_parse_failed"]
        # so the found/scraped/failed equation reconciles from the summary
        # alone — not just from grepping record_failed events.
        self.partition_row_failures: dict[tuple[str, str], int] = {}

    async def start(self):
        size = settings.scraper.partition_size
        for body_id in self.body_ids:
            for partition_date, p_start, p_end in iter_partitions(
                self.range_start, self.range_end, size
            ):
                url = self._search_url(body_id, p_start, p_end, page=1)
                yield scrapy.Request(
                    url,
                    callback=self.parse_search,
                    errback=self.errback_request,
                    meta={
                        "body_id": body_id,
                        "body_name": BODIES[body_id],
                        "partition_date": partition_date.isoformat(),
                        "partition_end": p_end.isoformat(),
                        "is_first_page": True,
                    },
                )

    def parse_search(self, response):
        meta = response.meta

        if meta.get("is_first_page"):
            total = self._parse_total(response)
            self.partition_totals[(meta["body_name"], meta["partition_date"])] = total
            self.logger.info(
                "partition_started",
                extra={
                    "event": "partition_started",
                    "body": meta["body_name"],
                    "body_id": meta["body_id"],
                    "partition_date": meta["partition_date"],
                    "partition_end": meta["partition_end"],
                    "total_found": total,
                    "url": response.url,
                },
            )
            if total > RESULTS_PER_PAGE:
                total_pages = math.ceil(total / RESULTS_PER_PAGE)
                body_id = meta["body_id"]
                # parse dates back from query string so we don't drift on month boundaries
                for page in range(2, total_pages + 1):
                    url = self._search_url_from_first(response.url, page)
                    yield scrapy.Request(
                        url,
                        callback=self.parse_search,
                        errback=self.errback_request,
                        meta={
                            "body_id": body_id,
                            "body_name": meta["body_name"],
                            "partition_date": meta["partition_date"],
                            "partition_end": meta["partition_end"],
                            "is_first_page": False,
                        },
                    )

        for row_index, row in enumerate(response.css("li.each-item")):
            try:
                title_a = row.css("h2.title > a")
                title = (title_a.css("::text").get() or "").strip()
                # refNO is the canonical id but is occasionally empty in the source HTML
                # (observed on Labour Court page 4 for 2026-01). Fall back to h2 text.
                identifier = (row.css("span.refNO::text").get() or "").strip() or title
                detail_href = (title_a.attrib.get("href") or "").strip()
                description = " ".join(
                    t.strip() for t in row.css("p.description::text").getall() if t.strip()
                )
                date_text = (row.css("span.date::text").get() or "").strip()
            except AttributeError as exc:
                # A shape change in a single row shouldn't take down the whole
                # search page — log the row (with index so a human can locate
                # it in the HTML) and keep going.
                key = (meta["body_name"], meta["partition_date"])
                self.partition_row_failures[key] = (
                    self.partition_row_failures.get(key, 0) + 1
                )
                self.logger.error(
                    "record_failed",
                    extra={
                        "event": "record_failed",
                        "reason": "row_parse_failed",
                        "row_index": row_index,
                        "url": response.url,
                        "body": meta["body_name"],
                        "partition_date": meta["partition_date"],
                        "error": type(exc).__name__,
                        "error_message": str(exc)[:300],
                    },
                )
                continue

            if not identifier or not detail_href:
                self.logger.warning(
                    "record_skipped",
                    extra={
                        "event": "record_skipped",
                        "reason": "missing_identifier_or_href",
                        "body": meta["body_name"],
                        "partition_date": meta["partition_date"],
                        "url": response.url,
                    },
                )
                continue

            item = WrcItem(
                identifier=identifier,
                title=title,
                description=description,
                date=date_text,
                partition_date=meta["partition_date"],
                partition_end=meta["partition_end"],
                body=meta["body_name"],
                body_id=meta["body_id"],
                source_url=response.url,
                doc_url=response.urljoin(detail_href),
                scraped_at=datetime.now(timezone.utc).isoformat(timespec="seconds"),
            )
            yield scrapy.Request(
                item["doc_url"],
                callback=self.parse_detail,
                errback=self.errback_request,
                meta={
                    "item": item,
                    "body_name": meta["body_name"],
                    "partition_date": meta["partition_date"],
                },
            )

    def parse_detail(self, response):
        """Store the response bytes, or — if the response is an HTML wrapper
        that embeds a PDF/DOC link — follow that link and store the actual
        document (task req 6a). ``is_document_fetch`` in meta breaks any
        cycle if the embedded URL itself resolves to HTML for some reason.
        """
        item: WrcItem = response.meta["item"]
        content_type = response.headers.get("Content-Type", b"").decode(
            "latin-1", errors="replace"
        )

        if (
            content_type.lower().startswith("text/html")
            and not response.meta.get("is_document_fetch")
        ):
            doc_href = self._find_document_link(response)
            if doc_href:
                doc_url = response.urljoin(doc_href)
                item["doc_url"] = doc_url  # metadata should point at what we actually stored
                self.logger.info(
                    "document_link_followed",
                    extra={
                        "event": "document_link_followed",
                        "identifier": item.get("identifier"),
                        "wrapper_url": response.url,
                        "document_url": doc_url,
                        "body": item.get("body"),
                        "partition_date": item.get("partition_date"),
                    },
                )
                yield scrapy.Request(
                    doc_url,
                    callback=self.parse_detail,
                    errback=self.errback_request,
                    meta={
                        "item": item,
                        "body_name": item.get("body"),
                        "partition_date": item.get("partition_date"),
                        "is_document_fetch": True,
                    },
                )
                return

        item["content_type"] = content_type
        item["_body_bytes"] = response.body
        yield item

    def _find_document_link(self, response) -> str | None:
        """First anchor inside the decision subtree whose URL path ends in
        one of ``_DOCUMENT_EXTS``, or ``None`` if the page is a
        self-contained inline decision. Chrome links (footer / nav) are
        ignored by construction because we scope to ``col-sm-9`` / ``content``.
        """
        scope = response.css(_DOC_LINK_SCOPE)
        anchors = scope.css("a[href]") if scope else response.css("a[href]")
        for anchor in anchors:
            href = (anchor.attrib.get("href") or "").strip()
            if not href:
                continue
            path_only = href.split("?", 1)[0].split("#", 1)[0].lower()
            if path_only.endswith(_DOCUMENT_EXTS):
                return href
        return None

    def errback_request(self, failure):
        """Log any request that dies after retries so the reviewer can
        reconcile ``total_found`` against what actually reached the pipeline
        (task req 10: failed downloads with URLs and error codes)."""
        request = failure.request
        response = getattr(failure.value, "response", None)
        status = response.status if response is not None else None
        meta = request.meta or {}
        item = meta.get("item")
        self.logger.error(
            "record_failed",
            extra={
                "event": "record_failed",
                "url": request.url,
                "status": status,
                "error": type(failure.value).__name__,
                "error_message": str(failure.value)[:300],
                "body": meta.get("body_name"),
                "partition_date": meta.get("partition_date"),
                "identifier": item.get("identifier") if item else None,
            },
        )

    # -- helpers -------------------------------------------------------------

    def _search_url(self, body_id: int, p_start, p_end, page: int) -> str:
        qs = urlencode({
            "decisions": 1,
            "body": body_id,
            "pageNumber": page,
            "from": to_site_date(p_start),
            "to": to_site_date(p_end),
        })
        return f"https://www.workplacerelations.ie{SEARCH_PATH}?{qs}"

    def _search_url_from_first(self, first_page_url: str, page: int) -> str:
        # swap only pageNumber; keeps `from`/`to` verbatim
        return re.sub(r"pageNumber=\d+", f"pageNumber={page}", first_page_url)

    def _parse_total(self, response) -> int:
        # The "of N results" string is our only signal for pagination. If it
        # disappears or its format drifts we silently miss every page after
        # the first, so the miss must be loud and the fallback must at least
        # account for the rows we CAN see on page 1.
        searchhead = " ".join(response.css("div.searchhead::text").getall()).strip()
        match = TOTAL_RE.search(searchhead)
        if match is not None:
            return int(match.group(1))

        row_count = len(response.css("li.each-item"))
        self.logger.warning(
            "total_count_unparseable",
            extra={
                "event": "total_count_unparseable",
                "searchhead": searchhead[:200],
                "rows_on_page": row_count,
                "url": response.url,
            },
        )
        return row_count
