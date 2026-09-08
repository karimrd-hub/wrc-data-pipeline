import re
from datetime import datetime, timezone

import scrapy

from wrc_pipeline.scrapers.items import OigItem

_BROWSE_URL = "https://oig.hhs.gov/compliance/advisory-opinions/browse/"


class OigSpider(scrapy.Spider):
    """Scrapes OIG Advisory Opinions for a single year partition.

    Invocation:
        scrapy crawl oig -a year=2024

    Request flow per opinion:
        browse page (?year-posted=YYYY)
          → detail page (/compliance/advisory-opinions/{ID}/)
              → PDF #0, PDF #1, ... (cascade until all downloaded)
                  → yield OigItem

    All PDFs for an opinion are downloaded sequentially via a cascade of
    Requests so that Scrapy's throttle and retry machinery applies to each
    one. _pdf_bytes on the item accumulates bytes (or None on failure)
    in the same order as documents[].
    """

    name = "oig"
    allowed_domains = ["oig.hhs.gov"]

    custom_settings = {
        "ITEM_PIPELINES": {
            "wrc_pipeline.scrapers.pipelines.OigStoragePipeline": 300,
        },
    }

    def __init__(self, year: str | None = None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        if not year:
            raise ValueError("year is required (e.g. -a year=2024)")
        self.year = int(year)
        self.total_found: int = 0
        self.http_failures: int = 0

    async def start(self):
        yield scrapy.Request(
            f"{_BROWSE_URL}?year-posted={self.year}",
            callback=self.parse_browse,
            errback=self.errback_request,
        )

    def parse_browse(self, response):
        cards = response.css("li.usa-card.card--list")
        self.total_found += len(cards)

        # Log only on the first page so the event isn't duplicated for paginated years.
        if "page=" not in response.url:
            self.logger.info(
                "browse_parsed",
                extra={
                    "event": "browse_parsed",
                    "year": self.year,
                    "url": response.url,
                },
            )

        for card in cards:
            raw_id = card.css("h2.usa-card__heading a::text").get("").strip()
            opinion_id = re.sub(r"^AO\s+", "", raw_id).strip()
            detail_href = card.css("h2.usa-card__heading a::attr(href)").get("").strip()
            summary = card.css("div.usa-card__body p::text").get("").strip()
            posted_raw = card.css("span.text-base-dark::text").get("").strip()
            posted_date = re.sub(r"^Posted\s+", "", posted_raw).strip()

            if not opinion_id or not detail_href:
                self.logger.warning(
                    "record_skipped",
                    extra={
                        "event": "record_skipped",
                        "reason": "missing_id_or_href",
                        "raw_id": raw_id,
                        "year": self.year,
                        "url": response.url,
                    },
                )
                continue

            item = OigItem(
                opinion_id=opinion_id,
                year=self.year,
                summary=summary,
                posted_date=posted_date,
                detail_url=response.urljoin(detail_href),
                scraped_at=datetime.now(timezone.utc).isoformat(timespec="seconds"),
            )

            yield scrapy.Request(
                item["detail_url"],
                callback=self.parse_detail,
                errback=self.errback_request,
                meta={"item": item},
            )

        # Follow next page if the browse results are paginated (>20 opinions/year).
        next_href = response.css("a.pagination-next::attr(href)").get()
        if next_href:
            yield scrapy.Request(
                response.urljoin(next_href),
                callback=self.parse_browse,
                errback=self.errback_request,
            )

    def parse_detail(self, response):
        item: OigItem = response.meta["item"]

        # dt/dd pairs in the pep-metadata definition list
        terms: dict[str, str] = {}
        dts = response.css("dl.pep-metadata dt.pep-metadata__term")
        dds = response.css("dl.pep-metadata dd.pep-metadata__def")
        for dt, dd in zip(dts, dds):
            key = dt.css("::text").get("").strip()
            val = dd.css("::text").get("").strip()
            if key:
                terms[key] = val

        item["status"] = terms.get("Status")
        item["outcome"] = terms.get("Outcome")
        item["last_updated"] = terms.get("Last updated")

        # Documents — all PDF blocks on the page, in page order
        documents = []
        for doc in response.css("div.pep-document"):
            href = doc.css("a.pep-document__link::attr(href)").get("").strip()
            label = doc.css("a.pep-document__link::text").get("").strip()
            size_str = doc.css("p.pep-document__metadata::text").get("").strip()
            if href:
                documents.append(
                    {
                        "url": response.urljoin(href),
                        "label": label,
                        "file_size_str": size_str,
                    }
                )
        item["documents"] = documents

        # Updates section — timestamped post-publication notices
        update_lis = response.xpath(
            "//h2[normalize-space(text())='Updates']"
            "/following-sibling::ul[1]/li"
        )
        updates = []
        for li in update_lis:
            date_attr = li.css("time::attr(datetime)").get("")
            date_str = date_attr[:10] if date_attr else li.css("time::text").get("").strip()
            text = li.css("p::text").get("").strip()
            if text:
                updates.append({"date": date_str or None, "text": text})
        item["updates"] = updates if updates else None

        item["_pdf_bytes"] = []

        if not documents:
            self.logger.warning(
                "no_documents_found",
                extra={
                    "event": "no_documents_found",
                    "opinion_id": item["opinion_id"],
                    "year": self.year,
                    "url": response.url,
                },
            )
            yield item
            return

        yield scrapy.Request(
            documents[0]["url"],
            callback=self.parse_pdf,
            errback=self.errback_pdf,
            meta={"item": item, "pdf_index": 0},
        )

    def parse_pdf(self, response):
        item: OigItem = response.meta["item"]
        idx: int = response.meta["pdf_index"]

        item["_pdf_bytes"].append(response.body)

        next_idx = idx + 1
        if next_idx < len(item["documents"]):
            yield scrapy.Request(
                item["documents"][next_idx]["url"],
                callback=self.parse_pdf,
                errback=self.errback_pdf,
                meta={"item": item, "pdf_index": next_idx},
            )
        else:
            yield item

    def errback_pdf(self, failure):
        """A single PDF download failed — log it, mark the slot as None,
        and continue the cascade so the remaining PDFs are still collected."""
        request = failure.request
        item: OigItem = request.meta["item"]
        idx: int = request.meta["pdf_index"]
        response = getattr(failure.value, "response", None)

        self.logger.error(
            "pdf_download_failed",
            extra={
                "event": "pdf_download_failed",
                "opinion_id": item["opinion_id"],
                "pdf_index": idx,
                "url": request.url,
                "status": response.status if response else None,
                "error": type(failure.value).__name__,
                "error_message": str(failure.value)[:300],
            },
        )

        item["_pdf_bytes"].append(None)

        next_idx = idx + 1
        if next_idx < len(item["documents"]):
            yield scrapy.Request(
                item["documents"][next_idx]["url"],
                callback=self.parse_pdf,
                errback=self.errback_pdf,
                meta={"item": item, "pdf_index": next_idx},
            )
        else:
            yield item

    def errback_request(self, failure):
        """Browse or detail page request failed after all retries."""
        self.http_failures += 1
        request = failure.request
        response = getattr(failure.value, "response", None)
        item = request.meta.get("item")
        self.logger.error(
            "record_failed",
            extra={
                "event": "record_failed",
                "url": request.url,
                "status": response.status if response else None,
                "error": type(failure.value).__name__,
                "error_message": str(failure.value)[:300],
                "opinion_id": item["opinion_id"] if item else None,
                "year": self.year,
            },
        )
