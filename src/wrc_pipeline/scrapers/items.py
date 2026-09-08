import scrapy

class WrcItem(scrapy.Item):
    identifier = scrapy.Field()      # unique case reference from span.refNO (e.g. "TED2616")
    title = scrapy.Field()           # heading text from h2.title (usually equals identifier on this site)
    description = scrapy.Field()     # parties block from p.description (e.g. "X AND Y")
    date = scrapy.Field()            # decision date as shown on the site, DD/MM/YYYY
    partition_date = scrapy.Field()  # first day of the (body × month) partition, ISO YYYY-MM-DD
    partition_end = scrapy.Field()   # last day of the (body × month) partition, ISO YYYY-MM-DD
    body = scrapy.Field()            # human-readable body name (e.g. "Labour Court")
    body_id = scrapy.Field()         # numeric body id used in the search URL (see utils/bodies.py)
    source_url = scrapy.Field()      # search results URL where this record was discovered
    doc_url = scrapy.Field()         # detail-page URL (also the download URL for HTML records)
    file_path = scrapy.Field()       # storage path/key where the downloaded file was written
    file_hash = scrapy.Field()       # sha256 of the downloaded file bytes, hex-encoded
    content_type = scrapy.Field()    # Content-Type header of the detail response (drives extension)
    scraped_at = scrapy.Field()      # UTC ISO-8601 timestamp of when the record was scraped
    _body_bytes = scrapy.Field()     # transient raw bytes of the document, stripped by FileStoragePipeline


class OigItem(scrapy.Item):
    opinion_id = scrapy.Field()     # e.g. "24-13" — unique identifier, derived from URL path
    year = scrapy.Field()           # partition key (int), from browse URL ?year-posted=YYYY
    status = scrapy.Field()         # Issued / Issued with Modifications / Rescinded / Terminated
    outcome = scrapy.Field()        # Favorable / Unfavorable — absent on Rescinded/Terminated
    posted_date = scrapy.Field()    # as shown on site, e.g. "December 30, 2024"
    last_updated = scrapy.Field()   # present only when opinion was modified or terminated
    summary = scrapy.Field()        # brief arrangement description from the browse card
    detail_url = scrapy.Field()     # full URL of the opinion detail page
    documents = scrapy.Field()      # list of {url, label, file_size_str} — all PDFs for this opinion
    updates = scrapy.Field()        # list of {date, text} post-publication notices, or None
    scraped_at = scrapy.Field()     # UTC ISO-8601 timestamp
    _pdf_bytes = scrapy.Field()     # transient: list of bytes (or None on download failure), parallel to documents
