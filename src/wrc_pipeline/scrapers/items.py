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
