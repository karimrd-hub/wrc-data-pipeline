"""Body-id → human-readable-name mapping.

Populated from ``SCRAPER_BODIES`` (see ``.env.example``) so a reviewer can
scrape a subset of tribunals without editing code. Default matches the four
bodies exposed in the WRC search UI at ingestion time.
"""

from wrc_pipeline.config.settings import settings

BODIES: dict[int, str] = settings.scraper.bodies


def body_slug(name: str) -> str:
    return name.lower().replace(" ", "_")
