"""Firecrawl API client with Supabase audit logging.

Wraps the standalone firecrawl functions (scrape_url, crawl_url) and adds
pre/post API call logging to the Supabase log table.
"""

import sys
import time
import logging
from pathlib import Path

# Add parent lib directory to path for the local modules
_PARENT_LIB = str(Path(__file__).resolve().parent.parent.parent / "lib")
if _PARENT_LIB not in sys.path:
    sys.path.insert(0, _PARENT_LIB)

import firecrawl_local

logger = logging.getLogger(__name__)


class FirecrawlAuditClient:
    """Firecrawl client with pre/post audit logging to Supabase."""

    def __init__(self, api_key: str, supabase_client, log_table: str):
        self.api_key = api_key
        self.sb = supabase_client
        self.log_table = log_table

    def scrape_url(self, url: str, domain: str, pass_name: str) -> dict:
        """Scrape a URL with audit logging."""
        log_id = self.sb.log_api_start(
            self.log_table, domain, pass_name, "firecrawl", f"scrape {url}"
        )
        start = time.monotonic()
        try:
            result = firecrawl_local.scrape_url(url, self.api_key)
            elapsed_ms = int((time.monotonic() - start) * 1000)
            success = result["status"] == "success"
            self.sb.log_api_end(
                self.log_table, log_id,
                http_status=200 if success else None,
                response_time_ms=elapsed_ms,
                success=success,
                error_message=result.get("error"),
            )
            return result
        except Exception as e:
            elapsed_ms = int((time.monotonic() - start) * 1000)
            self.sb.log_api_end(
                self.log_table, log_id,
                http_status=None,
                response_time_ms=elapsed_ms,
                success=False,
                error_code="EXCEPTION",
                error_message=str(e)[:500],
            )
            raise

    def crawl_url(self, url: str, domain: str, pass_name: str, **kwargs) -> list[dict]:
        """Crawl a URL with audit logging."""
        log_id = self.sb.log_api_start(
            self.log_table, domain, pass_name, "firecrawl", f"crawl {url}"
        )
        start = time.monotonic()
        try:
            pages = firecrawl_local.crawl_url(url, self.api_key, **kwargs)
            elapsed_ms = int((time.monotonic() - start) * 1000)
            self.sb.log_api_end(
                self.log_table, log_id,
                http_status=200,
                response_time_ms=elapsed_ms,
                success=True,
                response_preview=f"{len(pages)} pages crawled",
            )
            return pages
        except Exception as e:
            elapsed_ms = int((time.monotonic() - start) * 1000)
            self.sb.log_api_end(
                self.log_table, log_id,
                http_status=None,
                response_time_ms=elapsed_ms,
                success=False,
                error_code="EXCEPTION",
                error_message=str(e)[:500],
            )
            raise
