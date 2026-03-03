"""Standalone Firecrawl API wrapper using requests only.

Provides two functions for scraping and crawling URLs via the Firecrawl v1 API.
No external SDK, database, or job queue dependencies.
"""

import time
import logging
import requests
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)

__all__ = ["scrape_url", "crawl_url"]

BASE_URL = "https://api.firecrawl.dev/v1"
MAX_ATTEMPTS = 3
RETRY_BASE_SECONDS = 2


def _headers(api_key: str) -> Dict[str, str]:
    """Build request headers with Bearer auth."""
    return {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }


def _is_retryable(status_code: Optional[int]) -> bool:
    """Return True if the HTTP status code warrants a retry."""
    if status_code is None:
        return True
    return status_code == 429 or status_code >= 500


def scrape_url(
    url: str,
    api_key: str,
    formats: Optional[List[str]] = None,
) -> Dict:
    """Scrape a single URL via Firecrawl's synchronous scrape endpoint.

    Args:
        url: The target URL to scrape.
        api_key: Firecrawl API key.
        formats: Output formats to request (default: ['markdown', 'html']).

    Returns:
        Dict with keys: markdown, html, metadata, status, error.
        - status is "success" or "error".
        - On error, markdown/html may be empty strings and error contains detail.
    """
    if formats is None:
        formats = ["markdown", "html"]

    endpoint = f"{BASE_URL}/scrape"
    payload = {"url": url, "formats": formats}
    last_error: Optional[str] = None

    for attempt in range(1, MAX_ATTEMPTS + 1):
        try:
            logger.debug(
                "Scrape attempt %d/%d for %s", attempt, MAX_ATTEMPTS, url
            )
            response = requests.post(
                endpoint,
                json=payload,
                headers=_headers(api_key),
                timeout=30,
            )
            response.raise_for_status()
            body = response.json()

            data = body.get("data", {})
            return {
                "markdown": data.get("markdown", ""),
                "html": data.get("html", ""),
                "metadata": data.get("metadata", {}),
                "status": "success",
                "error": None,
            }

        except requests.exceptions.HTTPError as exc:
            status_code = exc.response.status_code if exc.response is not None else None
            last_error = f"HTTP {status_code}: {exc}"
            logger.warning(
                "Scrape HTTP error on attempt %d/%d for %s: %s",
                attempt, MAX_ATTEMPTS, url, last_error,
            )
            if not _is_retryable(status_code):
                break

        except requests.exceptions.Timeout:
            last_error = "Request timed out after 30s"
            logger.warning(
                "Scrape timeout on attempt %d/%d for %s",
                attempt, MAX_ATTEMPTS, url,
            )

        except requests.exceptions.RequestException as exc:
            last_error = f"Request error: {exc}"
            logger.warning(
                "Scrape request error on attempt %d/%d for %s: %s",
                attempt, MAX_ATTEMPTS, url, last_error,
            )

        # Exponential backoff before next attempt
        if attempt < MAX_ATTEMPTS:
            wait = RETRY_BASE_SECONDS * (2 ** (attempt - 1))
            logger.info("Retrying scrape in %ds...", wait)
            time.sleep(wait)

    logger.error("Scrape failed after %d attempts for %s: %s", MAX_ATTEMPTS, url, last_error)
    return {
        "markdown": "",
        "html": "",
        "metadata": {},
        "status": "error",
        "error": last_error,
    }


def crawl_url(
    url: str,
    api_key: str,
    include_paths: Optional[List[str]] = None,
    exclude_paths: Optional[List[str]] = None,
    limit: int = 25,
    wait_for: int = 0,
) -> List[Dict]:
    """Crawl a URL via Firecrawl's async crawl endpoint with polling.

    Starts a crawl job, then polls until completion or timeout (120s).
    Returns whatever pages have been collected, even on timeout or error.

    Args:
        url: The base URL to crawl.
        api_key: Firecrawl API key.
        include_paths: Optional list of URL path patterns to include.
        exclude_paths: Optional list of URL path patterns to exclude.
        limit: Maximum number of pages to crawl (default 25).
        wait_for: Milliseconds to wait for JS rendering per page (0 = disabled).

    Returns:
        List of dicts, each with keys: url, markdown.
    """
    # --- Build payload ---
    scrape_options: Dict = {"formats": ["markdown"]}
    if wait_for > 0:
        scrape_options["waitFor"] = wait_for

    payload: Dict = {
        "url": url,
        "limit": limit,
        "scrapeOptions": scrape_options,
    }
    if include_paths:
        payload["includePaths"] = include_paths
    if exclude_paths:
        payload["excludePaths"] = exclude_paths

    # --- Start crawl ---
    start_endpoint = f"{BASE_URL}/crawl"
    crawl_id: Optional[str] = None
    last_error: Optional[str] = None

    for attempt in range(1, MAX_ATTEMPTS + 1):
        try:
            logger.debug(
                "Crawl start attempt %d/%d for %s", attempt, MAX_ATTEMPTS, url
            )
            response = requests.post(
                start_endpoint,
                json=payload,
                headers=_headers(api_key),
                timeout=30,
            )
            response.raise_for_status()
            body = response.json()
            crawl_id = body.get("id")
            if not crawl_id:
                last_error = "Crawl response missing job ID"
                logger.error(last_error)
                return []
            logger.info("Started crawl %s for %s", crawl_id, url)
            break

        except requests.exceptions.HTTPError as exc:
            status_code = exc.response.status_code if exc.response is not None else None
            last_error = f"HTTP {status_code}: {exc}"
            logger.warning(
                "Crawl start HTTP error on attempt %d/%d for %s: %s",
                attempt, MAX_ATTEMPTS, url, last_error,
            )
            if not _is_retryable(status_code):
                break

        except requests.exceptions.Timeout:
            last_error = "Request timed out after 30s"
            logger.warning(
                "Crawl start timeout on attempt %d/%d for %s",
                attempt, MAX_ATTEMPTS, url,
            )

        except requests.exceptions.RequestException as exc:
            last_error = f"Request error: {exc}"
            logger.warning(
                "Crawl start request error on attempt %d/%d for %s: %s",
                attempt, MAX_ATTEMPTS, url, last_error,
            )

        if attempt < MAX_ATTEMPTS:
            wait = RETRY_BASE_SECONDS * (2 ** (attempt - 1))
            logger.info("Retrying crawl start in %ds...", wait)
            time.sleep(wait)

    if not crawl_id:
        logger.error("Failed to start crawl for %s: %s", url, last_error)
        return []

    # --- Poll for completion ---
    poll_endpoint = f"{BASE_URL}/crawl/{crawl_id}"
    poll_interval = 5
    max_wait = 120
    start_time = time.monotonic()
    collected_pages: List[Dict] = []

    while True:
        elapsed = time.monotonic() - start_time
        if elapsed > max_wait:
            logger.warning(
                "Crawl %s timed out after %ds; returning %d pages collected so far",
                crawl_id, int(elapsed), len(collected_pages),
            )
            return collected_pages

        try:
            poll_response = requests.get(
                poll_endpoint, headers=_headers(api_key), timeout=30
            )
            poll_response.raise_for_status()
            poll_data = poll_response.json()
        except requests.exceptions.RequestException as exc:
            logger.warning("Crawl poll error for %s: %s", crawl_id, exc)
            time.sleep(poll_interval)
            continue

        status = poll_data.get("status", "")
        pages = poll_data.get("data", [])

        # Accumulate any new pages from this poll response
        if isinstance(pages, list):
            for page in pages:
                page_url = page.get("metadata", {}).get("sourceURL", page.get("url", ""))
                page_md = page.get("markdown", "")
                if page_url and page_md:
                    collected_pages.append({"url": page_url, "markdown": page_md})

        if status == "completed":
            logger.info(
                "Crawl %s completed in %ds with %d pages",
                crawl_id, int(elapsed), len(collected_pages),
            )
            return collected_pages

        if status in ("failed", "cancelled"):
            logger.error(
                "Crawl %s %s: %s; returning %d pages collected so far",
                crawl_id, status, poll_data.get("error", "unknown"),
                len(collected_pages),
            )
            return collected_pages

        logger.debug(
            "Crawl %s: status=%s, %d pages so far (%ds elapsed)",
            crawl_id, status, len(collected_pages), int(elapsed),
        )
        time.sleep(poll_interval)
