"""Standalone Linkup API wrapper for booking detection.

Provides a minimal, dependency-light interface to the Linkup search API
with retry logic and structured response parsing. No database or Supabase
dependencies -- just ``requests``.
"""

import json
import time
import logging
from typing import Dict, Optional

import requests

__all__ = ["search_booking", "fill_prompt"]

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_BASE_URL = "https://api.linkup.so/v1/search"
_MAX_ATTEMPTS = 3
_RETRY_BASE_SECONDS = 2
_REQUEST_TIMEOUT = 60  # seconds


# ---------------------------------------------------------------------------
# Public helpers
# ---------------------------------------------------------------------------


def fill_prompt(prompt_template: str, domain: str) -> str:
    """Replace ``{domain}`` placeholders in a prompt template.

    Args:
        prompt_template: Raw prompt text containing ``{domain}`` placeholders.
        domain: The domain value to substitute in.

    Returns:
        The prompt string with all ``{domain}`` occurrences replaced.
    """
    return prompt_template.replace("{domain}", domain)


# ---------------------------------------------------------------------------
# Core search function
# ---------------------------------------------------------------------------


def search_booking(
    domain: str,
    prompt_text: str,
    api_key: str,
    depth: str = "deep",
) -> Dict:
    """Call the Linkup search API and return a normalised booking-detection result.

    The caller is responsible for filling any ``{domain}`` placeholders in
    *prompt_text* before passing it here (see :func:`fill_prompt`).

    Args:
        domain: The website domain to search (used in ``includedDomains``).
        prompt_text: The fully-rendered search prompt.
        api_key: Linkup API key.
        depth: Search depth -- ``"standard"`` or ``"deep"`` (default ``"deep"``).

    Returns:
        A dict with the following shape::

            {
                "status": "success" | "error",
                "has_booking": bool | None,
                "booking_platform": str | None,
                "reasoning": str | None,
                "raw_response": dict,
                "error": str | None,
            }

    Retries up to 3 times with exponential backoff on 429 / 5xx errors.
    Each individual request has a 60-second timeout.
    """
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }

    payload = {
        "q": prompt_text,
        "depth": depth,
        "outputType": "structured",
        "structuredOutputSchema": json.dumps({
            "type": "object",
            "properties": {
                "has_booking": {
                    "type": "boolean",
                    "description": "Whether the domain has an online booking system",
                },
                "booking_platform": {
                    "type": "string",
                    "description": "Name of the booking platform detected, or null if none",
                },
                "reasoning": {
                    "type": "string",
                    "description": "Brief explanation of how the conclusion was reached",
                },
            },
            "required": ["has_booking", "booking_platform", "reasoning"],
        }),
        "includedDomains": [domain],
    }

    last_error: Optional[str] = None

    for attempt in range(1, _MAX_ATTEMPTS + 1):
        try:
            logger.info(
                "Linkup API request: domain=%s depth=%s attempt=%d/%d",
                domain, depth, attempt, _MAX_ATTEMPTS,
            )

            response = requests.post(
                _BASE_URL,
                json=payload,
                headers=headers,
                timeout=_REQUEST_TIMEOUT,
            )

            # ----- retryable HTTP errors -----
            if response.status_code == 429 or response.status_code >= 500:
                last_error = f"HTTP {response.status_code}: {response.text[:300]}"
                logger.warning(
                    "Linkup API %d (attempt %d/%d): %s",
                    response.status_code, attempt, _MAX_ATTEMPTS, last_error,
                )
                if attempt < _MAX_ATTEMPTS:
                    _backoff(attempt)
                continue

            # ----- non-retryable HTTP errors -----
            response.raise_for_status()

            # ----- success -----
            data = response.json()
            logger.info(
                "Linkup API success: domain=%s depth=%s attempt=%d",
                domain, depth, attempt,
            )
            return _parse_response(data)

        except requests.exceptions.HTTPError as exc:
            # 4xx (non-429) -- don't retry
            last_error = str(exc)
            logger.error("Linkup API client error: %s", last_error)
            return _error_result(last_error)

        except requests.exceptions.Timeout:
            last_error = f"Request timed out after {_REQUEST_TIMEOUT}s"
            logger.warning(
                "Linkup API TIMEOUT (attempt %d/%d)", attempt, _MAX_ATTEMPTS,
            )
            if attempt < _MAX_ATTEMPTS:
                _backoff(attempt)

        except requests.exceptions.RequestException as exc:
            last_error = f"Request error: {exc}"
            logger.warning(
                "Linkup API error (attempt %d/%d): %s",
                attempt, _MAX_ATTEMPTS, last_error,
            )
            if attempt < _MAX_ATTEMPTS:
                _backoff(attempt)

    # All attempts exhausted
    logger.error("Linkup API failed after %d attempts: %s", _MAX_ATTEMPTS, last_error)
    return _error_result(last_error)


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _backoff(attempt: int) -> None:
    """Sleep with exponential backoff capped at 60 s."""
    wait = min(_RETRY_BASE_SECONDS * (2 ** (attempt - 1)), 60)
    logger.info("Retrying in %ds ...", wait)
    time.sleep(wait)


def _error_result(error_msg: str) -> Dict:
    """Return a standardised error envelope."""
    return {
        "status": "error",
        "has_booking": None,
        "booking_platform": None,
        "reasoning": None,
        "raw_response": {},
        "error": error_msg,
    }


def _parse_response(data: dict) -> Dict:
    """Extract booking-detection fields from Linkup's structured output.

    Linkup returns structured output either as a top-level dict or nested
    inside ``data["answer"]`` / ``data["results"]``. This function tries
    several common shapes and falls back gracefully.

    The fields we look for (case-insensitive keys):
        * ``has_booking`` / ``hasBooking`` -- bool
        * ``booking_platform`` / ``bookingPlatform`` -- str
        * ``reasoning`` / ``explanation`` -- str
    """
    try:
        structured = _extract_structured(data)

        has_booking = _get_field(structured, ["has_booking", "hasBooking"])
        booking_platform = _get_field(structured, ["booking_platform", "bookingPlatform"])
        reasoning = _get_field(structured, ["reasoning", "explanation"])

        # Coerce has_booking to bool if it came back as a string
        if isinstance(has_booking, str):
            has_booking = has_booking.strip().lower() in ("true", "yes", "1")

        return {
            "status": "success",
            "has_booking": has_booking,
            "booking_platform": booking_platform,
            "reasoning": reasoning,
            "raw_response": data,
            "error": None,
        }
    except Exception as exc:
        logger.warning("Failed to parse Linkup response: %s", exc)
        return {
            "status": "success",
            "has_booking": None,
            "booking_platform": None,
            "reasoning": None,
            "raw_response": data,
            "error": None,
        }


def _extract_structured(data: dict) -> dict:
    """Dig into Linkup's response to find the structured payload.

    Tries (in order):
        1. ``data`` itself if it contains booking-related keys
        2. ``data["answer"]`` (string → try JSON-parse, or dict)
        3. First element of ``data["results"]``
    """
    booking_keys = {"has_booking", "hasBooking", "booking_platform", "bookingPlatform"}

    # 1. Top-level already has the fields
    if booking_keys & set(data.keys()):
        return data

    # 2. Nested under "answer"
    answer = data.get("answer")
    if isinstance(answer, dict):
        return answer
    if isinstance(answer, str):
        try:
            parsed = json.loads(answer)
            if isinstance(parsed, dict):
                return parsed
        except (json.JSONDecodeError, TypeError):
            pass

    # 3. First result entry
    results = data.get("results")
    if isinstance(results, list) and results:
        first = results[0]
        if isinstance(first, dict):
            # Check nested "content" field
            content = first.get("content")
            if isinstance(content, str):
                try:
                    parsed = json.loads(content)
                    if isinstance(parsed, dict):
                        return parsed
                except (json.JSONDecodeError, TypeError):
                    pass
            return first

    # Fallback: return the whole thing
    return data


def _get_field(d: dict, keys: list) -> Optional[str]:
    """Return the first matching value from *d* for the given key variants."""
    for key in keys:
        if key in d:
            return d[key]
    # Try case-insensitive lookup as last resort
    lower_map = {k.lower(): v for k, v in d.items()}
    for key in keys:
        if key.lower() in lower_map:
            return lower_map[key.lower()]
    return None
