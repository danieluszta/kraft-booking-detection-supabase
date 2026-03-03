"""
Standalone regex text scanning module.

Pure-Python reimplementation of the core scanning logic from
kraft_os/components/text_scanner/run_scanner.py, without any database
or network I/O.  Depends only on re, yaml, and logging.

Usage:
    patterns = load_patterns("path/to/patterns.yaml")
    hits     = scan_text(page_text, patterns)
    results  = scan_domains({"example.com": page_text, ...}, patterns)
"""

from __future__ import annotations

import logging
import re
from typing import Any

import yaml

__all__ = ["load_patterns", "scan_text", "scan_domains"]

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Pattern loading
# ---------------------------------------------------------------------------

def load_patterns(config_path: str) -> list[dict[str, Any]]:
    """Load and compile regex patterns from a YAML config file.

    The YAML file must contain a top-level ``patterns`` list where each
    entry has the keys ``label``, ``regex``, and ``category``.

    Args:
        config_path: Filesystem path to the YAML config file.

    Returns:
        List of dicts, each containing:
            - ``label``    (str):  Short identifier for the pattern.
            - ``regex``    (re.Pattern): Compiled regex (case-insensitive).
            - ``category`` (str):  Free-form grouping string.

    Raises:
        FileNotFoundError: If *config_path* does not exist.
        KeyError: If a pattern entry is missing a required field.
        re.error: If a regex string fails to compile.
    """
    with open(config_path, "r", encoding="utf-8") as fh:
        config = yaml.safe_load(fh)

    raw_patterns: list[dict] = config.get("patterns", [])
    compiled: list[dict[str, Any]] = []

    for entry in raw_patterns:
        label = entry["label"]
        regex_str = entry["regex"]
        category = entry.get("category", "")

        try:
            compiled_re = re.compile(regex_str, re.IGNORECASE)
        except re.error as exc:
            logger.error("Failed to compile regex for pattern '%s': %s", label, exc)
            raise

        compiled.append({
            "label": label,
            "regex": compiled_re,
            "category": category,
        })

    logger.info("Loaded %d patterns from %s", len(compiled), config_path)
    return compiled


# ---------------------------------------------------------------------------
# Single-text scanning
# ---------------------------------------------------------------------------

def scan_text(text: str, patterns: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Run every compiled pattern against *text* and return hits.

    Args:
        text:     The text content to scan.
        patterns: List of pattern dicts as returned by :func:`load_patterns`.

    Returns:
        List of hit dicts (only patterns with >= 1 match are included).
        Each hit contains:
            - ``label``        (str): Pattern label.
            - ``category``     (str): Pattern category.
            - ``matched_text`` (str): First match, truncated to 200 chars.
            - ``match_count``  (int): Total number of matches found.

        An empty list means no patterns matched.
    """
    hits: list[dict[str, Any]] = []

    for p in patterns:
        matches = p["regex"].findall(text)
        if matches:
            matched_text = str(matches[0])[:200]
            hits.append({
                "label": p["label"],
                "category": p["category"],
                "matched_text": matched_text,
                "match_count": len(matches),
            })

    return hits


# ---------------------------------------------------------------------------
# Batch scanning over domains
# ---------------------------------------------------------------------------

def scan_domains(
    domains: dict[str, str],
    patterns: list[dict[str, Any]],
) -> dict[str, list[dict[str, Any]]]:
    """Scan multiple domains' text content in one call.

    Convenience wrapper around :func:`scan_text` for batch processing.

    Args:
        domains:  Mapping of ``{domain: text_content}``.
        patterns: List of pattern dicts as returned by :func:`load_patterns`.

    Returns:
        Mapping of ``{domain: [list of hits]}`` for every input domain.
        Domains with no matches will have an empty list.
    """
    results: dict[str, list[dict[str, Any]]] = {}

    for domain, text in domains.items():
        results[domain] = scan_text(text or "", patterns)

    return results
