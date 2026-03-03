#!/usr/bin/env python3
"""
Booking Detection Pipeline — Supabase-Backed 9-Pass Orchestrator

Reads domains from a Supabase input table, runs 9 progressive detection passes,
writes JSONB results to an output table, and logs all API calls to a log table.

Checkpointed: each pass writes immediately so crashes don't lose work.

Usage:
    python3 run_pipeline.py
    python3 run_pipeline.py --source-table my_input --dest-table my_output --batch-size 100
    python3 run_pipeline.py --include-linkup --verbose
"""

import argparse
import logging
import os
import sys
import time
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from dotenv import load_dotenv

load_dotenv(override=True)

# Local imports
from lib.supabase_client import SupabaseBookingClient
from lib.firecrawl_client import FirecrawlAuditClient
from lib.llm_client import LLMAuditClient, load_prompt
from lib.linkup_client import LinkupAuditClient, fill_prompt
from lib.text_scanner import load_patterns, scan_text

logger = logging.getLogger("pipeline")

SCRIPT_DIR = Path(__file__).parent
CONFIGS_DIR = SCRIPT_DIR / "configs"
PROMPTS_DIR = CONFIGS_DIR / "prompts"

BOOKING_INCLUDE_PATHS = [
    "/book*", "/reserv*", "/ticket*", "/tour*", "/activit*",
    "/pricing*", "/schedule*", "/shop*", "/order*", "/checkout*",
]
BOOKING_EXCLUDE_PATHS = [
    "/blog*", "/news*", "/press*", "/about*", "/contact*",
    "/faq*", "/team*", "/career*",
]

MAX_CRAWL_MARKDOWN_CHARS = 15000


# ---------------------------------------------------------------------------
# Pass implementations
# ---------------------------------------------------------------------------

def pass1_scrape(domains, firecrawl, sb, dest_table, log_table):
    """Pass 1: Scrape homepage of each domain."""
    unresolved = sb.get_unresolved_domains(dest_table, domains)
    if not unresolved:
        return {}
    logger.info("Pass 1/9: Scraping %d homepages...", len(unresolved))
    sb.log_event(log_table, None, "pass1_scrape", "pass_start",
                 metadata={"domain_count": len(unresolved)})

    homepage_cache = {}  # domain -> {html, markdown}

    def scrape_one(domain):
        url = f"https://{domain}"
        result = firecrawl.scrape_url(url, domain, "pass1_scrape")
        return domain, result

    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = {}
        for domain in unresolved:
            futures[executor.submit(scrape_one, domain)] = domain
            time.sleep(0.3)

        for future in as_completed(futures):
            domain, result = future.result()
            if result["status"] == "success" and (result["html"] or result["markdown"]):
                homepage_cache[domain] = {
                    "html": result["html"],
                    "markdown": result["markdown"],
                }
            else:
                sb.upsert_result(dest_table, domain, {
                    "has_booking": False,
                    "booking_platform": None,
                    "reasoning": f"Failed to scrape: {result.get('error', 'unknown')}",
                    "source_pass": "scrape_failed",
                }, last_pass="scrape_failed", completed=True)

    logger.info("Pass 1 complete: %d/%d scraped", len(homepage_cache), len(unresolved))
    sb.log_event(log_table, None, "pass1_scrape", "pass_end",
                 metadata={"scraped": len(homepage_cache), "failed": len(unresolved) - len(homepage_cache)})
    return homepage_cache


def pass2_llm_html(domains, homepage_cache, llm, sb, dest_table, log_table):
    """Pass 2: LLM classification on homepage HTML."""
    unresolved = sb.get_unresolved_domains(dest_table, domains)
    candidates = [d for d in unresolved if d in homepage_cache]
    if not candidates:
        return
    prompt = load_prompt(str(PROMPTS_DIR / "booking_detection_html.txt"))
    logger.info("Pass 2/9: LLM on %d homepage HTMLs...", len(candidates))
    hits = 0

    def analyze_one(domain):
        html = homepage_cache[domain]["html"][:50000]
        result = llm.analyze(html, prompt, domain, "pass2_llm_html",
                             placeholders={"homepage_html": html})
        return domain, result

    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = {}
        for domain in candidates:
            futures[executor.submit(analyze_one, domain)] = domain
            time.sleep(0.2)

        for future in as_completed(futures):
            domain, result = future.result()
            if result["status"] == "success" and result["parsed"]:
                parsed = result["parsed"]
                has_booking = parsed.get("has_booking", False)
                if has_booking:
                    hits += 1
                    sb.upsert_result(dest_table, domain, {
                        "has_booking": True,
                        "booking_platform": parsed.get("booking_platform"),
                        "reasoning": parsed.get("reasoning"),
                        "source_pass": "llm_html",
                    }, last_pass="llm_html", completed=True)

    logger.info("Pass 2 complete: %d/%d have booking", hits, len(candidates))


def pass3_regex_homepage(domains, homepage_cache, patterns, sb, dest_table, log_table):
    """Pass 3: Regex scan on homepage HTML/markdown."""
    unresolved = sb.get_unresolved_domains(dest_table, domains)
    candidates = [d for d in unresolved if d in homepage_cache]
    if not candidates:
        return
    logger.info("Pass 3/9: Regex on %d homepages...", len(candidates))
    hits = 0

    for domain in candidates:
        text = (homepage_cache[domain].get("html", "") + "\n" +
                homepage_cache[domain].get("markdown", ""))
        scan_hits = scan_text(text, patterns)
        if scan_hits:
            platform_hits = [h for h in scan_hits
                             if h["category"] in ("booking_platform", "ecommerce", "payment_signal")]
            if platform_hits:
                hits += 1
                platform = platform_hits[0]["label"]
                labels = ", ".join(h["label"] for h in scan_hits)
                sb.upsert_result(dest_table, domain, {
                    "has_booking": True,
                    "booking_platform": platform,
                    "reasoning": f"Regex detected: {labels}",
                    "source_pass": "regex_homepage",
                }, last_pass="regex_homepage", completed=True)

    logger.info("Pass 3 complete: %d hits", hits)


def pass4_crawl_booking_pages(domains, firecrawl, sb, dest_table, log_table):
    """Pass 4: Path-filtered subpage crawl."""
    unresolved = sb.get_unresolved_domains(dest_table, domains)
    if not unresolved:
        return {}
    logger.info("Pass 4/9: Crawling booking pages for %d domains...", len(unresolved))
    crawled_pages = {}

    def crawl_one(domain):
        url = f"https://{domain}"
        pages = firecrawl.crawl_url(url, domain, "pass4_crawl",
                                    include_paths=BOOKING_INCLUDE_PATHS,
                                    exclude_paths=BOOKING_EXCLUDE_PATHS,
                                    limit=20)
        return domain, pages

    with ThreadPoolExecutor(max_workers=3) as executor:
        futures = {}
        for domain in unresolved:
            futures[executor.submit(crawl_one, domain)] = domain
            time.sleep(1.0)

        for future in as_completed(futures):
            domain, pages = future.result()
            if pages:
                crawled_pages[domain] = pages

    logger.info("Pass 4 complete: %d/%d had crawlable pages", len(crawled_pages), len(unresolved))
    return crawled_pages


def pass5_regex_subpages(domains, crawled_pages, patterns, sb, dest_table, log_table):
    """Pass 5: Regex scan on crawled subpages."""
    unresolved = sb.get_unresolved_domains(dest_table, domains)
    candidates = [d for d in unresolved if d in crawled_pages]
    if not candidates:
        return
    logger.info("Pass 5/9: Regex on %d domains' subpages...", len(candidates))
    hits = 0

    for domain in candidates:
        combined = "\n".join(p["markdown"] for p in crawled_pages[domain])
        scan_hits = scan_text(combined, patterns)
        if scan_hits:
            platform_hits = [h for h in scan_hits
                             if h["category"] in ("booking_platform", "ecommerce", "payment_signal")]
            if platform_hits:
                hits += 1
                platform = platform_hits[0]["label"]
                labels = ", ".join(h["label"] for h in scan_hits)
                sb.upsert_result(dest_table, domain, {
                    "has_booking": True,
                    "booking_platform": platform,
                    "reasoning": f"Regex on subpages: {labels}",
                    "source_pass": "regex_subpages",
                }, last_pass="regex_subpages", completed=True)

    logger.info("Pass 5 complete: %d hits", hits)


def pass6_straight_crawl(domains, crawled_pages, firecrawl, sb, dest_table, log_table):
    """Pass 6: Broad crawl (no path filter) for remaining domains."""
    unresolved = sb.get_unresolved_domains(dest_table, domains)
    if not unresolved:
        return crawled_pages
    logger.info("Pass 6/9: Straight crawl for %d domains...", len(unresolved))

    def crawl_one(domain):
        url = f"https://{domain}"
        pages = firecrawl.crawl_url(url, domain, "pass6_straight_crawl", limit=25)
        return domain, pages

    with ThreadPoolExecutor(max_workers=3) as executor:
        futures = {}
        for domain in unresolved:
            futures[executor.submit(crawl_one, domain)] = domain
            time.sleep(1.0)

        for future in as_completed(futures):
            domain, pages = future.result()
            if pages:
                existing = crawled_pages.get(domain, [])
                existing_urls = {p["url"] for p in existing}
                new_pages = [p for p in pages if p["url"] not in existing_urls]
                crawled_pages[domain] = existing + new_pages

    logger.info("Pass 6 complete: %d/%d domains crawled", len(unresolved), len(unresolved))
    return crawled_pages


def pass7_regex_straight_crawl(domains, crawled_pages, patterns, sb, dest_table, log_table):
    """Pass 7: Regex on straight-crawl results."""
    unresolved = sb.get_unresolved_domains(dest_table, domains)
    candidates = [d for d in unresolved if d in crawled_pages]
    if not candidates:
        return
    logger.info("Pass 7/9: Regex on %d domains' straight crawl...", len(candidates))
    hits = 0

    for domain in candidates:
        combined = "\n".join(p["markdown"] for p in crawled_pages[domain])
        scan_hits = scan_text(combined, patterns)
        if scan_hits:
            platform_hits = [h for h in scan_hits
                             if h["category"] in ("booking_platform", "ecommerce", "payment_signal")]
            if platform_hits:
                hits += 1
                platform = platform_hits[0]["label"]
                labels = ", ".join(h["label"] for h in scan_hits)
                sb.upsert_result(dest_table, domain, {
                    "has_booking": True,
                    "booking_platform": platform,
                    "reasoning": f"Regex on crawl: {labels}",
                    "source_pass": "regex_straight_crawl",
                }, last_pass="regex_straight_crawl", completed=True)

    logger.info("Pass 7 complete: %d hits", hits)


def pass8_llm_crawled(domains, crawled_pages, llm, sb, dest_table, log_table):
    """Pass 8: LLM on concatenated crawled markdown."""
    unresolved = sb.get_unresolved_domains(dest_table, domains)
    candidates = [d for d in unresolved if d in crawled_pages]
    if not candidates:
        return
    prompt = load_prompt(str(PROMPTS_DIR / "booking_detection_crawled.txt"))
    logger.info("Pass 8/9: LLM on %d domains' crawled content...", len(candidates))
    hits = 0

    def analyze_one(domain):
        pages = crawled_pages[domain]
        combined = "\n\n---\n\n".join(
            f"Page: {p['url']}\n{p['markdown']}" for p in pages
        )[:MAX_CRAWL_MARKDOWN_CHARS]
        result = llm.analyze(combined, prompt, domain, "pass8_llm_crawled",
                             placeholders={"domain": domain, "page_content": combined})
        return domain, result

    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = {}
        for domain in candidates:
            futures[executor.submit(analyze_one, domain)] = domain
            time.sleep(0.2)

        for future in as_completed(futures):
            domain, result = future.result()
            if result["status"] == "success" and result["parsed"]:
                parsed = result["parsed"]
                has_booking = parsed.get("has_booking", False)
                if has_booking:
                    hits += 1
                    sb.upsert_result(dest_table, domain, {
                        "has_booking": True,
                        "booking_platform": parsed.get("booking_platform"),
                        "reasoning": parsed.get("reasoning"),
                        "source_pass": "llm_crawled",
                    }, last_pass="llm_crawled", completed=True)

    logger.info("Pass 8 complete: %d/%d have booking", hits, len(candidates))


def pass9_linkup(domains, linkup, sb, dest_table, log_table):
    """Pass 9 (optional): Linkup deep validation."""
    unresolved = sb.get_unresolved_domains(dest_table, domains)
    if not unresolved:
        return
    prompt_template = load_prompt(str(PROMPTS_DIR / "booking_detection_own_site.md"))
    logger.info("Pass 9/9: Linkup deep search for %d domains ($0.05 each)...", len(unresolved))
    hits = 0

    for i, domain in enumerate(unresolved, 1):
        prompt_text = fill_prompt(prompt_template, domain)
        result = linkup.search_booking(domain, prompt_text, "pass9_linkup")

        if result["status"] == "success" and result["has_booking"]:
            hits += 1
            sb.upsert_result(dest_table, domain, {
                "has_booking": True,
                "booking_platform": result["booking_platform"],
                "reasoning": result["reasoning"],
                "source_pass": "linkup_deep",
            }, last_pass="linkup_deep", completed=True)

        if i % 10 == 0:
            logger.info("Pass 9 progress: %d/%d (%.0f%% done, %d hits)",
                        i, len(unresolved), 100 * i / len(unresolved), hits)
        time.sleep(0.5)

    logger.info("Pass 9 complete: %d/%d recovered", hits, len(unresolved))


# ---------------------------------------------------------------------------
# Main orchestrator
# ---------------------------------------------------------------------------

def run_pipeline(args):
    """Run the full 9-pass pipeline."""
    level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )

    # Validate env
    supabase_url = os.getenv("SUPABASE_URL")
    supabase_key = os.getenv("SUPABASE_KEY")
    database_url = os.getenv("DATABASE_URL")
    firecrawl_key = os.getenv("FIRECRAWL_API_KEY")
    openai_key = os.getenv("OPENAI_API_KEY")
    linkup_key = os.getenv("LINKUP_API_KEY")

    if not supabase_url or not supabase_key:
        logger.error("SUPABASE_URL and SUPABASE_KEY must be set in .env")
        sys.exit(1)
    if not firecrawl_key:
        logger.error("FIRECRAWL_API_KEY not set")
        sys.exit(1)
    if not openai_key:
        logger.error("OPENAI_API_KEY not set")
        sys.exit(1)
    if args.include_linkup and not linkup_key:
        logger.error("LINKUP_API_KEY not set but --include-linkup specified")
        sys.exit(1)

    # Init clients
    sb = SupabaseBookingClient(supabase_url, supabase_key, database_url)
    firecrawl = FirecrawlAuditClient(firecrawl_key, sb, args.log_table)
    llm = LLMAuditClient(openai_key, sb, args.log_table)
    linkup = LinkupAuditClient(linkup_key, sb, args.log_table) if args.include_linkup else None
    patterns = load_patterns(str(CONFIGS_DIR / "booking_fingerprints.yaml"))

    start_time = time.time()
    total_processed = 0
    total_booking = 0

    logger.info("=" * 60)
    logger.info("Booking Detection Pipeline (Supabase)")
    logger.info("Source: %s | Dest: %s | Log: %s",
                args.source_table, args.dest_table, args.log_table)
    logger.info("Batch size: %d | Linkup: %s",
                args.batch_size, "enabled" if args.include_linkup else "disabled")
    logger.info("=" * 60)

    # Batch loop
    batch_num = 0
    while True:
        domains = sb.fetch_pending_domains(args.source_table, args.batch_size)
        if not domains:
            if batch_num == 0:
                logger.info("No pending domains found in %s", args.source_table)
            break

        batch_num += 1
        logger.info("Batch %d: processing %d domains", batch_num, len(domains))

        # Initialize output rows
        for domain in domains:
            sb.upsert_result(args.dest_table, domain, {
                "has_booking": None, "booking_platform": None,
                "reasoning": None, "source_pass": None,
            }, last_pass="initialized")

        # Run 9 passes
        homepage_cache = pass1_scrape(domains, firecrawl, sb, args.dest_table, args.log_table)

        pass2_llm_html(domains, homepage_cache, llm, sb, args.dest_table, args.log_table)
        _log_progress(sb, args.dest_table, domains, 2)

        pass3_regex_homepage(domains, homepage_cache, patterns, sb, args.dest_table, args.log_table)
        _log_progress(sb, args.dest_table, domains, 3)

        crawled_pages = pass4_crawl_booking_pages(domains, firecrawl, sb, args.dest_table, args.log_table)

        pass5_regex_subpages(domains, crawled_pages, patterns, sb, args.dest_table, args.log_table)
        _log_progress(sb, args.dest_table, domains, 5)

        crawled_pages = pass6_straight_crawl(domains, crawled_pages, firecrawl, sb, args.dest_table, args.log_table)

        pass7_regex_straight_crawl(domains, crawled_pages, patterns, sb, args.dest_table, args.log_table)
        _log_progress(sb, args.dest_table, domains, 7)

        pass8_llm_crawled(domains, crawled_pages, llm, sb, args.dest_table, args.log_table)
        _log_progress(sb, args.dest_table, domains, 8)

        if args.include_linkup and linkup:
            pass9_linkup(domains, linkup, sb, args.dest_table, args.log_table)
            _log_progress(sb, args.dest_table, domains, 9)

        # Mark remaining unresolved as no_booking
        unresolved = sb.get_unresolved_domains(args.dest_table, domains)
        for domain in unresolved:
            sb.upsert_result(args.dest_table, domain, {
                "has_booking": False,
                "booking_platform": None,
                "reasoning": "No booking detected after all passes",
                "source_pass": "no_booking",
            }, last_pass="no_booking", completed=True)

        # Mark all as done in input table
        for domain in domains:
            sb.mark_domain_done(args.source_table, domain)

        batch_booking = len(domains) - len(unresolved)
        total_processed += len(domains)
        total_booking += batch_booking
        logger.info("Batch %d complete: %d/%d have booking",
                    batch_num, batch_booking, len(domains))

    # Summary
    elapsed = time.time() - start_time
    if total_processed > 0:
        no_booking = total_processed - total_booking
        logger.info("=" * 60)
        logger.info("Pipeline complete in %.0f seconds", elapsed)
        logger.info("Total: %d | Booking: %d (%.1f%%) | No booking: %d (%.1f%%)",
                    total_processed, total_booking,
                    100 * total_booking / total_processed,
                    no_booking, 100 * no_booking / total_processed)
        logger.info("=" * 60)


def _log_progress(sb, dest_table, domains, pass_num):
    """Log progress after each pass."""
    unresolved = sb.get_unresolved_domains(dest_table, domains)
    resolved = len(domains) - len(unresolved)
    logger.info("After pass %d: %d resolved, %d unresolved",
                pass_num, resolved, len(unresolved))


def main():
    parser = argparse.ArgumentParser(
        description="Run the 9-pass booking detection pipeline (Supabase-backed)."
    )
    parser.add_argument("--source-table", default="booking_detection_input",
                        help="Supabase table with domains to process")
    parser.add_argument("--dest-table", default="booking_detection_output",
                        help="Supabase table for results")
    parser.add_argument("--log-table", default="booking_detection_log",
                        help="Supabase table for audit logs")
    parser.add_argument("--batch-size", type=int, default=500,
                        help="Domains per batch (default 500)")
    parser.add_argument("--include-linkup", action="store_true",
                        help="Enable pass 9 (Linkup deep, $0.05/domain)")
    parser.add_argument("--verbose", action="store_true",
                        help="Enable debug logging")
    args = parser.parse_args()
    run_pipeline(args)


if __name__ == "__main__":
    main()
