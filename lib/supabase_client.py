"""Supabase data-access layer for the booking detection pipeline.

Handles all reads/writes to the input, output, and log tables.
Uses supabase-py for CRUD and psycopg2 for batch operations.
"""

import json
import logging
import time
from datetime import datetime, timezone
from typing import Optional

import httpx
from supabase import create_client, Client
from supabase.lib.client_options import SyncClientOptions

logger = logging.getLogger(__name__)

_MAX_RETRIES = 3
_RETRY_BASE_SECONDS = 1


def _with_retry(func, description: str = "operation"):
    """Execute a Supabase operation with retry on transient HTTP/connection errors."""
    for attempt in range(1, _MAX_RETRIES + 1):
        try:
            return func()
        except (httpx.RemoteProtocolError, httpx.ReadError, httpx.WriteError,
                httpx.ConnectError, httpx.PoolTimeout) as e:
            if attempt < _MAX_RETRIES:
                wait = _RETRY_BASE_SECONDS * (2 ** (attempt - 1))
                logger.warning(
                    "Supabase %s failed (attempt %d/%d): %s — retrying in %ds",
                    description, attempt, _MAX_RETRIES, e, wait,
                )
                time.sleep(wait)
            else:
                logger.error(
                    "Supabase %s failed after %d attempts: %s",
                    description, _MAX_RETRIES, e,
                )
                raise


class SupabaseBookingClient:
    """Central data-access client for the booking detection pipeline."""

    def __init__(self, supabase_url: str, supabase_key: str, database_url: str = None):
        # Force HTTP/1.1 to avoid HTTP/2 framing errors under concurrent load
        http1_client = httpx.Client(http2=False, timeout=httpx.Timeout(120))
        self.client: Client = create_client(
            supabase_url,
            supabase_key,
            options=SyncClientOptions(httpx_client=http1_client),
        )
        self.database_url = database_url

    # -------------------------------------------------------------------
    # Input table operations
    # -------------------------------------------------------------------

    def fetch_pending_domains(self, table: str, batch_size: int) -> list[str]:
        """Fetch pending domains and atomically mark them as processing."""
        try:
            resp = _with_retry(
                lambda: (
                    self.client.table(table)
                    .select("domain")
                    .eq("status", "pending")
                    .order("id")
                    .limit(batch_size)
                    .execute()
                ),
                description="fetch_pending_domains",
            )
            domains = [row["domain"] for row in resp.data]
            if domains:
                _with_retry(
                    lambda: (
                        self.client.table(table)
                        .update({
                            "status": "processing",
                            "updated_at": _now_iso(),
                        })
                        .in_("domain", domains)
                        .execute()
                    ),
                    description="mark_domains_processing",
                )
            return domains
        except Exception as e:
            logger.error("Failed to fetch pending domains: %s", e)
            return []

    def mark_domain_done(self, table: str, domain: str):
        """Mark a domain as done in the input table."""
        try:
            _with_retry(
                lambda: (
                    self.client.table(table)
                    .update({"status": "done", "updated_at": _now_iso()})
                    .eq("domain", domain)
                    .execute()
                ),
                description=f"mark_domain_done({domain})",
            )
        except Exception as e:
            logger.warning("Failed to mark domain done: %s %s", domain, e)

    def mark_domain_error(self, table: str, domain: str, error_msg: str):
        """Mark a domain as errored in the input table."""
        try:
            _with_retry(
                lambda: (
                    self.client.table(table)
                    .update({
                        "status": "error",
                        "error_message": error_msg[:500],
                        "updated_at": _now_iso(),
                    })
                    .eq("domain", domain)
                    .execute()
                ),
                description=f"mark_domain_error({domain})",
            )
        except Exception as e:
            logger.warning("Failed to mark domain error: %s %s", domain, e)

    # -------------------------------------------------------------------
    # Output table operations
    # -------------------------------------------------------------------

    def get_result(self, table: str, domain: str) -> dict | None:
        """Fetch existing result row for a domain."""
        try:
            resp = (
                self.client.table(table)
                .select("*")
                .eq("domain", domain)
                .limit(1)
                .execute()
            )
            return resp.data[0] if resp.data else None
        except Exception as e:
            logger.warning("Failed to get result for %s: %s", domain, e)
            return None

    def upsert_result(self, table: str, domain: str, result: dict,
                      last_pass: str, completed: bool = False):
        """Insert or update a result row for a domain."""
        try:
            _with_retry(
                lambda: (
                    self.client.table(table)
                    .upsert({
                        "domain": domain,
                        "result": result,
                        "last_pass": last_pass,
                        "completed": completed,
                        "updated_at": _now_iso(),
                    }, on_conflict="domain")
                    .execute()
                ),
                description=f"upsert_result({domain})",
            )
        except Exception as e:
            logger.error("Failed to upsert result for %s: %s", domain, e)

    def get_unresolved_domains(self, table: str, domains: list[str]) -> list[str]:
        """Return domains from list where completed=False."""
        try:
            resp = (
                self.client.table(table)
                .select("domain")
                .in_("domain", domains)
                .eq("completed", False)
                .execute()
            )
            return [row["domain"] for row in resp.data]
        except Exception as e:
            logger.warning("Failed to get unresolved domains: %s", e)
            return domains  # Safe fallback: treat all as unresolved

    # -------------------------------------------------------------------
    # Log table operations (audit trail)
    # -------------------------------------------------------------------

    def log_event(self, table: str, domain: str | None, pass_name: str,
                  event_type: str, **kwargs) -> int | None:
        """Insert a log event. Returns the row id or None on failure."""
        row = {
            "domain": domain,
            "pass_name": pass_name,
            "event_type": event_type,
            "created_at": _now_iso(),
        }
        for key in ("api_service", "request_preview", "response_preview",
                     "http_status", "response_time_ms", "success",
                     "error_code", "error_message"):
            if key in kwargs:
                row[key] = kwargs[key]
        if "metadata" in kwargs and kwargs["metadata"]:
            row["metadata"] = kwargs["metadata"]
        return self._insert_log(table, row)

    def log_api_start(self, table: str, domain: str, pass_name: str,
                      api_service: str, request_preview: str) -> int | None:
        """Pre-insert log row BEFORE an API call. Returns id for later update."""
        row = {
            "domain": domain,
            "pass_name": pass_name,
            "event_type": "api_call_start",
            "api_service": api_service,
            "request_preview": (request_preview or "")[:500],
            "success": None,
            "created_at": _now_iso(),
        }
        return self._insert_log(table, row)

    def log_api_end(self, table: str, log_id: int | None,
                    http_status: int | None, response_time_ms: int,
                    success: bool, error_code: str = None,
                    error_message: str = None, response_preview: str = None):
        """Update a log row after API call completes."""
        if log_id is None:
            return
        try:
            update = {
                "event_type": "api_call_end",
                "http_status": http_status,
                "response_time_ms": response_time_ms,
                "success": success,
            }
            if error_code:
                update["error_code"] = error_code
            if error_message:
                update["error_message"] = (error_message or "")[:500]
            if response_preview:
                update["response_preview"] = (response_preview or "")[:500]
            _with_retry(
                lambda: (
                    self.client.table(table)
                    .update(update)
                    .eq("id", log_id)
                    .execute()
                ),
                description=f"log_api_end({log_id})",
            )
        except Exception as e:
            logger.warning("Failed to update log %s: %s", log_id, e)

    def _insert_log(self, table: str, row: dict) -> int | None:
        """Insert a log row and return its id. Silent failure."""
        try:
            resp = _with_retry(
                lambda: self.client.table(table).insert(row).execute(),
                description="insert_log",
            )
            if resp.data:
                return resp.data[0].get("id")
            return None
        except Exception as e:
            logger.warning("Failed to insert log: %s", e)
            return None

    # -------------------------------------------------------------------
    # Direct SQL (for batch operations or complex queries)
    # -------------------------------------------------------------------

    def execute_sql(self, sql: str, params: tuple = None) -> list[dict]:
        """Run a SELECT query via psycopg2. Returns list of dicts."""
        if not self.database_url:
            logger.error("DATABASE_URL not set; cannot run direct SQL")
            return []
        import psycopg2
        import psycopg2.extras
        try:
            with psycopg2.connect(self.database_url) as conn:
                with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                    cur.execute(sql, params)
                    return [dict(row) for row in cur.fetchall()]
        except Exception as e:
            logger.error("SQL query failed: %s", e)
            return []

    def execute_write_sql(self, sql: str, params: tuple = None) -> int:
        """Run an INSERT/UPDATE/DELETE via psycopg2. Returns row count."""
        if not self.database_url:
            logger.error("DATABASE_URL not set; cannot run direct SQL")
            return 0
        import psycopg2
        try:
            with psycopg2.connect(self.database_url) as conn:
                with conn.cursor() as cur:
                    cur.execute(sql, params)
                    conn.commit()
                    return cur.rowcount
        except Exception as e:
            logger.error("SQL write failed: %s", e)
            return 0


def _now_iso() -> str:
    """Return current UTC time as ISO string."""
    return datetime.now(timezone.utc).isoformat()
