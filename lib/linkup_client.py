"""Linkup API client with Supabase audit logging.

Wraps the standalone linkup_local module and adds
pre/post API call logging to the Supabase log table.
"""

import time
import logging

from lib import linkup_local

logger = logging.getLogger(__name__)

# Re-export for convenience
fill_prompt = linkup_local.fill_prompt


class LinkupAuditClient:
    """Linkup client with pre/post audit logging to Supabase."""

    def __init__(self, api_key: str, supabase_client, log_table: str):
        self.api_key = api_key
        self.sb = supabase_client
        self.log_table = log_table

    def search_booking(self, domain: str, prompt_text: str,
                       pass_name: str) -> dict:
        """Search for booking with audit logging."""
        log_id = self.sb.log_api_start(
            self.log_table, domain, pass_name, "linkup",
            f"domain={domain} depth=deep"
        )
        start = time.monotonic()
        try:
            result = linkup_local.search_booking(
                domain, prompt_text, self.api_key
            )
            elapsed_ms = int((time.monotonic() - start) * 1000)
            success = result["status"] == "success"
            self.sb.log_api_end(
                self.log_table, log_id,
                http_status=200 if success else None,
                response_time_ms=elapsed_ms,
                success=success,
                response_preview=str(result.get("reasoning", ""))[:500],
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
