"""OpenAI LLM client with Supabase audit logging.

Wraps the standalone llm_analysis_local module and adds
pre/post API call logging to the Supabase log table.
"""

import sys
import time
import logging
from pathlib import Path

_PARENT_LIB = str(Path(__file__).resolve().parent.parent.parent / "lib")
if _PARENT_LIB not in sys.path:
    sys.path.insert(0, _PARENT_LIB)

import llm_analysis_local

logger = logging.getLogger(__name__)

# Re-export for convenience
load_prompt = llm_analysis_local.load_prompt


class LLMAuditClient:
    """OpenAI client with pre/post audit logging to Supabase."""

    def __init__(self, api_key: str, supabase_client, log_table: str):
        self.api_key = api_key
        self.sb = supabase_client
        self.log_table = log_table

    def analyze(self, text: str, prompt_template: str, domain: str,
                pass_name: str, placeholders: dict = None) -> dict:
        """Run LLM analysis with audit logging."""
        preview = (text or "")[:500]
        log_id = self.sb.log_api_start(
            self.log_table, domain, pass_name, "openai", preview
        )
        start = time.monotonic()
        try:
            result = llm_analysis_local.analyze(
                text, prompt_template, self.api_key, placeholders=placeholders
            )
            elapsed_ms = int((time.monotonic() - start) * 1000)
            self.sb.log_api_end(
                self.log_table, log_id,
                http_status=200 if result["status"] == "success" else None,
                response_time_ms=elapsed_ms,
                success=result["status"] == "success",
                response_preview=str(result.get("raw_response", ""))[:500],
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
