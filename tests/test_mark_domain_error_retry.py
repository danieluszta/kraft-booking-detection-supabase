"""Tests for mark_domain_error retry behavior (previously missing)."""

from unittest.mock import MagicMock, patch, call
import httpx
import pytest


class TestMarkDomainErrorRetry:
    """Verify mark_domain_error uses _with_retry instead of bare calls."""

    def _make_client(self):
        """Create a SupabaseBookingClient with a mocked Supabase client."""
        with patch("lib.supabase_client.create_client") as mock_create:
            mock_sb = MagicMock()
            mock_create.return_value = mock_sb
            with patch("lib.supabase_client.httpx.Client"):
                from lib.supabase_client import SupabaseBookingClient
                client = SupabaseBookingClient("https://fake.supabase.co", "fake-key")
        return client, mock_sb

    def test_mark_domain_error_retries_on_transient_failure(self):
        """mark_domain_error should retry on httpx transport errors."""
        client, mock_sb = self._make_client()

        # Make .table().update().eq().execute() fail once then succeed
        mock_table = MagicMock()
        mock_sb.table.return_value = mock_table
        mock_update = MagicMock()
        mock_table.update.return_value = mock_update
        mock_eq = MagicMock()
        mock_update.eq.return_value = mock_eq
        mock_eq.execute.side_effect = [
            httpx.RemoteProtocolError("connection terminated"),
            MagicMock(data=[]),
        ]

        with patch("lib.supabase_client.time.sleep"):
            client.mark_domain_error("input_table", "example.com", "some error")

        # Should have been called twice (1 fail + 1 retry)
        assert mock_eq.execute.call_count == 2

    def test_mark_domain_error_does_not_raise_after_exhausting_retries(self):
        """mark_domain_error should log warning, not crash, after all retries fail."""
        client, mock_sb = self._make_client()

        mock_table = MagicMock()
        mock_sb.table.return_value = mock_table
        mock_update = MagicMock()
        mock_table.update.return_value = mock_update
        mock_eq = MagicMock()
        mock_update.eq.return_value = mock_eq
        mock_eq.execute.side_effect = httpx.RemoteProtocolError("dead")

        with patch("lib.supabase_client.time.sleep"):
            # Should not raise — the outer try/except catches it
            client.mark_domain_error("input_table", "example.com", "some error")

        # 3 attempts (max retries)
        assert mock_eq.execute.call_count == 3
