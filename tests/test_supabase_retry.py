"""Unit tests for lib.supabase_client — retry logic."""

import pytest
from unittest.mock import MagicMock, patch
import httpx
from lib.supabase_client import _with_retry


class TestWithRetry:
    def test_succeeds_on_first_try(self):
        func = MagicMock(return_value="ok")
        result = _with_retry(func, "test_op")
        assert result == "ok"
        assert func.call_count == 1

    def test_retries_on_remote_protocol_error(self):
        func = MagicMock(side_effect=[
            httpx.RemoteProtocolError("connection terminated"),
            "ok",
        ])
        with patch("lib.supabase_client.time.sleep"):
            result = _with_retry(func, "test_op")
        assert result == "ok"
        assert func.call_count == 2

    def test_retries_on_read_error(self):
        func = MagicMock(side_effect=[
            httpx.ReadError("read failed"),
            "ok",
        ])
        with patch("lib.supabase_client.time.sleep"):
            result = _with_retry(func, "test_op")
        assert result == "ok"
        assert func.call_count == 2

    def test_retries_on_connect_error(self):
        func = MagicMock(side_effect=[
            httpx.ConnectError("connect failed"),
            httpx.ConnectError("connect failed again"),
            "ok",
        ])
        with patch("lib.supabase_client.time.sleep"):
            result = _with_retry(func, "test_op")
        assert result == "ok"
        assert func.call_count == 3

    def test_raises_after_max_retries(self):
        func = MagicMock(side_effect=httpx.RemoteProtocolError("dead"))
        with patch("lib.supabase_client.time.sleep"):
            with pytest.raises(httpx.RemoteProtocolError):
                _with_retry(func, "test_op")
        assert func.call_count == 3

    def test_non_retryable_errors_propagate_immediately(self):
        func = MagicMock(side_effect=ValueError("bad data"))
        with pytest.raises(ValueError):
            _with_retry(func, "test_op")
        assert func.call_count == 1

    def test_pool_timeout_retries(self):
        func = MagicMock(side_effect=[
            httpx.PoolTimeout("pool full"),
            "ok",
        ])
        with patch("lib.supabase_client.time.sleep"):
            result = _with_retry(func, "test_op")
        assert result == "ok"
