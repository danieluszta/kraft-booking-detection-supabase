"""Unit tests for lib.linkup_local — response parsing and prompt filling."""

import pytest
from lib.linkup_local import fill_prompt, _parse_response, _extract_structured, _get_field


# -------------------------------------------------------------------
# fill_prompt
# -------------------------------------------------------------------

class TestFillPrompt:
    def test_single_replacement(self):
        assert fill_prompt("Check {domain}", "example.com") == "Check example.com"

    def test_multiple_replacements(self):
        template = "Search {domain} and verify {domain} has booking"
        assert fill_prompt(template, "test.com") == "Search test.com and verify test.com has booking"

    def test_no_placeholder(self):
        assert fill_prompt("No placeholders here", "test.com") == "No placeholders here"


# -------------------------------------------------------------------
# _parse_response — structured output extraction
# -------------------------------------------------------------------

class TestParseResponse:
    def test_top_level_fields(self):
        data = {"has_booking": True, "booking_platform": "Bokun", "reasoning": "Found widget"}
        result = _parse_response(data)
        assert result["status"] == "success"
        assert result["has_booking"] is True
        assert result["booking_platform"] == "Bokun"
        assert result["reasoning"] == "Found widget"

    def test_nested_under_answer_dict(self):
        data = {"answer": {"has_booking": False, "booking_platform": None, "reasoning": "No signals"}}
        result = _parse_response(data)
        assert result["has_booking"] is False

    def test_nested_under_answer_json_string(self):
        import json
        inner = {"has_booking": True, "booking_platform": "Rezdy", "reasoning": "Found"}
        data = {"answer": json.dumps(inner)}
        result = _parse_response(data)
        assert result["has_booking"] is True
        assert result["booking_platform"] == "Rezdy"

    def test_nested_under_results_array(self):
        data = {"results": [{"has_booking": True, "booking_platform": "Xola", "reasoning": "Widget"}]}
        result = _parse_response(data)
        assert result["has_booking"] is True

    def test_nested_results_content_json_string(self):
        import json
        inner = {"has_booking": False, "booking_platform": None, "reasoning": "Nothing"}
        data = {"results": [{"content": json.dumps(inner)}]}
        result = _parse_response(data)
        assert result["has_booking"] is False

    def test_string_true_coerced_to_bool(self):
        data = {"has_booking": "true", "booking_platform": "Test", "reasoning": "x"}
        result = _parse_response(data)
        assert result["has_booking"] is True

    def test_string_false_coerced_to_bool(self):
        data = {"has_booking": "false", "booking_platform": None, "reasoning": "x"}
        result = _parse_response(data)
        assert result["has_booking"] is False

    def test_camelCase_keys(self):
        data = {"hasBooking": True, "bookingPlatform": "FareHarbor", "reasoning": "Found"}
        result = _parse_response(data)
        assert result["has_booking"] is True
        assert result["booking_platform"] == "FareHarbor"

    def test_empty_response_returns_success_with_nones(self):
        result = _parse_response({})
        assert result["status"] == "success"
        assert result["has_booking"] is None

    def test_malformed_answer_string(self):
        data = {"answer": "I couldn't determine the booking status"}
        result = _parse_response(data)
        assert result["status"] == "success"  # graceful fallback


# -------------------------------------------------------------------
# _get_field — case-insensitive field lookup
# -------------------------------------------------------------------

class TestGetField:
    def test_exact_match(self):
        assert _get_field({"has_booking": True}, ["has_booking"]) is True

    def test_fallback_key(self):
        assert _get_field({"hasBooking": True}, ["has_booking", "hasBooking"]) is True

    def test_case_insensitive(self):
        assert _get_field({"HAS_BOOKING": True}, ["has_booking"]) is True

    def test_missing_returns_none(self):
        assert _get_field({"other": "val"}, ["has_booking"]) is None
