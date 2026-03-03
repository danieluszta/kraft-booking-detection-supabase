"""Unit tests for lib.llm_analysis_local — JSON parsing and prompt loading."""

import pytest
from pathlib import Path
from lib.llm_analysis_local import parse_json_response, load_prompt

PROMPTS_DIR = Path(__file__).parent.parent / "configs" / "prompts"


# -------------------------------------------------------------------
# JSON response parsing
# -------------------------------------------------------------------

class TestParseJsonResponse:
    def test_clean_json(self):
        result = parse_json_response('{"has_booking": true, "booking_platform": "Bokun"}')
        assert result["has_booking"] is True
        assert result["booking_platform"] == "Bokun"

    def test_markdown_code_block(self):
        text = 'Here is the result:\n```json\n{"has_booking": false, "reasoning": "No signals"}\n```'
        result = parse_json_response(text)
        assert result["has_booking"] is False

    def test_chatty_prefix(self):
        text = 'After analyzing the HTML, I found:\n\n{"has_booking": true, "booking_platform": "FareHarbor"}'
        result = parse_json_response(text)
        assert result["has_booking"] is True
        assert result["booking_platform"] == "FareHarbor"

    def test_whitespace_padding(self):
        text = '  \n  {"has_booking": false}  \n  '
        result = parse_json_response(text)
        assert result["has_booking"] is False

    def test_null_values(self):
        text = '{"has_booking": false, "booking_platform": null, "reasoning": null}'
        result = parse_json_response(text)
        assert result["booking_platform"] is None

    def test_nested_json(self):
        text = '{"has_booking": true, "booking_platform": "Rezdy", "reasoning": "Found widget"}'
        result = parse_json_response(text)
        assert result["reasoning"] == "Found widget"

    def test_markdown_with_extra_text_after(self):
        text = '```json\n{"has_booking": true}\n```\nHope this helps!'
        result = parse_json_response(text)
        assert result["has_booking"] is True

    def test_curly_braces_extraction(self):
        text = 'The answer is: {"has_booking": false, "booking_platform": null} based on my analysis.'
        result = parse_json_response(text)
        assert result["has_booking"] is False

    def test_raises_on_garbage(self):
        with pytest.raises(ValueError):
            parse_json_response("I don't know what to say")

    def test_raises_on_empty(self):
        with pytest.raises(ValueError):
            parse_json_response("")

    def test_raises_on_invalid_json_in_block(self):
        text = '```json\n{broken json here}\n```'
        with pytest.raises(ValueError):
            parse_json_response(text)


# -------------------------------------------------------------------
# Prompt loading
# -------------------------------------------------------------------

class TestLoadPrompt:
    def test_html_prompt_loads(self):
        prompt = load_prompt(str(PROMPTS_DIR / "booking_detection_html.txt"))
        assert "{homepage_html}" in prompt
        assert "has_booking" in prompt

    def test_crawled_prompt_loads(self):
        prompt = load_prompt(str(PROMPTS_DIR / "booking_detection_crawled.txt"))
        assert "{domain}" in prompt
        assert "{page_content}" in prompt

    def test_own_site_prompt_loads(self):
        prompt = load_prompt(str(PROMPTS_DIR / "booking_detection_own_site.md"))
        assert "{domain}" in prompt

    def test_missing_file_raises(self):
        with pytest.raises(FileNotFoundError):
            load_prompt("/nonexistent/path/prompt.txt")
