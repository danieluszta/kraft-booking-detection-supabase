"""
Standalone OpenAI LLM analysis module.

Provides prompt-template-based structured extraction via OpenAI chat completions,
with JSON response parsing and retry logic. No database dependencies.
"""

import json
import logging
import time

from openai import OpenAI, APIError, RateLimitError, APITimeoutError

__all__ = ["parse_json_response", "analyze", "load_prompt"]

logger = logging.getLogger(__name__)


def parse_json_response(response_text: str) -> dict:
    """Parse JSON from an LLM response, handling various formats.

    Attempts three strategies in order:
      1. Direct json.loads() on the full response text
      2. Extract from markdown code blocks (```json ... ```)
      3. Extract between the first '{' and last '}'

    Args:
        response_text: Raw text returned by the LLM.

    Returns:
        Parsed dictionary.

    Raises:
        ValueError: If no valid JSON can be extracted.
    """
    # Strategy 1: direct parse
    try:
        return json.loads(response_text)
    except json.JSONDecodeError:
        pass

    # Strategy 2: markdown code block
    if "```json" in response_text:
        start = response_text.find("```json") + 7
        end = response_text.find("```", start)
        if end != -1:
            json_str = response_text[start:end].strip()
            try:
                return json.loads(json_str)
            except json.JSONDecodeError:
                pass

    # Strategy 3: first '{' to last '}'
    if "{" in response_text and "}" in response_text:
        start = response_text.find("{")
        end = response_text.rfind("}") + 1
        json_str = response_text[start:end].strip()
        try:
            return json.loads(json_str)
        except json.JSONDecodeError:
            pass

    raise ValueError(f"Could not parse JSON from response: {response_text[:200]}")


def analyze(
    text: str,
    prompt_template: str,
    api_key: str,
    model: str = "gpt-4o-mini",
    temperature: float = 0.1,
    placeholders: dict = None,
) -> dict:
    """Run a single OpenAI analysis using a prompt template.

    Fills placeholders in the template, sends a chat completion request,
    and parses the JSON response. Retries up to 3 times on transient API errors.

    Args:
        text: The primary text to analyze (inserted as {text} in the template).
        prompt_template: Prompt string with {key} placeholders.
        api_key: OpenAI API key.
        model: OpenAI model identifier.
        temperature: Sampling temperature.
        placeholders: Optional dict of additional placeholder key/value pairs.

    Returns:
        Dict with keys: status, parsed, raw_response, error.
    """
    # Build the complete set of placeholders
    all_placeholders = {"text": text}
    if placeholders:
        all_placeholders.update(placeholders)

    # Fill the prompt template
    prompt_text = prompt_template
    for key, value in all_placeholders.items():
        placeholder = "{" + key + "}"
        if placeholder in prompt_text:
            prompt_text = prompt_text.replace(placeholder, str(value))

    # Retry logic
    max_retries = 3
    client = OpenAI(api_key=api_key)

    for attempt in range(1, max_retries + 1):
        try:
            logger.debug(
                "OpenAI call attempt %d/%d  model=%s  prompt_len=%d",
                attempt,
                max_retries,
                model,
                len(prompt_text),
            )

            response = client.chat.completions.create(
                model=model,
                messages=[
                    {
                        "role": "system",
                        "content": "You are a data extraction assistant. Always return valid JSON.",
                    },
                    {"role": "user", "content": prompt_text},
                ],
                temperature=temperature,
            )

            raw_response = response.choices[0].message.content
            parsed = parse_json_response(raw_response)

            return {
                "status": "success",
                "parsed": parsed,
                "raw_response": raw_response,
                "error": None,
            }

        except (RateLimitError, APITimeoutError, APIError) as e:
            # Retry on transient API errors
            if attempt < max_retries:
                wait = 2 ** attempt
                logger.warning(
                    "OpenAI API error (attempt %d/%d): %s — retrying in %ds",
                    attempt,
                    max_retries,
                    e,
                    wait,
                )
                time.sleep(wait)
            else:
                logger.error("OpenAI API error after %d attempts: %s", max_retries, e)
                return {
                    "status": "error",
                    "parsed": None,
                    "raw_response": None,
                    "error": str(e),
                }

        except ValueError as e:
            # JSON parsing failure — not transient, don't retry
            logger.error("JSON parse error: %s", e)
            return {
                "status": "error",
                "parsed": None,
                "raw_response": raw_response if "raw_response" in locals() else None,
                "error": str(e),
            }

        except Exception as e:
            logger.error("Unexpected error: %s", e)
            return {
                "status": "error",
                "parsed": None,
                "raw_response": None,
                "error": str(e),
            }

    # Should not be reached, but guard anyway
    return {
        "status": "error",
        "parsed": None,
        "raw_response": None,
        "error": "Max retries exhausted",
    }


def load_prompt(prompt_path: str) -> str:
    """Read a prompt template file and return its contents.

    Args:
        prompt_path: Path to the prompt template file.

    Returns:
        The prompt template string.

    Raises:
        FileNotFoundError: If the file does not exist.
    """
    with open(prompt_path, "r", encoding="utf-8") as f:
        return f.read()
