from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import AsyncIterator
from typing import Any

import httpx

logger = logging.getLogger(__name__)

_RETRIES = 3
_RETRY_BASE = 0.5  # seconds; doubles per attempt
_CONTROL_TOKEN_STOPS = [
    "<|im_start|>",
    "<|im_end|>",
    "<|assistant|>",
    "<|user|>",
]

# Sliding-window repetition detector: if the same phrase of at least this
# many characters appears this many times in the accumulated output, the
# stream is considered looping and will be truncated.
_LOOP_MIN_PHRASE_LEN = 40   # chars
_LOOP_MAX_PHRASE_LEN = 300  # avoid matching against the whole output
_LOOP_REPEAT_COUNT   = 3    # how many non-overlapping occurrences = loop
_LOOP_CHECK_WINDOW   = 2000 # only scan the last N chars (performance)


def _sanitize_control_tokens(text: str) -> str:
    cleaned = text
    for marker in _CONTROL_TOKEN_STOPS:
        cleaned = cleaned.replace(marker, "")
    return cleaned


def _truncate_at_control_token(text: str) -> tuple[str, bool]:
    first_idx = -1
    for marker in _CONTROL_TOKEN_STOPS:
        idx = text.find(marker)
        if idx >= 0 and (first_idx < 0 or idx < first_idx):
            first_idx = idx
    if first_idx < 0:
        return text, False
    return text[:first_idx], True


def _detect_output_loop(accumulated: str) -> bool:
    """
    Return True when the tail of *accumulated* contains the same phrase
    repeated _LOOP_REPEAT_COUNT or more times.  Only the last
    _LOOP_CHECK_WINDOW characters are scanned for performance.
    """
    window = accumulated[-_LOOP_CHECK_WINDOW:]
    n = len(window)
    # Slide phrase lengths from large to small so we catch the smallest
    # repeating unit first (avoids false positives from short common words).
    for phrase_len in range(_LOOP_MAX_PHRASE_LEN, _LOOP_MIN_PHRASE_LEN - 1, -1):
        if phrase_len >= n:
            continue
        # Anchor at the very end of the window: the most recent phrase.
        candidate = window[n - phrase_len:]
        count = 0
        pos = 0
        while True:
            idx = window.find(candidate, pos)
            if idx < 0:
                break
            count += 1
            if count >= _LOOP_REPEAT_COUNT:
                return True
            pos = idx + phrase_len  # non-overlapping
    return False


class LlamaServerUnavailableError(Exception):
    pass


class LLMClient:
    """
    Async client for llama-server's OpenAI-compatible /v1/chat/completions endpoint.
    Uses httpx with streaming enabled; retries on transient connection errors.
    """

    def __init__(self, base_url: str) -> None:
        self.base_url = base_url.rstrip("/")
        self._client = httpx.AsyncClient(
            timeout=httpx.Timeout(120.0, connect=5.0),
        )

    async def aclose(self) -> None:
        await self._client.aclose()

    async def is_healthy(self) -> bool:
        """Return True if llama-server responds to GET /health."""
        try:
            resp = await self._client.get(f"{self.base_url}/health", timeout=3.0)
            return resp.status_code < 500
        except Exception:
            return False

    async def chat_stream(
        self,
        messages: list[dict[str, Any]],
        tools: list[dict[str, Any]],
        temperature: float = 0.7,
        top_p: float = 0.9,
        max_tokens: int = 1024,
        repetition_penalty: float = 1.15,
    ) -> AsyncIterator[dict[str, Any]]:
        """
        Yield raw parsed SSE data objects from llama-server.
        Each object is one parsed JSON chunk from the stream.
        Raises LlamaServerUnavailableError after exhausting retries.
        """
        # llama.cpp does not support streaming when tools are provided
        use_stream = not bool(tools)
        payload: dict[str, Any] = {
            "messages": messages,
            "temperature": temperature,
            "top_p": top_p,
            "max_tokens": max_tokens,
            "repeat_penalty": repetition_penalty,
            "repeat_last_n": -1,       # penalise over the full context, not just 64 tokens
            "frequency_penalty": 0.15, # extra per-token frequency dampening
            "stream": use_stream,
            "stop": _CONTROL_TOKEN_STOPS,
        }
        if tools:
            payload["tools"] = tools
            # Tool-call argument JSON can be large; ensure there is enough
            # token budget so llama-server never truncates it mid-string.
            payload["max_tokens"] = max(max_tokens, 2048)

        logger.debug("LLM payload → %d message(s), %d tool(s): %s",
                    len(messages),
                    len(tools),
                    [t["function"]["name"] for t in tools] or "(none)")

        url = f"{self.base_url}/v1/chat/completions"
        for attempt in range(_RETRIES):
            try:
                if not use_stream:
                    # Non-streaming path: used when tools are present since
                    # llama.cpp does not support stream=true with tools.
                    resp = await self._client.post(url, json=payload)
                    if resp.status_code >= 400:
                        logger.error(
                            "llama-server HTTP %d: %s",
                            resp.status_code,
                            resp.text[:1000],
                        )
                    resp.raise_for_status()
                    chunk = resp.json()
                    # Normalise to streaming-style delta so callers see a
                    # consistent shape: wrap message as a delta chunk.
                    choices = chunk.get("choices", [])
                    if choices and "message" in choices[0] and "delta" not in choices[0]:
                        choices[0]["delta"] = choices[0]["message"]
                    yield chunk
                    return

                async with self._client.stream("POST", url, json=payload) as resp:
                    if resp.status_code >= 400:
                        await resp.aread()
                        logger.error(
                            "llama-server HTTP %d: %s",
                            resp.status_code,
                            resp.text[:1000],
                        )
                    resp.raise_for_status()
                    accumulated_content = ""
                    async for raw_line in resp.aiter_lines():
                        if not raw_line.startswith("data:"):
                            continue
                        data_str = raw_line[5:].strip()
                        if data_str == "[DONE]":
                            return
                        try:
                            chunk = json.loads(data_str)
                            # Guard against template-control token leakage from
                            # model variants that emit raw chat markers.
                            choices = chunk.get("choices", [])
                            stop_after_chunk = False
                            if choices:
                                delta = choices[0].get("delta", {})
                                content = delta.get("content")
                                if isinstance(content, str) and content:
                                    safe_content, found_control = _truncate_at_control_token(content)
                                    delta["content"] = _sanitize_control_tokens(safe_content)
                                    stop_after_chunk = found_control
                                    accumulated_content += delta["content"]
                            yield chunk
                            if stop_after_chunk:
                                logger.warning("Control token detected in model stream; truncating response")
                                return
                            # Repetition loop guard: check every ~200 chars of new output.
                            if len(accumulated_content) > _LOOP_CHECK_WINDOW // 2 and _detect_output_loop(accumulated_content):
                                logger.warning(
                                    "Output loop detected after %d chars; truncating stream",
                                    len(accumulated_content),
                                )
                                # Signal the caller so it can emit a user-visible notice.
                                yield {"choices": [{"delta": {"content": ""}, "finish_reason": "loop_truncated"}]}
                                return
                        except json.JSONDecodeError:
                            logger.debug("Skipping malformed SSE line: %r", raw_line)
                return  # successful stream finished

            except httpx.ConnectError as exc:
                if attempt == _RETRIES - 1:
                    raise LlamaServerUnavailableError(
                        f"llama-server unreachable after {_RETRIES} attempts"
                    ) from exc
                wait = _RETRY_BASE * (2**attempt)
                logger.warning("llama-server connect error (attempt %d/%d), retrying in %.1fs", attempt + 1, _RETRIES, wait)
                await asyncio.sleep(wait)

            except httpx.HTTPStatusError as exc:
                raise LlamaServerUnavailableError(
                    f"llama-server returned HTTP {exc.response.status_code}"
                ) from exc
