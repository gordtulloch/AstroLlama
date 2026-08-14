from __future__ import annotations

import asyncio
import json
import logging
import re
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


# CJK characters that can leak into English output from multilingual models
# such as Qwen (e.g. "what would您 like" — 您 = formal Chinese "you").
_CJK_CHAR_RE = re.compile(
    "["
    "　-〿"      # CJK symbols and punctuation
    "぀-ヿ"      # Hiragana and Katakana
    "㐀-䶿"      # CJK Extension A
    "一-鿿"      # CJK Unified Ideographs (main block)
    "豈-﫿"      # CJK Compatibility Ideographs
    "︰-﹏"      # CJK Compatibility Forms
    "\U00020000-\U0002a6df"  # CJK Extension B
    "]+"
)
_CJK_ASCII_THRESHOLD = 0.90  # texts above this ASCII ratio are treated as English


def _sanitize_mixed_script(text: str) -> str:
    """Strip spurious CJK characters from predominantly ASCII/Latin content.

    Small quantized multilingual models (e.g. Qwen) occasionally emit CJK
    characters mid-sentence in otherwise English responses. This strips them
    when the surrounding content is ≥70% ASCII, but leaves full CJK text
    (e.g. a legitimate Chinese-language answer) untouched.
    """
    if not text or not _CJK_CHAR_RE.search(text):
        return text
    non_space = [c for c in text if not c.isspace()]
    if not non_space:
        return text
    ascii_ratio = sum(1 for c in non_space if ord(c) < 128) / len(non_space)
    if ascii_ratio >= _CJK_ASCII_THRESHOLD:
        cleaned = _CJK_CHAR_RE.sub("", text)
        if cleaned != text:
            logger.debug("Stripped spurious CJK chars from content (ASCII ratio %.2f)", ascii_ratio)
        return cleaned
    return text


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


def _strip_loop_tail(text: str, min_phrase: int = _LOOP_MIN_PHRASE_LEN, max_phrase: int = _LOOP_MAX_PHRASE_LEN, repeats: int = _LOOP_REPEAT_COUNT) -> str:
    """
    Remove the looping suffix from *text* by walking backwards through phrase
    lengths and cutting just before the first repeated occurrence. Mirrors
    app.services.tool_orchestrator._strip_repeated_tail; duplicated here
    (rather than imported) to avoid a circular import with that module.
    """
    for phrase_len in range(min(max_phrase, len(text) // repeats), min_phrase - 1, -1):
        candidate = text[-phrase_len:]
        count = 0
        pos = 0
        first_pos = -1
        while True:
            idx = text.find(candidate, pos)
            if idx < 0:
                break
            count += 1
            if first_pos < 0:
                first_pos = idx
            pos = idx + phrase_len
        if count >= repeats and first_pos >= 0:
            return text[:first_pos].rstrip()
    return text


# A distinct failure mode from word/phrase looping: strong repetition_penalty
# / frequency_penalty pushes the model away from anything already used, so it
# starts grabbing novel emoji tokens it hasn't emitted yet instead of real
# words. No phrase repeats (each emoji differs), so _detect_output_loop never
# fires — this needs its own run-length detector over emoji-class glyphs.
_EMOJI_CHAR_RE = re.compile(
    "["
    "\U0001F300-\U0001FAFF"  # symbols/pictographs through extended-A
    "\U00002600-\U000026FF"  # misc symbols
    "\U00002700-\U000027BF"  # dingbats
    "\U00002B00-\U00002BFF"  # misc symbols and arrows (stars etc.)
    "\U0001F1E6-\U0001F1FF"  # regional indicators (flags)
    "\U0000FE0F"             # variation selector-16
    "]"
)
_EMOJI_SPAM_MIN_RUN = 10  # consecutive emoji glyphs considered degenerate
_EMOJI_SPAM_CHECK_WINDOW = 300  # only scan the last N chars per chunk (performance)


def _find_emoji_spam_start(text: str) -> int | None:
    """
    Return the index where a run of >= _EMOJI_SPAM_MIN_RUN emoji characters
    begins, or None if no such run exists. Whitespace between emoji doesn't
    break a run (degenerate output is often "emoji emoji emoji ..."), but any
    other character does — normal usage rarely strings together more than a
    couple of emoji without intervening words.
    """
    run_start: int | None = None
    emoji_count = 0
    for i, ch in enumerate(text):
        if _EMOJI_CHAR_RE.match(ch):
            if run_start is None:
                run_start = i
            emoji_count += 1
            if emoji_count >= _EMOJI_SPAM_MIN_RUN:
                return run_start
        elif not ch.isspace():
            run_start = None
            emoji_count = 0
    return None


def _detect_emoji_spam(text: str) -> bool:
    return _find_emoji_spam_start(text) is not None


def _strip_emoji_spam_tail(text: str) -> str:
    idx = _find_emoji_spam_start(text)
    if idx is None:
        return text
    return text[:idx].rstrip()


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
                    # Apply the same control-token / output-loop guards used
                    # on the streaming path. The non-streaming path returns
                    # the full response in one shot, so both checks run once
                    # against the complete content. The "loop_truncated"
                    # signal must arrive as its own chunk *after* the content
                    # chunk — tool_orchestrator.run_chat() checks finish_reason
                    # before reading delta.content and breaks immediately, so
                    # packing both into one chunk would silently drop the text.
                    if choices:
                        delta = choices[0].get("delta", {})
                        content = delta.get("content")
                        truncation_reason: str | None = None
                        if isinstance(content, str) and content:
                            safe_content, found_control = _truncate_at_control_token(content)
                            safe_content = _sanitize_control_tokens(safe_content)
                            safe_content = _sanitize_mixed_script(safe_content)
                            if found_control:
                                logger.warning("Control token detected in model response; truncating")
                            elif _detect_output_loop(safe_content):
                                logger.warning(
                                    "Output loop detected in non-streaming response (%d chars); truncating",
                                    len(safe_content),
                                )
                                truncation_reason = "loop_truncated"
                                safe_content = _strip_loop_tail(safe_content)
                            elif _detect_emoji_spam(safe_content):
                                logger.warning(
                                    "Emoji spam detected in non-streaming response (%d chars); truncating",
                                    len(safe_content),
                                )
                                truncation_reason = "emoji_spam_truncated"
                                safe_content = _strip_emoji_spam_tail(safe_content)
                            delta["content"] = safe_content
                        if truncation_reason:
                            yield chunk
                            yield {"choices": [{"delta": {"content": ""}, "finish_reason": truncation_reason}]}
                            return
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
                                    safe_content = _sanitize_control_tokens(safe_content)
                                    delta["content"] = _sanitize_mixed_script(safe_content)
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
                            # Emoji-enumeration guard: distinct from the loop guard above —
                            # no phrase repeats here, just a long run of distinct emoji.
                            if _detect_emoji_spam(accumulated_content[-_EMOJI_SPAM_CHECK_WINDOW:]):
                                logger.warning(
                                    "Emoji spam detected after %d chars; truncating stream",
                                    len(accumulated_content),
                                )
                                yield {"choices": [{"delta": {"content": ""}, "finish_reason": "emoji_spam_truncated"}]}
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
