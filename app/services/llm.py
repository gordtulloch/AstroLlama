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
    ) -> AsyncIterator[dict[str, Any]]:
        """
        Yield raw parsed SSE data objects from llama-server.
        Each object is one parsed JSON chunk from the stream.
        Raises LlamaServerUnavailableError after exhausting retries.
        """
        payload: dict[str, Any] = {
            "messages": messages,
            "temperature": temperature,
            "top_p": top_p,
            "max_tokens": max_tokens,
            "stream": True,
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
                async with self._client.stream("POST", url, json=payload) as resp:
                    if resp.status_code >= 400:
                        await resp.aread()
                        logger.error(
                            "llama-server HTTP %d: %s",
                            resp.status_code,
                            resp.text[:1000],
                        )
                    resp.raise_for_status()
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
                            yield chunk
                            if stop_after_chunk:
                                logger.warning("Control token detected in model stream; truncating response")
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
