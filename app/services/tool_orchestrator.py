from __future__ import annotations

import json
import logging
import re
import uuid
from collections.abc import AsyncIterator
from pathlib import Path
from typing import Any

from app.models.chat import ChatSettings
from app.services.llm import LLMClient, LlamaServerUnavailableError
from app.services.mcp_client import MCPClient
from app.services.retriever import Retriever

logger = logging.getLogger(__name__)

# Resolved relative to this file: app/services/ -> app/ -> repo root
_DOWNLOADS_DIR = Path(__file__).resolve().parent.parent.parent / "data" / "downloads"

# Tool results larger than this (chars) are written to a file instead of
# being sent inline.  10 000 chars ≈ ~7 500 tokens — well above what most
# models can usefully process in a single context window.
_LARGE_RESULT_THRESHOLD = 10_000
_LLM_PREVIEW_LEN = 500  # chars of preview sent to the LLM

_MAX_TOOL_ITERATIONS = 10

# Matches Mistral's [TOOL_CALLS] token followed by a JSON array
_MISTRAL_TOOL_RE = re.compile(r"\[TOOL_CALLS\]\s*(\[.*?\])", re.DOTALL)

# Matches an image URL produced by the generate_map tool
_IMAGE_URL_RE = re.compile(r"/api/files/[^\s]+\.png")


def _extract_image_url(result: str) -> str | None:
    """Return the first /api/files/*.png URL found in *result*, or None."""
    m = _IMAGE_URL_RE.search(result)
    return m.group(0) if m else None


def _parse_mistral_tool_calls(content: str) -> tuple[list[dict[str, Any]], str]:
    """
    Detect and normalise a Mistral-style [TOOL_CALLS] block into the OpenAI
    tool_calls structure.  Returns (tool_calls, cleaned_content).
    """
    match = _MISTRAL_TOOL_RE.search(content)
    if not match:
        return [], content

    try:
        raw_calls: list[dict[str, Any]] = json.loads(match.group(1))
    except json.JSONDecodeError:
        logger.warning("Failed to parse Mistral tool calls JSON")
        return [], content

    tool_calls: list[dict[str, Any]] = []
    for i, call in enumerate(raw_calls):
        tool_calls.append(
            {
                "id": f"call_mistral_{i}",
                "type": "function",
                "function": {
                    "name": call.get("name", ""),
                    "arguments": json.dumps(call.get("arguments", {})),
                },
            }
        )

    cleaned = content[: match.start()].strip()
    return tool_calls, cleaned


def _content_item_to_str(item: Any) -> str:
    """Serialize a single MCP content item to a string for the tool result message."""
    if hasattr(item, "text"):
        return item.text
    if isinstance(item, dict):
        return item.get("text", json.dumps(item))
    return str(item)


def _serialize_tool_result(content: Any) -> str:
    if isinstance(content, list):
        parts = [_content_item_to_str(c) for c in content]
        return "\n".join(parts)
    return _content_item_to_str(content)


def _save_large_result(tool_name: str, result: str) -> tuple[str, str]:
    """
    Write *result* to a file in the downloads directory.

    Returns (filename, download_url) so the orchestrator can emit the right
    SSE event and pass a trimmed summary to the LLM.
    """
    _DOWNLOADS_DIR.mkdir(parents=True, exist_ok=True)
    # Sanitise tool name for use in filename
    safe_name = re.sub(r"[^\w\-]", "_", tool_name)[:40]
    filename = f"{uuid.uuid4().hex}_{safe_name}.txt"
    (_DOWNLOADS_DIR / filename).write_text(result, encoding="utf-8")
    return filename, f"/api/files/{filename}"

# Characters-per-token approximation for history trimming.
# Real tokeniser not available here; 3.5 chars/token is conservative for
# mixed English + JSON content.
_CHARS_PER_TOKEN = 3.5
# Leave this much headroom for the model's own reply + tool schemas.
_CTX_HEADROOM_TOKENS = 2048


def _trim_messages(messages: list[dict[str, Any]], ctx_tokens: int) -> list[dict[str, Any]]:
    """
    Drop the oldest non-system messages until the estimated token count fits
    within ctx_tokens minus _CTX_HEADROOM_TOKENS.
    The system message (index 0) is always kept.
    """
    budget = (ctx_tokens - _CTX_HEADROOM_TOKENS) * _CHARS_PER_TOKEN
    while len(messages) > 1:
        total = sum(len(json.dumps(m)) for m in messages)
        if total <= budget:
            break
        # Remove the oldest non-system message
        messages.pop(1)
        logger.debug("History trimmed to %d messages to stay within context", len(messages))
    return messages


async def run_chat(
    history: list[dict[str, Any]],
    settings: ChatSettings,
    llm_client: LLMClient,
    mcp_client: MCPClient,
    retriever: Retriever | None = None,
) -> AsyncIterator[dict[str, Any]]:
    """
    Core tool-call loop.  Yields SSE event dicts:
      {"type": "token",       "text": "..."}
      {"type": "tool_start",  "name": "...", "args": {...}}
      {"type": "tool_result", "name": "...", "result": "..."}
      {"type": "tool_error",  "name": "...", "error": "..."}
      {"type": "done"}
      {"type": "error",       "message": "..."}
    """
    tools = mcp_client.tools if mcp_client.available else []
    logger.debug("run_chat: %d tool(s) available to model: %s",
                len(tools),
                [t["function"]["name"] for t in tools] or "(none)")

    # --- RAG: build a one-shot messages list with context appended to the
    #     system prompt. We never mutate the stored history. ----------------
    llm_messages = list(history)  # shallow copy — safe to modify positions

    # When tools are active, remind the model how to route tool calls correctly.
    if tools and llm_messages and llm_messages[0].get("role") == "system":
        tool_names = [t["function"]["name"] for t in tools]
        simbad_hint = (
            "\n\nTOOL USE POLICY (follow strictly — tools are a LAST resort, not a default):\n"
            "1. Answer from training knowledge first. If you know the answer, say it directly.\n"
            "2. If RAG context was injected above, use it to answer before considering any tool.\n"
            "3. Only call a tool when ALL of the following are true:\n"
            "   a) The user explicitly asked for a live lookup, a chart, weather, location, or the current time.\n"
            "   b) The answer cannot be found in your training knowledge or the provided context.\n"
            "\nTOOL ROUTING (only when rule 3 applies):\n"
            "- simbad_search: only when the user wants to look up a SPECIFIC named object "
            "(e.g. 'look up Betelgeuse', 'find data on M31') or search for objects by explicit criteria. "
            "Do NOT call it for general astronomy questions or explanations.\n"
            "- generate_constellation_map / generate_map: only when the user asks to SEE or SHOW a chart or map.\n"
            "- get_weather / get_latlong: only for weather or location queries.\n"
            "- get_current_time: only when the user asks what time it is.\n"
            "- Do NOT call any tool more than once for the same question.\n"
            "- Do NOT call any tool for greetings, general knowledge questions, explanations, "
            "or anything answerable from training or context.\n"
        ) if "simbad_search" in tool_names else ""
        if simbad_hint:
            llm_messages[0] = {
                **llm_messages[0],
                "content": llm_messages[0]["content"] + simbad_hint,
            }

    if retriever and retriever.available and retriever.document_count > 0:
        user_msgs = [m for m in history if m.get("role") == "user"]
        if user_msgs:
            query_text = user_msgs[-1].get("content", "")
            chunks = retriever.query(query_text)
            if chunks:
                context_text = "\n\n---\n\n".join(chunks)
                rag_addition = (
                    "\n\nThe following context was retrieved from the local knowledge base. "
                    "Use it to help answer the question.\n\n"
                    f"{context_text}"
                )

                if llm_messages and llm_messages[0].get("role") == "system":
                    # Append to existing system message (copy, don't modify history)
                    llm_messages[0] = {
                        **llm_messages[0],
                        "content": llm_messages[0]["content"] + rag_addition,
                    }
                else:
                    # No system message yet — prepend one
                    llm_messages.insert(0, {"role": "system", "content": rag_addition.strip()})

                logger.debug("RAG: injected %d chunk(s) into context", len(chunks))

    pending_image_url: str | None = None

    for iteration in range(_MAX_TOOL_ITERATIONS):
        # Trim history to avoid exceeding the model's context window.
        llm_messages = _trim_messages(llm_messages, settings.context_size)

        assistant_content = ""
        # tool_calls_acc is keyed by index
        tool_calls_acc: dict[int, dict[str, Any]] = {}

        try:
            async for chunk in llm_client.chat_stream(
                llm_messages,
                tools,
                temperature=settings.temperature,
                top_p=settings.top_p,
                max_tokens=settings.max_tokens,
            ):
                choices = chunk.get("choices", [])
                if not choices:
                    continue
                choice = choices[0]
                delta = choice.get("delta", {})

                # --- content tokens -----------------------------------------------
                token_text = delta.get("content") or ""
                if token_text:
                    assistant_content += token_text
                    yield {"type": "token", "text": token_text}

                # --- tool call deltas (OpenAI streaming format) --------------------
                for tc_delta in delta.get("tool_calls") or []:
                    idx: int = tc_delta.get("index", 0)
                    if idx not in tool_calls_acc:
                        tool_calls_acc[idx] = {
                            "id": f"call_{uuid.uuid4().hex[:8]}",  # fallback if server omits id
                            "type": "function",
                            "function": {"name": "", "arguments": ""},
                        }
                    entry = tool_calls_acc[idx]
                    if tc_delta.get("id"):
                        entry["id"] = tc_delta["id"]  # prefer server-supplied id
                    func_delta = tc_delta.get("function") or {}
                    if func_delta.get("name"):
                        entry["function"]["name"] += func_delta["name"]
                    if func_delta.get("arguments"):
                        entry["function"]["arguments"] += func_delta["arguments"]

        except LlamaServerUnavailableError as exc:
            yield {"type": "error", "message": str(exc)}
            return

        # --- Post-stream: compile tool calls --------------------------------------
        tool_calls: list[dict[str, Any]] = [
            tool_calls_acc[i] for i in sorted(tool_calls_acc)
        ]

        logger.debug("Raw assistant_content after stream: %r", assistant_content[:500])
        logger.debug("OpenAI-format tool_calls_acc: %s", tool_calls_acc)

        # Fallback: detect Mistral [TOOL_CALLS] format
        if not tool_calls and "[TOOL_CALLS]" in assistant_content:
            tool_calls, assistant_content = _parse_mistral_tool_calls(assistant_content)

        # --- No tool calls → final response, we're done ---------------------------
        if not tool_calls:
            break

        # --- Dispatch tool calls --------------------------------------------------
        # Use empty string rather than null — some llama-server builds reject
        # a null content field even when tool_calls are present.
        assistant_msg: dict[str, Any] = {
            "role": "assistant",
            "content": assistant_content,
            "tool_calls": tool_calls,
        }
        history.append(assistant_msg)
        llm_messages.append(assistant_msg)

        for tc in tool_calls:
            name = tc["function"]["name"]
            try:
                args: dict[str, Any] = json.loads(tc["function"]["arguments"] or "{}")
            except json.JSONDecodeError:
                args = {}

            yield {"type": "tool_start", "name": name, "args": args}
            logger.info("Tool call → %s  args=%s", name, json.dumps(args, ensure_ascii=False)[:200])

            try:
                raw_result = await mcp_client.call_tool(name, args)
                result_str = _serialize_tool_result(raw_result)
                logger.info("Tool result ← %s  (%d chars)\n%s",
                            name, len(result_str),
                            result_str[:1000] + ("..." if len(result_str) > 1000 else ""))

                if len(result_str) > _LARGE_RESULT_THRESHOLD:
                    filename, url = _save_large_result(name, result_str)
                    logger.debug("Large result saved → %s", filename)
                    preview = result_str[:_LLM_PREVIEW_LEN]
                    yield {
                        "type": "tool_download",
                        "name": name,
                        "url": url,
                        "size": len(result_str),
                        "preview": preview,
                    }
                    # Give the LLM a short summary so it can refer to the file
                    llm_content = (
                        f"[Result set too large to include inline ({len(result_str):,} chars). "
                        f"Saved for download at: {url}\n"
                        f"Preview (first {_LLM_PREVIEW_LEN} chars):\n{preview}]"
                    )
                else:
                    yield {"type": "tool_result", "name": name, "result": result_str}
                    # Emit an inline image event when the result contains a PNG URL
                    image_url = _extract_image_url(result_str)
                    logger.info("Tool image URL extracted from %s result: %s", name, image_url)
                    if image_url:
                        pending_image_url = image_url
                        yield {"type": "tool_image", "name": name, "url": image_url}
                    llm_content = result_str

                tool_msg: dict[str, Any] = {
                    "role": "tool",
                    "tool_call_id": tc["id"],
                    "content": llm_content,
                }
                history.append(tool_msg)
                llm_messages.append(tool_msg)
            except Exception as exc:
                error_str = str(exc)
                logger.warning("Tool error ← %s: %s", name, error_str)
                yield {"type": "tool_error", "name": name, "error": error_str}
                error_msg: dict[str, Any] = {
                    "role": "tool",
                    "tool_call_id": tc["id"],
                    "content": (
                        f"Error: {error_str}\n"
                        "The tool call failed. Answer the user's question from your "
                        "own knowledge instead and do not call any more tools."
                    ),
                }
                history.append(error_msg)
                llm_messages.append(error_msg)
                # Strip tools so the follow-up LLM call is forced to answer
                # from knowledge rather than attempting another tool call.
                tools = []
                logger.debug("Tool error — tools suppressed for follow-up LLM call")

        # Loop back for next LLM call with tool results injected

    else:
        logger.warning("Tool call loop hit max iterations (%d)", _MAX_TOOL_ITERATIONS)

    # Append the generated star map using the Image: format so the highlight
    # renderer converts it to a clickable thumbnail (same as addToolImage)
    # in case the tool_image SSE event was missed.
    if pending_image_url:
        yield {"type": "token", "text": f"\nImage: {pending_image_url}"}

    yield {"type": "done"}
