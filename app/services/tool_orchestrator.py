from __future__ import annotations

import json
import logging
import re
import uuid
from difflib import SequenceMatcher
from collections.abc import AsyncIterator
from pathlib import Path
from typing import Any

import httpx

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

_ASR_DISAMBIGUATION_HINT = (
    "\n\nVOICE TRANSCRIPTION DISAMBIGUATION POLICY:\n"
    "- Users may be speaking via browser speech recognition; occasional mistranscriptions are expected.\n"
    "- Use recent conversation context to infer likely intended astronomy terms when wording appears phonetically close but semantically odd.\n"
    "- Example: if prior context mentions Cepheid variables and the user asks about 'seafood variables', interpret this as 'Cepheid variables'.\n"
    "- Apply this only when confidence is high from context and phonetic similarity; otherwise ask a brief clarification question.\n"
    "- If you corrected a likely transcription error, proceed with the corrected term and briefly note the interpreted term in one short phrase.\n"
)

# Matches Mistral's [TOOL_CALLS] token followed by a JSON array
_MISTRAL_TOOL_RE = re.compile(r"\[TOOL_CALLS\]\s*(\[.*?\])", re.DOTALL)

# Matches an image URL produced by the generate_map tool
_IMAGE_URL_RE = re.compile(r"/api/files/[^\s]+\.png")
_CHAT_CONTROL_RE = re.compile(r"<\|im_start\|>|<\|im_end\|>|<\|assistant\|>|<\|user\|>")

_ASR_ALIAS_MAP: dict[str, str] = {
    "delta cpi": "Delta Cephei",
    "delta c p i": "Delta Cephei",
    "seafood variables": "Cepheid variables",
}

_TOOL_NAME_QUERY_KEYS = {
    "object",
    "object_name",
    "name",
    "query",
    "target",
    "star",
    "designation",
}

_LATLONG_INTENT_RE = re.compile(
    r"\b(latitude\s*(?:and|&)\s*longitude|latitude|longitude|lat\s*/?\s*long|lat\s+lon|coordinates?)\b",
    re.IGNORECASE,
)

_AAVSO_FINDER_INTENT_RE = re.compile(
    r"\b(aavso|finder\s+chart|variable\s+star\s+finder\s+chart)\b",
    re.IGNORECASE,
)

_ALPACA_CAPTURE_TARGET_PATTERNS = [
    re.compile(
        r"\b(?:move|slew|point)(?:\s+the\s+telescope)?\s+to\s+(.+?)(?:\s+(?:and|then)\s+|[,.!?]|$)",
        re.IGNORECASE,
    ),
    re.compile(
        r"\b(?:take|capture|image)\s+(?:an?\s+|\d+\s+)?(?:\d+(?:\.\d+)?\s*(?:second|seconds|sec|s)\s+)?(?:images?|exposures?|frames?)\s+(?:of|for)\s+(.+?)(?:\s+(?:and|then)\s+|[,.!?]|$)",
        re.IGNORECASE,
    ),
]


def _extract_alpaca_slew_plate_solve_request(text: str) -> dict[str, Any] | None:
    """Best-effort extraction for explicit telescope slew + plate-solve commands."""
    raw = str(text or "").strip()
    if not raw:
        return None

    lowered = raw.lower()
    if not any(term in lowered for term in ["platesolve", "plate solve", "plate-solve"]):
        return None
    if not any(term in lowered for term in ["slew", "move", "point", "telescope"]):
        return None

    target: str | None = None
    for pattern in _ALPACA_CAPTURE_TARGET_PATTERNS:
        match = pattern.search(raw)
        if match:
            candidate = match.group(1).strip(" ?.!,:;")
            if candidate:
                target = candidate
                break

    if not target:
        return None

    args: dict[str, Any] = {"object_name": target}
    exposure_match = re.search(
        r"\b(\d+(?:\.\d+)?)\s*(?:second|seconds|sec|s)\b",
        raw,
        flags=re.IGNORECASE,
    )
    if exposure_match:
        args["plate_solve_exposure_seconds"] = float(exposure_match.group(1))

    return args


def _extract_alpaca_current_plate_solve_request(text: str) -> dict[str, Any] | None:
    """Best-effort extraction for explicit plate-solve-at-current-pointing commands."""
    raw = str(text or "").strip()
    if not raw:
        return None

    lowered = raw.lower()
    has_plate_solve_term = any(term in lowered for term in ["platesolve", "plate solve", "plate-solve"])
    has_current_pointing_term = any(
        term in lowered
        for term in ["current", "current location", "current position", "current pointing", "where i am", "here"]
    )
    has_verify_current_term = (
        any(term in lowered for term in ["verify", "check", "confirm"])
        and any(term in lowered for term in ["current pointing", "current position", "current location", "where i am", "here"])
    )

    if not ((has_plate_solve_term and has_current_pointing_term) or has_verify_current_term):
        return None

    args: dict[str, Any] = {}
    exposure_match = re.search(
        r"\b(\d+(?:\.\d+)?)\s*(?:second|seconds|sec|s)\b",
        raw,
        flags=re.IGNORECASE,
    )
    if exposure_match:
        args["plate_solve_exposure_seconds"] = float(exposure_match.group(1))

    return args


def _extract_location_for_latlong_query(text: str) -> str | None:
    """Best-effort extraction of location text from a lat/long user query."""
    raw = str(text or "").strip()
    if not raw:
        return None

    lowered = raw.lower()
    if not _LATLONG_INTENT_RE.search(lowered):
        return None

    patterns = [
        r"\b(?:what(?:'s| is)?\s+)?(?:the\s+)?(?:latitude\s*(?:and|&)\s*longitude|coordinates?)\s+(?:of|for)\s+(.+)$",
        r"\b(?:lat(?:itude)?\s*/?\s*long(?:itude)?|lat\s+lon)\s+(?:of|for)\s+(.+)$",
        r"\b(?:where\s+is)\s+(.+?)\s*(?:located)?\s*$",
    ]
    for pat in patterns:
        m = re.search(pat, lowered, flags=re.IGNORECASE)
        if m:
            candidate = raw[m.start(1):m.end(1)].strip(" ?.!,:;")
            if candidate:
                return candidate

    # Fallback: if the question is location-focused but extraction failed,
    # return the full text and let the geocoder fallback logic try to resolve it.
    return raw.strip(" ?.!")


def _extract_aavso_star_query(text: str) -> str | None:
    """Best-effort extraction of variable star target for AAVSO finder-chart requests."""
    raw = str(text or "").strip()
    if not raw:
        return None

    if not _AAVSO_FINDER_INTENT_RE.search(raw):
        return None

    patterns = [
        r"\b(?:aavso\s+)?(?:variable\s+star\s+finder\s+chart|finder\s+chart|chart)\s+(?:for|of)\s+(.+)$",
        r"\b(?:create|generate|show|make|build)\s+(?:an?\s+)?aavso\s+(?:finder\s+)?chart\s+(?:for|of)\s+(.+)$",
    ]
    for pat in patterns:
        m = re.search(pat, raw, flags=re.IGNORECASE)
        if m:
            candidate = raw[m.start(1):m.end(1)].strip(" ?.!,:;")
            if candidate:
                return candidate

    # Fall back to using text after the last "for"/"of" if it looks explicit.
    fallback = re.search(r"\b(?:for|of)\s+(.+)$", raw, flags=re.IGNORECASE)
    if fallback:
        candidate = raw[fallback.start(1):fallback.end(1)].strip(" ?.!,:;")
        if candidate:
            return candidate

    return None


def _extract_alpaca_capture_request(text: str) -> dict[str, Any] | None:
    """Best-effort extraction for explicit telescope slew/capture commands."""
    raw = str(text or "").strip()
    if not raw:
        return None

    lowered = raw.lower()
    if not any(term in lowered for term in ["exposure", "capture", "image", "slew", "telescope", "point"]):
        return None

    exposure_match = re.search(
        r"\b(\d+(?:\.\d+)?)\s*(?:second|seconds|sec|s)\b",
        raw,
        flags=re.IGNORECASE,
    )
    if not exposure_match:
        return None

    target: str | None = None
    for pattern in _ALPACA_CAPTURE_TARGET_PATTERNS:
        match = pattern.search(raw)
        if match:
            candidate = match.group(1).strip(" ?.!,:;")
            if candidate:
                target = candidate
                break

    if not target:
        return None

    exposure_count = 1
    count_patterns = [
        r"\b(?:take|capture|image)\s+(\d+)\s+\d+(?:\.\d+)?\s*(?:second|seconds|sec|s)\s+(?:images?|exposures?|frames?)\b",
        r"\b(\d+)\s*[x×]\s*\d+(?:\.\d+)?\s*(?:second|seconds|sec|s)\s+(?:images?|exposures?|frames?)\b",
    ]
    for pattern in count_patterns:
        match = re.search(pattern, raw, flags=re.IGNORECASE)
        if match:
            exposure_count = int(match.group(1))
            break

    light_frame = not bool(re.search(r"\bdark\s+frame\b", raw, flags=re.IGNORECASE))

    return {
        "object_name": target,
        "exposure_seconds": float(exposure_match.group(1)),
        "exposure_count": exposure_count,
        "light_frame": light_frame,
    }


def _find_tool_name(tools: list[dict[str, Any]], candidates: list[str]) -> str | None:
    available = {
        (t.get("function") or {}).get("name")
        for t in tools
        if isinstance(t, dict)
    }
    for name in candidates:
        if name in available:
            return name
    return None


def _summarize_geocode_result(result: str, requested_location: str) -> str:
    """Create a concise user-facing lat/long answer from tool output."""
    lat_match = re.search(r"Latitude:\s*([-+]?\d+(?:\.\d+)?)", result, flags=re.IGNORECASE)
    lon_match = re.search(r"Longitude:\s*([-+]?\d+(?:\.\d+)?)", result, flags=re.IGNORECASE)
    name_match = re.search(r"^\s*1\.\s+(.+)$", result, flags=re.MULTILINE)

    if lat_match and lon_match:
        place = (name_match.group(1).strip() if name_match else requested_location) or requested_location
        lat = lat_match.group(1)
        lon = lon_match.group(1)
        return f"{place}: latitude {lat}, longitude {lon}."

    # Fall back to raw tool text when parsing fails.
    return result


async def _direct_geocode_summary(location: str) -> str | None:
    """Best-effort direct geocode fallback if MCP geocode tool call fails."""
    location = str(location or "").strip()
    if not location:
        return None

    terms: list[str] = [location]
    if "," not in location:
        parts = [p for p in location.split() if p]
        if len(parts) >= 2:
            terms.append(f"{parts[0]}, {' '.join(parts[1:])}")
            terms.append(parts[0])
        elif len(parts) == 1:
            terms.append(parts[0])

    seen: set[str] = set()
    deduped_terms: list[str] = []
    for term in terms:
        key = term.lower()
        if key in seen:
            continue
        seen.add(key)
        deduped_terms.append(term)

    async with httpx.AsyncClient(timeout=10.0) as client:
        for term in deduped_terms:
            params = {"name": term, "count": 1, "language": "en", "format": "json"}
            try:
                resp = await client.get("https://geocoding-api.open-meteo.com/v1/search", params=params)
                resp.raise_for_status()
            except Exception:
                continue
            data = resp.json()
            results = data.get("results", [])
            if not results:
                continue

            r = results[0]
            name_parts = [r.get("name", "")]
            for field in ("admin1", "country"):
                v = r.get(field)
                if v:
                    name_parts.append(v)
            place = ", ".join([p for p in name_parts if p]) or location
            lat = r.get("latitude")
            lon = r.get("longitude")
            if lat is None or lon is None:
                continue
            return f"{place}: latitude {lat}, longitude {lon}."

    return None


def _sanitize_prompt_text(text: str) -> str:
    cleaned = _CHAT_CONTROL_RE.sub(" ", str(text or ""))
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    return cleaned


def _extract_image_url(result: str) -> str | None:
    """Return the first /api/files/*.png URL found in *result*, or None."""
    m = _IMAGE_URL_RE.search(result)
    return m.group(0) if m else None


def _normalize_phrase(text: str) -> str:
    return re.sub(r"\s+", " ", re.sub(r"[^a-z0-9\s]", " ", text.lower())).strip()


def _collect_context_terms(history: list[dict[str, Any]], limit: int = 10) -> list[str]:
    """Collect likely astronomy terms from recent turns for ASR disambiguation."""
    terms: list[str] = []
    seen: set[str] = set()

    for msg in history[-limit:]:
        content = str(msg.get("content") or "")
        if not content:
            continue

        # Prefer named entities / object-like phrases from recent context.
        for m in re.finditer(r"\b(?:[A-Z][A-Za-z0-9-]*)(?:\s+[A-Z][A-Za-z0-9-]*){0,2}\b", content):
            phrase = m.group(0).strip()
            norm = _normalize_phrase(phrase)
            if not norm or len(norm) < 4:
                continue
            if norm in seen:
                continue
            seen.add(norm)
            terms.append(phrase)

    # Always include explicit known high-value aliases.
    for canonical in _ASR_ALIAS_MAP.values():
        norm = _normalize_phrase(canonical)
        if norm and norm not in seen:
            seen.add(norm)
            terms.append(canonical)

    return terms


def _disambiguate_string_arg(text: str, context_terms: list[str]) -> tuple[str, str | None]:
    raw = str(text or "")
    if not raw.strip():
        return raw, None

    normalized_raw = _normalize_phrase(raw)

    # 1) Safe explicit substitutions for known repeated ASR failures.
    for alias, canonical in _ASR_ALIAS_MAP.items():
        if alias in normalized_raw:
            pattern = re.compile(rf"\b{re.escape(alias)}\b", re.IGNORECASE)
            replaced = pattern.sub(canonical, raw)
            if replaced != raw:
                return replaced, canonical
            return canonical, canonical

    # 2) Fuzzy whole-phrase correction for short object-like queries.
    word_count = len(normalized_raw.split())
    if word_count == 0 or word_count > 5 or len(normalized_raw) > 48:
        return raw, None

    best_term = ""
    best_score = 0.0
    first_raw = normalized_raw.split()[0]
    for term in context_terms:
        norm_term = _normalize_phrase(term)
        if not norm_term:
            continue
        score = SequenceMatcher(None, normalized_raw, norm_term).ratio()
        if score > best_score:
            best_score = score
            best_term = term

    if not best_term:
        return raw, None

    norm_best = _normalize_phrase(best_term)
    first_best = norm_best.split()[0] if norm_best else ""
    # Accept strong matches, or moderate matches when first token aligns
    # (e.g., "delta cpi" -> "Delta Cephei").
    if best_score >= 0.88 or (best_score >= 0.73 and first_raw == first_best):
        return best_term, best_term

    return raw, None


def _disambiguate_tool_args(
    name: str,
    args: dict[str, Any],
    history: list[dict[str, Any]],
) -> tuple[dict[str, Any], list[dict[str, str]]]:
    context_terms = _collect_context_terms(history)
    if not context_terms:
        return args, []

    changes: list[dict[str, str]] = []

    def walk(value: Any, key_hint: str = "") -> Any:
        if isinstance(value, dict):
            return {k: walk(v, k) for k, v in value.items()}
        if isinstance(value, list):
            return [walk(v, key_hint) for v in value]
        if not isinstance(value, str):
            return value

        key_norm = key_hint.lower().strip()
        is_query_like = key_norm in _TOOL_NAME_QUERY_KEYS or key_norm.endswith("name") or key_norm.endswith("query")

        # Disambiguate aggressively on query-like fields; conservatively otherwise.
        new_val, interpreted = _disambiguate_string_arg(value, context_terms)
        if interpreted and (is_query_like or new_val != value):
            if new_val != value:
                changes.append({"field": key_hint or "(value)", "from": value, "to": new_val})
            return new_val
        return value

    updated = walk(args)
    return updated, changes


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


def _alpaca_capture_start_user_message(args: dict[str, Any]) -> str:
    target = str(args.get("object_name") or "the target").strip() or "the target"
    exposure_seconds = args.get("exposure_seconds")
    exposure_count = int(args.get("exposure_count") or 1)

    try:
        exposure_value = float(exposure_seconds)
        exposure_text = str(int(exposure_value)) if exposure_value.is_integer() else str(exposure_value)
    except Exception:
        exposure_text = "requested"

    if exposure_text == "requested":
        capture_text = "A capture job has been started."
    elif exposure_count > 1:
        capture_text = f"A capture job for {exposure_count} x {exposure_text}-second exposures has been started."
    else:
        capture_text = f"A {exposure_text}-second exposure has been started."

    if target != "the target":
        capture_text = capture_text[:-1] + f" on {target}."

    return capture_text + " Ask for the capture status when you want an update."


def _append_hidden_tool_exchange(
    history: list[dict[str, Any]],
    tool_name: str,
    args: dict[str, Any],
    result_str: str,
) -> None:
    tool_call_id = f"hidden_{uuid.uuid4().hex[:8]}"
    history.append(
        {
            "role": "assistant",
            "content": "",
            "tool_calls": [
                {
                    "id": tool_call_id,
                    "type": "function",
                    "function": {
                        "name": tool_name,
                        "arguments": json.dumps(args),
                    },
                }
            ],
        }
    )
    history.append(
        {
            "role": "tool",
            "tool_call_id": tool_call_id,
            "content": result_str,
        }
    )


def _prepare_llm_tool_content(tool_name: str, result: str) -> str:
    """Return tool content plus tool-specific reply-format instructions for the LLM."""
    if tool_name == "search_papers" and "[" in result and "](" in result:
        return (
            "TOOL OUTPUT (use directly):\n"
            f"{result}\n\n"
            "RESPONSE FORMAT REQUIREMENT:\n"
            "- Preserve clickable markdown links from the tool output verbatim.\n"
            "- Do not replace links with plain text such as 'Read more'.\n"
            "- If listing papers, keep each title as [Title](URL).\n"
            "- Keep the [Summarize](astrollama://...) action link for each paper.\n"
            "- Prefer returning the same linked list from the tool output before any added commentary.\n"
        )
    return result


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
    emit_tool_events = not settings.hide_tool_bubbles
    logger.debug("run_chat: %d tool(s) available to model: %s",
                len(tools),
                [t["function"]["name"] for t in tools] or "(none)")

    async def _refresh_tools_from_mcp(reason: str) -> None:
        nonlocal tools
        if not mcp_client.available:
            return
        try:
            await mcp_client.reconnect()
            tools = mcp_client.tools if mcp_client.available else []
            logger.info(
                "Refreshed MCP tools for %s: %d tool(s)",
                reason,
                len(tools),
            )
        except Exception as exc:
            logger.warning("Failed to refresh MCP tools for %s: %s", reason, exc)

    # Deterministic fast-path for explicit coordinate questions.
    last_user_text = ""
    for msg in reversed(history):
        if msg.get("role") == "user":
            last_user_text = str(msg.get("content") or "")
            break

    location_query = _extract_location_for_latlong_query(last_user_text)
    geocode_tool = _find_tool_name(tools, ["get_lat_long", "get_latlong"])
    if location_query and geocode_tool:
        args = {"location": location_query}
        if emit_tool_events:
            yield {"type": "tool_start", "name": geocode_tool, "args": args}
        logger.info("Lat/long fast-path tool call → %s  args=%s", geocode_tool, json.dumps(args, ensure_ascii=False))
        try:
            raw_result = await mcp_client.call_tool(geocode_tool, args)
            result_str = _serialize_tool_result(raw_result)
            if emit_tool_events:
                yield {"type": "tool_result", "name": geocode_tool, "result": result_str}
            summary = _summarize_geocode_result(result_str, location_query)
            yield {"type": "token", "text": summary}
            yield {"type": "done"}
            return
        except Exception as exc:
            logger.warning("Lat/long fast-path failed, falling back to normal flow: %s", exc)
            if emit_tool_events:
                yield {"type": "tool_error", "name": geocode_tool, "error": str(exc)}
            direct_summary = await _direct_geocode_summary(location_query)
            if direct_summary:
                yield {"type": "token", "text": direct_summary}
                yield {"type": "done"}
                return

    # Deterministic fast-path for explicit AAVSO finder chart requests.
    aavso_target = _extract_aavso_star_query(last_user_text)
    aavso_tool = _find_tool_name(tools, ["generate_aavso_map"])
    if aavso_target and aavso_tool:
        args = {"star": aavso_target}
        if emit_tool_events:
            yield {"type": "tool_start", "name": aavso_tool, "args": args}
        logger.info("AAVSO fast-path tool call → %s  args=%s", aavso_tool, json.dumps(args, ensure_ascii=False))
        try:
            raw_result = await mcp_client.call_tool(aavso_tool, args)
            result_str = _serialize_tool_result(raw_result)
            if emit_tool_events:
                yield {"type": "tool_result", "name": aavso_tool, "result": result_str}
                image_url = _extract_image_url(result_str)
                if image_url:
                    yield {"type": "tool_image", "name": aavso_tool, "url": image_url}
            yield {"type": "token", "text": result_str}
            yield {"type": "done"}
            return
        except Exception as exc:
            logger.warning("AAVSO fast-path failed, falling back to normal flow: %s", exc)

    # Deterministic fast-path for explicit Alpaca slew/capture requests.
    alpaca_current_solve_args = _extract_alpaca_current_plate_solve_request(last_user_text)
    alpaca_current_solve_tool = _find_tool_name(
        tools,
        ["alpaca_plate_solve_current_position"],
    )
    if alpaca_current_solve_args is not None and not alpaca_current_solve_tool:
        await _refresh_tools_from_mcp("alpaca current plate-solve fast-path")
        alpaca_current_solve_tool = _find_tool_name(
            tools,
            ["alpaca_plate_solve_current_position"],
        )
    if alpaca_current_solve_args is not None and alpaca_current_solve_tool:
        if emit_tool_events:
            yield {"type": "tool_start", "name": alpaca_current_solve_tool, "args": alpaca_current_solve_args}
        logger.info(
            "Alpaca current plate-solve fast-path tool call -> %s  args=%s",
            alpaca_current_solve_tool,
            json.dumps(alpaca_current_solve_args, ensure_ascii=False),
        )
        try:
            raw_result = await mcp_client.call_tool(alpaca_current_solve_tool, alpaca_current_solve_args)
            result_str = _serialize_tool_result(raw_result)
            _append_hidden_tool_exchange(history, alpaca_current_solve_tool, alpaca_current_solve_args, result_str)
            if emit_tool_events:
                yield {"type": "tool_result", "name": alpaca_current_solve_tool, "result": result_str}
            yield {"type": "token", "text": result_str}
            yield {"type": "done"}
            return
        except Exception as exc:
            logger.warning("Alpaca current plate-solve fast-path failed, falling back to normal flow: %s", exc)
            if emit_tool_events:
                yield {"type": "tool_error", "name": alpaca_current_solve_tool, "error": str(exc)}

    # Deterministic fast-path for explicit Alpaca slew/capture requests.
    alpaca_slew_solve_args = _extract_alpaca_slew_plate_solve_request(last_user_text)
    alpaca_slew_solve_tool = _find_tool_name(
        tools,
        ["alpaca_slew_and_plate_solve"],
    )
    if alpaca_slew_solve_args and not alpaca_slew_solve_tool:
        await _refresh_tools_from_mcp("alpaca slew+plate-solve fast-path")
        alpaca_slew_solve_tool = _find_tool_name(
            tools,
            ["alpaca_slew_and_plate_solve"],
        )
    if alpaca_slew_solve_args and alpaca_slew_solve_tool:
        if emit_tool_events:
            yield {"type": "tool_start", "name": alpaca_slew_solve_tool, "args": alpaca_slew_solve_args}
        logger.info(
            "Alpaca slew+plate-solve fast-path tool call -> %s  args=%s",
            alpaca_slew_solve_tool,
            json.dumps(alpaca_slew_solve_args, ensure_ascii=False),
        )
        try:
            raw_result = await mcp_client.call_tool(alpaca_slew_solve_tool, alpaca_slew_solve_args)
            result_str = _serialize_tool_result(raw_result)
            _append_hidden_tool_exchange(history, alpaca_slew_solve_tool, alpaca_slew_solve_args, result_str)
            if emit_tool_events:
                yield {"type": "tool_result", "name": alpaca_slew_solve_tool, "result": result_str}
            yield {"type": "token", "text": result_str}
            yield {"type": "done"}
            return
        except Exception as exc:
            logger.warning("Alpaca slew+plate-solve fast-path failed, falling back to normal flow: %s", exc)
            if emit_tool_events:
                yield {"type": "tool_error", "name": alpaca_slew_solve_tool, "error": str(exc)}

    # Deterministic fast-path for explicit Alpaca slew/capture requests.
    alpaca_capture_args = _extract_alpaca_capture_request(last_user_text)
    alpaca_capture_tool = _find_tool_name(
        tools,
        ["alpaca_slew_and_capture", "alpaca_slew_and_capture_start"],
    )
    if alpaca_capture_args and alpaca_capture_tool:
        if emit_tool_events:
            yield {"type": "tool_start", "name": alpaca_capture_tool, "args": alpaca_capture_args}
        logger.info(
            "Alpaca capture fast-path tool call -> %s  args=%s",
            alpaca_capture_tool,
            json.dumps(alpaca_capture_args, ensure_ascii=False),
        )
        try:
            raw_result = await mcp_client.call_tool(alpaca_capture_tool, alpaca_capture_args)
            result_str = _serialize_tool_result(raw_result)
            _append_hidden_tool_exchange(history, alpaca_capture_tool, alpaca_capture_args, result_str)
            user_text = _alpaca_capture_start_user_message(alpaca_capture_args)
            if emit_tool_events:
                yield {"type": "tool_result", "name": alpaca_capture_tool, "result": user_text}
            yield {"type": "token", "text": user_text}
            yield {"type": "done"}
            return
        except Exception as exc:
            logger.warning("Alpaca capture fast-path failed, falling back to normal flow: %s", exc)
            if emit_tool_events:
                yield {"type": "tool_error", "name": alpaca_capture_tool, "error": str(exc)}

    # --- RAG: build a one-shot messages list with context appended to the
    #     system prompt. We never mutate the stored history. ----------------
    llm_messages = list(history)  # shallow copy — safe to modify positions

    # When tools are active, remind the model how to route tool calls correctly.
    if tools and llm_messages and llm_messages[0].get("role") == "system":
        tool_names = [t["function"]["name"] for t in tools]
        tool_policy = (
            "\n\nTOOL USE POLICY (follow strictly):\n"
            "1. Answer from your own training knowledge. If you know the answer, say it directly.\n"
            "2. If RAG context was injected above, use it to supplement your answer.\n"
            "3. Do NOT call any tool simply because the topic is astronomical or because you are uncertain.\n"
            "4. Never call orchestrate for a single direct action that one tool can execute immediately.\n"
            "   - Example: 'Move the telescope to M45 and take a 10 second exposure' should call alpaca_slew_and_capture directly.\n"
            "   - Example: 'Slew to M45 and platesolve' should call alpaca_slew_and_plate_solve directly.\n"
            "   - Example: 'Platesolve at current location and return current coordinates' should call alpaca_plate_solve_current_position directly.\n"
            "   - Use orchestrate only for true multi-step planning, explicit workflow design, or when critical information is missing and safe execution is impossible.\n"
            "5. Prefer the most specific execution tool over a planning tool.\n"
            "   - For telescope imaging requests with a clear target and exposure, call the Alpaca imaging tool directly.\n"
            "   - For discovery/setup questions, use Alpaca discovery or diagnostics tools directly.\n"
            "6. Only call a tool when the user has EXPLICITLY requested that specific action:\n"
            "   - simbad_lookup_object: use for ONE specific object (e.g. coordinates/properties of NGC 1015).\n"
            "   - simbad_search: use for multi-result list/browse queries (e.g. 'brightest stars', 'galaxies in Orion').\n"
            "     Never call either SIMBAD tool to answer general astronomy questions, explain concepts, or as a fallback.\n"
            "   - search_papers: ONLY if the user explicitly asks for papers, literature, or published research.\n"
            "     Never use to answer general astronomy questions (e.g., not for 'What are variable stars?').\n"
            "   - load_paper_html_text: When user asks to retrieve/read/summarize a specific paper.\n"
            "     This tool returns complete paper content. After calling it, summarize the content directly\n"
            "     WITHOUT calling any other tool. NEVER follow with summarize_news or any other tool.\n"
            "   - summarize_news: ONLY for news articles and current events.\n"
            "     NEVER use for arXiv papers or academic content.\n"
            "   - generate_constellation_map / generate_map: ONLY if the user asks to SEE or SHOW a chart or map.\n"
            "   - get_weather / get_latlong: ONLY for explicit weather or location queries.\n"
            "   - get_current_time: ONLY when the user asks what time it is.\n"
            "7. If you do not know something, say so plainly. Do NOT call a tool as a substitute for not knowing.\n"
            "8. Do NOT call any tool more than once for the same question. In particular:\n"
            "   - After load_paper_html_text returns content, summarize it directly (no follow-up tools).\n"
            "   - After simbad tools return results, answer directly (no follow-up data lookups).\n"
        )
        llm_messages[0] = {
            **llm_messages[0],
            "content": llm_messages[0]["content"] + tool_policy,
        }

    # Help the model recover likely speech-to-text mistakes using nearby context.
    if llm_messages and llm_messages[0].get("role") == "system":
        llm_messages[0] = {
            **llm_messages[0],
            "content": llm_messages[0]["content"] + _ASR_DISAMBIGUATION_HINT,
        }

    if retriever and retriever.available and retriever.document_count > 0:
        user_msgs = [m for m in history if m.get("role") == "user"]
        if user_msgs:
            query_text = user_msgs[-1].get("content", "")
            chunks = retriever.query(query_text)
            if chunks:
                clean_chunks = [_sanitize_prompt_text(c) for c in chunks]
                clean_chunks = [c for c in clean_chunks if c]
                context_text = "\n\n---\n\n".join(clean_chunks)
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
    called_tool_names: set[str] = set()
    force_no_more_tools = False

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

            if name in called_tool_names:
                duplicate_msg = (
                    f"Tool '{name}' was already called for this question. "
                    "Do not call it again; use the existing result and answer directly."
                )
                logger.info("Tool call blocked (duplicate) → %s", name)
                if emit_tool_events:
                    yield {"type": "tool_error", "name": name, "error": duplicate_msg}
                tool_msg: dict[str, Any] = {
                    "role": "tool",
                    "tool_call_id": tc["id"],
                    "content": duplicate_msg,
                }
                history.append(tool_msg)
                llm_messages.append(tool_msg)
                tools = []
                force_no_more_tools = True
                continue

            try:
                args: dict[str, Any] = json.loads(tc["function"]["arguments"] or "{}")
            except json.JSONDecodeError:
                args = {}

            args, disambiguation_changes = _disambiguate_tool_args(name, args, history)
            if disambiguation_changes:
                logger.info(
                    "Tool args disambiguated for %s: %s",
                    name,
                    json.dumps(disambiguation_changes, ensure_ascii=False),
                )

            if emit_tool_events:
                yield {"type": "tool_start", "name": name, "args": args}
            logger.info("Tool call → %s  args=%s", name, json.dumps(args, ensure_ascii=False)[:200])

            try:
                raw_result = await mcp_client.call_tool(name, args)
                called_tool_names.add(name)
                result_str = _serialize_tool_result(raw_result)
                logger.info("Tool result ← %s  (%d chars)\n%s",
                            name, len(result_str),
                            result_str[:1000] + ("..." if len(result_str) > 1000 else ""))

                if len(result_str) > _LARGE_RESULT_THRESHOLD:
                    filename, url = _save_large_result(name, result_str)
                    logger.debug("Large result saved → %s", filename)
                    preview = result_str[:_LLM_PREVIEW_LEN]
                    if emit_tool_events:
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
                    if emit_tool_events:
                        yield {"type": "tool_result", "name": name, "result": result_str}
                    # Emit an inline image event when the result contains a PNG URL
                    image_url = _extract_image_url(result_str)
                    logger.info("Tool image URL extracted from %s result: %s", name, image_url)
                    if image_url:
                        pending_image_url = image_url
                        if emit_tool_events:
                            yield {"type": "tool_image", "name": name, "url": image_url}
                    llm_content = _prepare_llm_tool_content(name, result_str)

                tool_msg: dict[str, Any] = {
                    "role": "tool",
                    "tool_call_id": tc["id"],
                    "content": llm_content,
                }
                history.append(tool_msg)
                llm_messages.append(tool_msg)

                # Paper retrieval already returns the text needed for summarization.
                # Prevent any follow-up tool calls (e.g., summarize_news) and force
                # the next assistant turn to summarize directly from this content.
                if name == "load_paper_html_text":
                    tools = []
                    force_no_more_tools = True
                    logger.info("Tool suppression enabled after load_paper_html_text")
            except Exception as exc:
                error_str = str(exc)
                logger.warning("Tool error ← %s: %s", name, error_str)
                if emit_tool_events:
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
                force_no_more_tools = True
                logger.debug("Tool error — tools suppressed for follow-up LLM call")

        if force_no_more_tools:
            tools = []

        # Loop back for next LLM call with tool results injected

    else:
        logger.warning("Tool call loop hit max iterations (%d)", _MAX_TOOL_ITERATIONS)

    # Append the generated star map using the Image: format so the highlight
    # renderer converts it to a clickable thumbnail (same as addToolImage)
    # in case the tool_image SSE event was missed.
    if pending_image_url:
        yield {"type": "token", "text": f"\nImage: {pending_image_url}"}

    yield {"type": "done"}
