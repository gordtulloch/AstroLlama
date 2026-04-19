from __future__ import annotations

import json
import logging
import uuid
from typing import Any

from fastapi import APIRouter, Depends, Request
from fastapi.responses import StreamingResponse

from app.models.chat import ChatRequest
from app.config import settings as app_settings
from app.services.auth import require_auth
from app.services.tool_orchestrator import run_chat

logger = logging.getLogger(__name__)

router = APIRouter()

# In-memory conversation history store: conversation_id → list of messages.
# A single-user local tool — no auth layer needed.
_histories: dict[str, list[dict[str, Any]]] = {}


def _format_sse(event: dict[str, Any]) -> str:
    return f"data: {json.dumps(event)}\n\n"


@router.post("/chat")
async def chat(
    request: Request,
    body: ChatRequest,
    _claims: dict[str, Any] | None = Depends(require_auth),
) -> StreamingResponse:
    """
    Accept a chat message and stream the response as text/event-stream.
    Uses fetch + ReadableStream on the client side (not EventSource)
    because the request carries a POST body.
    """
    llm_client = request.app.state.llm_client
    mcp_client = request.app.state.mcp_client

    # Resolve / create conversation history
    conv_id = body.conversation_id or str(uuid.uuid4())
    history = _histories.setdefault(conv_id, [])

    # Prepend system prompt from settings (only if no system msg already in history)
    system_prompt = body.settings.system_prompt or app_settings.default_system_prompt
    has_system = any(m.get("role") == "system" for m in history)
    if system_prompt and not has_system:
        history.insert(0, {"role": "system", "content": system_prompt})
        logger.info("System prompt applied (%d chars)", len(system_prompt))
    else:
        logger.info("System prompt skipped — already present or empty")

    # Append the new user message
    history.append({"role": "user", "content": body.message})

    async def event_stream():
        # Yield conversation_id first so the client can track it
        yield _format_sse({"type": "conversation_id", "conversation_id": conv_id})

        assistant_parts: list[str] = []

        async for event in run_chat(history, body.settings, llm_client, mcp_client, request.app.state.retriever):
            if event.get("type") == "token":
                assistant_parts.append(event["text"])
            yield _format_sse(event)

        # Persist the completed assistant message to history
        if assistant_parts:
            history.append({"role": "assistant", "content": "".join(assistant_parts)})

    return StreamingResponse(
        event_stream(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
            "Connection": "keep-alive",
        },
    )


@router.get("/status")
async def status(
    request: Request,
    _claims: dict[str, Any] | None = Depends(require_auth),
) -> dict[str, Any]:
    llm_client = request.app.state.llm_client
    mcp_client = request.app.state.mcp_client
    retriever = request.app.state.retriever

    llama_ok = await llm_client.is_healthy()
    mcp_ok = mcp_client.available

    return {
        "llama_server": "ok" if llama_ok else "unavailable",
        "mcp_server": "ok" if mcp_ok else "unavailable",
        "tools": mcp_client.tools,
        "rag": {
            "enabled": retriever.available,
            "documents": retriever.document_count,
        },
    }


@router.get("/auth/config")
async def auth_config() -> dict[str, Any]:
    return {
        "enabled": app_settings.entra_auth_enabled,
        "tenant_id": app_settings.entra_tenant_id,
        "spa_client_id": app_settings.entra_spa_client_id,
        "api_scope": app_settings.entra_api_scope,
        "redirect_uri": app_settings.entra_redirect_uri,
    }
