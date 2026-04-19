from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Any, Optional

from pydantic import BaseModel, Field


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


class ConversationSettings(BaseModel):
    system_prompt: str = ""
    temperature: float = 0.7
    top_p: float = 0.9
    max_tokens: int = 1024


class Message(BaseModel):
    role: str
    content: Optional[str] = None
    tool_calls: Optional[list[dict[str, Any]]] = None
    tool_call_id: Optional[str] = None


class Conversation(BaseModel):
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    name: str = "Untitled"
    created_at: str = Field(default_factory=_now_iso)
    updated_at: str = Field(default_factory=_now_iso)
    settings: ConversationSettings = ConversationSettings()
    messages: list[Message] = []
