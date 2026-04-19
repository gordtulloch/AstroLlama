from __future__ import annotations

from typing import Optional

from pydantic import BaseModel, Field

from app.config import settings as _app_settings


class ChatSettings(BaseModel):
    system_prompt: Optional[str] = None
    temperature: float = Field(default=0.7, ge=0.0, le=2.0)
    top_p: float = Field(default=0.9, ge=0.0, le=1.0)
    max_tokens: int = Field(default=1024, ge=1, le=32768)
    context_size: int = Field(default_factory=lambda: _app_settings.default_context_size, ge=512)


class ChatRequest(BaseModel):
    conversation_id: Optional[str] = None
    message: str = Field(min_length=1, max_length=32768)
    settings: ChatSettings = ChatSettings()
