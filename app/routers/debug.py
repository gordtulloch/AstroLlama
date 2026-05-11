from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from fastapi import APIRouter, Depends
from pydantic import BaseModel, Field

from app.services.auth import require_auth

logger = logging.getLogger(__name__)

_LOG_PATH = Path(__file__).resolve().parent.parent.parent / "logs" / "dev" / "client_debug.log"

router = APIRouter()


class SpeechLogEntry(BaseModel):
    event: str = Field(min_length=1, max_length=64)
    text: str = Field(default="", max_length=4096)
    final: bool | None = None
    listening: bool | None = None
    awaiting_wake: bool | None = None
    auth_mode: str | None = Field(default=None, max_length=32)
    conversation_id: str | None = Field(default=None, max_length=128)
    extra: dict[str, Any] | None = None


@router.post("/debug/speech-log")
async def write_speech_log(
    body: SpeechLogEntry,
    _claims: dict[str, Any] | None = Depends(require_auth),
) -> dict[str, bool]:
    _LOG_PATH.parent.mkdir(parents=True, exist_ok=True)

    record = {
        "ts": datetime.now(timezone.utc).isoformat(),
        **body.model_dump(),
    }
    with _LOG_PATH.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(record, ensure_ascii=False) + "\n")

    logger.debug("speech debug log appended: %s", body.event)
    return {"ok": True}