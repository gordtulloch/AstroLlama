from __future__ import annotations

import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import anyio
from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel

from app.models.conversation import Conversation
from app.services.auth import require_auth

router = APIRouter()

_UUID_RE = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"
)


def _conv_dir(request: Request) -> Path:
    return request.app.state.conv_dir


def _safe_path(conv_dir: Path, conv_id: str) -> Path:
    """Validate UUID format to prevent path traversal, then return the file path."""
    if not _UUID_RE.match(conv_id):
        raise HTTPException(status_code=400, detail="Invalid conversation ID format")
    return conv_dir / f"{conv_id}.json"


async def _read_json(path: Path) -> dict[str, Any]:
    def _read():
        return path.read_text(encoding="utf-8")

    raw = await anyio.to_thread.run_sync(_read)
    import json
    return json.loads(raw)


async def _write_json(path: Path, data: dict[str, Any]) -> None:
    import json

    def _write():
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")

    await anyio.to_thread.run_sync(_write)


# ---------------------------------------------------------------------------


@router.get("/conversations")
async def list_conversations(
    request: Request,
    _claims: dict[str, Any] | None = Depends(require_auth),
) -> list[dict[str, Any]]:
    conv_dir = _conv_dir(request)

    def _list():
        if not conv_dir.exists():
            return []
        results = []
        for p in conv_dir.glob("*.json"):
            try:
                import json
                data = json.loads(p.read_text(encoding="utf-8"))
                results.append(
                    {
                        "id": data.get("id"),
                        "name": data.get("name", "Untitled"),
                        "created_at": data.get("created_at"),
                        "updated_at": data.get("updated_at"),
                    }
                )
            except Exception:
                pass
        results.sort(key=lambda x: x.get("updated_at") or "", reverse=True)
        return results

    return await anyio.to_thread.run_sync(_list)


class SaveRequest(BaseModel):
    conversation_id: str
    name: str = "Untitled"
    messages: list[dict[str, Any]]
    settings: dict[str, Any] = {}


@router.post("/conversations", status_code=201)
async def save_conversation(
    request: Request,
    body: SaveRequest,
    _claims: dict[str, Any] | None = Depends(require_auth),
) -> dict[str, Any]:
    conv_dir = _conv_dir(request)
    path = _safe_path(conv_dir, body.conversation_id)

    now = datetime.now(timezone.utc).isoformat()

    # Load existing to preserve created_at
    created_at = now
    if path.exists():
        try:
            existing = await _read_json(path)
            created_at = existing.get("created_at", now)
        except Exception:
            pass

    conv = {
        "id": body.conversation_id,
        "name": body.name,
        "created_at": created_at,
        "updated_at": now,
        "settings": body.settings,
        "messages": body.messages,
    }
    await _write_json(path, conv)
    return {"id": body.conversation_id, "name": body.name}


@router.get("/conversations/{conv_id}")
async def load_conversation(
    request: Request,
    conv_id: str,
    _claims: dict[str, Any] | None = Depends(require_auth),
) -> dict[str, Any]:
    conv_dir = _conv_dir(request)
    path = _safe_path(conv_dir, conv_id)
    if not path.exists():
        raise HTTPException(status_code=404, detail="Conversation not found")
    return await _read_json(path)


@router.delete("/conversations/{conv_id}", status_code=204)
async def delete_conversation(
    request: Request,
    conv_id: str,
    _claims: dict[str, Any] | None = Depends(require_auth),
) -> None:
    conv_dir = _conv_dir(request)
    path = _safe_path(conv_dir, conv_id)
    if not path.exists():
        raise HTTPException(status_code=404, detail="Conversation not found")

    def _delete():
        path.unlink(missing_ok=True)

    await anyio.to_thread.run_sync(_delete)
