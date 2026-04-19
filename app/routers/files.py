from __future__ import annotations

import logging
from pathlib import Path

from fastapi import APIRouter, HTTPException
from fastapi.responses import FileResponse

logger = logging.getLogger(__name__)

# Resolved relative to this file: app/routers/ -> app/ -> repo root
_DOWNLOADS_DIR = Path(__file__).resolve().parent.parent.parent / "data" / "downloads"

router = APIRouter()


@router.get("/files/{filename}")
async def download_file(filename: str) -> FileResponse:
    """Serve a previously saved large tool-result file."""
    # Reject any path traversal attempts
    safe_name = Path(filename).name
    if safe_name != filename or not safe_name:
        raise HTTPException(status_code=400, detail="Invalid filename")

    file_path = _DOWNLOADS_DIR / safe_name
    if not file_path.is_file():
        raise HTTPException(status_code=404, detail="File not found")

    return FileResponse(
        path=str(file_path),
        filename=safe_name,
        media_type="application/octet-stream",
    )
