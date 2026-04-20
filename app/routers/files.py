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
    logger.info("files: request for %r → resolved path %s (exists=%s)", safe_name, file_path, file_path.is_file())
    if not file_path.is_file():
        raise HTTPException(status_code=404, detail="File not found")

    suffix = file_path.suffix.lower()
    media_type = {
        ".png": "image/png",
        ".jpg": "image/jpeg",
        ".jpeg": "image/jpeg",
        ".gif": "image/gif",
        ".webp": "image/webp",
    }.get(suffix, "application/octet-stream")

    # Only set Content-Disposition: attachment for non-image files so that
    # images load inline when referenced from an <img> src.
    kwargs = {} if suffix in {".png", ".jpg", ".jpeg", ".gif", ".webp"} else {"filename": safe_name}

    return FileResponse(
        path=str(file_path),
        media_type=media_type,
        **kwargs,
    )
