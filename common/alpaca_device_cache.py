from __future__ import annotations

import threading
import time
from typing import Any


_LOCK = threading.Lock()
_CACHE: dict[str, dict[str, Any]] = {}
_DEFAULT_MAX_AGE_SECONDS = 900.0


def remember_server_snapshot(snapshot: dict[str, Any]) -> None:
    endpoint = str(snapshot.get("alpaca_address", "")).strip()
    if not endpoint:
        return

    row = dict(snapshot)
    row["cached_epoch"] = time.time()
    with _LOCK:
        _CACHE[endpoint] = row


def get_server_snapshot(endpoint: str, max_age_seconds: float = _DEFAULT_MAX_AGE_SECONDS) -> dict[str, Any] | None:
    key = str(endpoint or "").strip()
    if not key:
        return None

    with _LOCK:
        snapshot = _CACHE.get(key)

    if snapshot is None:
        return None

    cached_epoch = float(snapshot.get("cached_epoch", 0.0) or 0.0)
    if time.time() - cached_epoch > float(max_age_seconds):
        with _LOCK:
            _CACHE.pop(key, None)
        return None

    return dict(snapshot)


def clear_server_snapshot_cache() -> None:
    with _LOCK:
        _CACHE.clear()