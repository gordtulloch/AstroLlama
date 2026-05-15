from __future__ import annotations

import json
import sqlite3
import time
from pathlib import Path
from typing import Any


def default_db_path() -> Path:
    repo_root = Path(__file__).resolve().parent.parent
    return repo_root / "data" / "tool_valves.sqlite3"


class ValvesStore:
    """Simple SQLite store for tool-level valves overrides."""

    def __init__(self, db_path: Path | str | None = None) -> None:
        self._db_path = Path(db_path) if db_path else default_db_path()
        self._db_path.parent.mkdir(parents=True, exist_ok=True)
        self._ensure_schema()

    @property
    def db_path(self) -> Path:
        return self._db_path

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self._db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def _ensure_schema(self) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS tool_valves (
                    tool_name TEXT PRIMARY KEY,
                    valves_json TEXT NOT NULL,
                    updated_epoch_ns INTEGER NOT NULL
                )
                """
            )
            conn.commit()

    def get(self, tool_name: str) -> dict[str, Any]:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT valves_json FROM tool_valves WHERE tool_name = ?",
                (tool_name,),
            ).fetchone()

        if row is None:
            return {}

        try:
            data = json.loads(row["valves_json"])
        except json.JSONDecodeError:
            return {}

        return data if isinstance(data, dict) else {}

    def list_all(self) -> dict[str, dict[str, Any]]:
        with self._connect() as conn:
            rows = conn.execute("SELECT tool_name, valves_json FROM tool_valves").fetchall()

        results: dict[str, dict[str, Any]] = {}
        for row in rows:
            tool_name = str(row["tool_name"])
            try:
                parsed = json.loads(row["valves_json"])
            except json.JSONDecodeError:
                continue
            if isinstance(parsed, dict):
                results[tool_name] = parsed
        return results

    def set(self, tool_name: str, valves: dict[str, Any]) -> None:
        payload = json.dumps(valves, ensure_ascii=True)
        now_ns = time.time_ns()
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO tool_valves (tool_name, valves_json, updated_epoch_ns)
                VALUES (?, ?, ?)
                ON CONFLICT(tool_name)
                DO UPDATE SET
                    valves_json = excluded.valves_json,
                    updated_epoch_ns = excluded.updated_epoch_ns
                """,
                (tool_name, payload, now_ns),
            )
            conn.commit()

    def delete(self, tool_name: str) -> None:
        with self._connect() as conn:
            conn.execute("DELETE FROM tool_valves WHERE tool_name = ?", (tool_name,))
            conn.commit()

    def fingerprint(self) -> int:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT COALESCE(MAX(updated_epoch_ns), 0) AS fp FROM tool_valves"
            ).fetchone()
        return int(row["fp"]) if row is not None else 0
