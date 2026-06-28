from __future__ import annotations

import json

import pytest

from common.valves_store import ValvesStore
from mcp_server.tools.telescope_interface_tool import Tools


@pytest.mark.asyncio
async def test_telescope_server_status_routes_to_active_indi(tmp_path, monkeypatch):
    store = ValvesStore(tmp_path / "tool_valves.sqlite3")
    store.set(
        "telescope_registry_tool",
        {
            "active_telescope_id": "indi1",
            "telescopes": [
                {
                    "telescope_id": "indi1",
                    "name": "INDI Scope",
                    "platform": "indi",
                    "address": "spao-s30.local:7624",
                    "protocol": "http",
                    "inventory": {},
                    "registered_epoch": 0.0,
                    "last_seen_epoch": 0.0,
                }
            ],
        },
    )

    tool = Tools(store=store)

    async def fake_indi(include_only):
        assert include_only == "telescope"
        return json.dumps({"platform": "indi", "ok": True})

    monkeypatch.setattr(tool, "_indi_server_status", fake_indi)

    payload = json.loads(await tool.telescope_server_status(include_only="telescope"))
    assert payload["platform"] == "indi"
    assert payload["ok"] is True


@pytest.mark.asyncio
async def test_telescope_server_status_routes_to_active_alpaca(tmp_path, monkeypatch):
    store = ValvesStore(tmp_path / "tool_valves.sqlite3")
    store.set(
        "telescope_registry_tool",
        {
            "active_telescope_id": "alp1",
            "telescopes": [
                {
                    "telescope_id": "alp1",
                    "name": "Alpaca Scope",
                    "platform": "alpaca",
                    "address": "seestar.local:32323",
                    "protocol": "http",
                    "inventory": {},
                    "registered_epoch": 0.0,
                    "last_seen_epoch": 0.0,
                }
            ],
        },
    )

    tool = Tools(store=store)

    async def fake_alpaca(include_only):
        assert include_only == "camera"
        return json.dumps({"platform": "alpaca", "ok": True})

    monkeypatch.setattr(tool, "_alpaca_server_status", fake_alpaca)

    payload = json.loads(await tool.telescope_server_status(include_only="camera"))
    assert payload["platform"] == "alpaca"
    assert payload["ok"] is True


@pytest.mark.asyncio
async def test_telescope_server_status_requires_active_profile(tmp_path):
    store = ValvesStore(tmp_path / "tool_valves.sqlite3")
    tool = Tools(store=store)

    with pytest.raises(ValueError, match="No active telescope selected"):
        await tool.telescope_server_status()
