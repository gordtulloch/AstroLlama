from __future__ import annotations

import json

import pytest

from common.valves_store import ValvesStore
from mcp_server.tools.telescope_registry_tool import Tools


@pytest.mark.asyncio
async def test_register_multiple_telescopes_and_select(tmp_path, monkeypatch):
    store = ValvesStore(tmp_path / "tool_valves.sqlite3")
    tool = Tools(store=store)

    def fake_discover(platform: str, address: str):
        if platform == "alpaca":
            return {
                "platform": "alpaca",
                "alpaca_address": address,
                "devices": [
                    {"device_type": "Telescope", "device_number": 1},
                    {"device_type": "Camera", "device_number": 2},
                ],
                "recommended_telescope_device_number": 1,
                "recommended_camera_device_number": 2,
            }
        return {
            "platform": "indi",
            "indi_address": "spao-s30.local:7624",
            "devices": [{"device_type": "telescope", "device_number": 0, "capabilities": ["telescope"]}],
            "recommended_telescope_device_number": 0,
            "recommended_camera_device_number": None,
        }

    monkeypatch.setattr(tool, "_discover_inventory", fake_discover)

    first = json.loads(
        tool._register_telescope_sync(
            platform="alpaca",
            address="seestar.local:32323",
            name="Seestar",
            protocol="http",
            auto_select=True,
        )
    )
    second = json.loads(
        tool._register_telescope_sync(
            platform="indi",
            address="spao-s30.local",
            name="SPAO",
            protocol="http",
            auto_select=False,
        )
    )

    assert first["status"] == "registered"
    assert second["status"] == "registered"
    assert len(tool.valves.telescopes) == 2
    assert second["telescope"]["address"] == "spao-s30.local:7624"

    profiles = json.loads(await tool.list_registered_telescopes())
    assert len(profiles["telescopes"]) == 2

    indi_id = next(item.telescope_id for item in tool.valves.telescopes if item.platform == "indi")
    selected = json.loads(await tool.select_telescope(indi_id))
    assert selected["active_telescope"]["platform"] == "indi"


@pytest.mark.asyncio
async def test_register_alpaca_syncs_default_valves(tmp_path, monkeypatch):
    store = ValvesStore(tmp_path / "tool_valves.sqlite3")
    tool = Tools(store=store)

    monkeypatch.setattr(
        tool,
        "_discover_inventory",
        lambda platform, address: {
            "platform": platform,
            "alpaca_address": address,
            "devices": [],
            "recommended_telescope_device_number": 3,
            "recommended_camera_device_number": 4,
        },
    )

    _ = tool._register_telescope_sync(
        platform="alpaca",
        address="scope.local:32323",
        name="Primary",
        protocol="https",
        auto_select=True,
    )

    discovery_valves = store.get("alpaca_device_discovery_tool")
    imaging_valves = store.get("alpaca_telescope_imaging_tool")

    assert discovery_valves["default_alpaca_address"] == "scope.local:32323"
    assert imaging_valves["default_alpaca_address"] == "scope.local:32323"
    assert imaging_valves["default_protocol"] == "https"
    assert imaging_valves["default_telescope_device_number"] == 3
    assert imaging_valves["default_camera_device_number"] == 4


@pytest.mark.asyncio
async def test_register_alpaca_unreachable_server(tmp_path, monkeypatch):
    """Registration should succeed even when the Alpaca server cannot be reached."""
    store = ValvesStore(tmp_path / "tool_valves.sqlite3")
    tool = Tools(store=store)

    def raise_unreachable(platform: str, address: str):
        raise RuntimeError(
            f"Unable to reach Alpaca server at '{address}'. Verify host:port and server availability."
        )

    monkeypatch.setattr(tool, "_discover_inventory", raise_unreachable)

    result = json.loads(
        tool._register_telescope_sync(
            platform="alpaca",
            address="seestar.local",
            name=None,
            protocol="http",
            auto_select=True,
        )
    )

    assert result["status"] == "registered"
    assert result["telescope"]["address"] == "seestar.local"
    assert result["telescope"]["platform"] == "alpaca"
    assert result["active_telescope_id"] == result["telescope"]["telescope_id"]
    assert "inventory_warning" in result
    assert "seestar.local" in result["inventory_warning"]
    assert result["telescope"]["inventory"] == {}
