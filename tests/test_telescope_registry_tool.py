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

    assert first["success"] is True
    assert second["success"] is True
    assert "Seestar" in first["message"]
    assert "SPAO" in second["message"]
    assert len(tool.valves.telescopes) == 2
    indi_profile = next(item for item in tool.valves.telescopes if item.platform == "indi")
    assert indi_profile.address == "spao-s30.local:7624"

    listing = await tool.list_registered_telescopes()
    assert "Seestar" in listing
    assert "SPAO" in listing
    assert "— active" in listing  # only the auto-selected Seestar profile

    indi_id = next(item.telescope_id for item in tool.valves.telescopes if item.platform == "indi")
    selected = json.loads(await tool.select_telescope(indi_id))
    assert selected["active_telescope"]["platform"] == "indi"

    listing_after_select = await tool.list_registered_telescopes()
    spao_line = next(line for line in listing_after_select.splitlines() if "SPAO" in line)
    assert "— active" in spao_line
    seestar_line = next(line for line in listing_after_select.splitlines() if "Seestar" in line)
    assert "— active" not in seestar_line


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
    """Registration should save the profile but clearly report failure when the
    Alpaca server cannot be reached."""
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

    assert result["success"] is False
    assert "FAILED" in result["message"]
    assert "seestar.local" in result["message"]
    assert result["active_telescope_id"] == result["telescope_id"]

    profile = tool.valves.telescopes[0]
    assert profile.address == "seestar.local"
    assert profile.platform == "alpaca"
    assert profile.inventory == {}
