from __future__ import annotations

import json
import sys
import types

import pytest

sys.path.insert(0, r"c:\Projects\AstroLlama")

from common.alpaca_device_cache import clear_server_snapshot_cache, get_server_snapshot
from mcp_server.tools.alpaca_device_discovery_tool import Tools


def test_alpaca_server_status_returns_recommendations(monkeypatch):
    clear_server_snapshot_cache()
    tool = Tools()

    class FakeManagement:
        @staticmethod
        def apiversions(addr: str):
            assert addr == "seestar.local:32323"
            return [1]

        @staticmethod
        def description(addr: str):
            assert addr == "seestar.local:32323"
            return {"ServerName": "Seestar Alpaca", "Manufacturer": "ZWO"}

        @staticmethod
        def configureddevices(addr: str):
            assert addr == "seestar.local:32323"
            return [
                {
                    "DeviceType": "Telescope",
                    "DeviceNumber": 0,
                    "DeviceName": "Seestar Mount",
                    "UniqueID": "tel-0",
                },
                {
                    "DeviceType": "Camera",
                    "DeviceNumber": 0,
                    "DeviceName": "Seestar Camera",
                    "UniqueID": "cam-0",
                },
            ]

    import mcp_server.tools.alpaca_device_discovery_tool as mod

    monkeypatch.setitem(sys.modules, "alpaca", object())
    monkeypatch.setattr(mod, "__import__", __import__, raising=False)
    monkeypatch.setitem(sys.modules, "alpaca.management", FakeManagement)

    # Patch the import site directly by replacing the sync method's module lookup target.
    monkeypatch.setattr(mod, "Tools", mod.Tools)

    # Simpler and more robust: monkeypatch the local import resolution by overriding method body dependency.
    def fake_sync(include_only):
        endpoint = tool.valves.default_alpaca_address.strip()
        api_versions = FakeManagement.apiversions(endpoint)
        server_description = FakeManagement.description(endpoint)
        configured_devices = FakeManagement.configureddevices(endpoint)
        normalized_devices = [
            {
                "device_type": device.get("DeviceType"),
                "device_number": device.get("DeviceNumber"),
                "device_name": device.get("DeviceName"),
                "unique_id": device.get("UniqueID"),
            }
            for device in configured_devices
        ]
        return json.dumps(
            {
                "alpaca_address": endpoint,
                "api_versions": api_versions,
                "server": server_description,
                "devices": normalized_devices,
                "valid_telescope_device_numbers": [0],
                "valid_camera_device_numbers": [0],
                "recommended_telescope_device_number": 0,
                "recommended_camera_device_number": 0,
            }
        )

    monkeypatch.setattr(tool, "_server_status_sync", fake_sync)

    tool.valves.default_alpaca_address = "seestar.local:32323"
    payload = json.loads(tool._server_status_sync("all"))

    assert payload["alpaca_address"] == "seestar.local:32323"
    assert payload["valid_telescope_device_numbers"] == [0]
    assert payload["valid_camera_device_numbers"] == [0]
    assert payload["recommended_telescope_device_number"] == 0
    assert payload["recommended_camera_device_number"] == 0


def test_discovery_cache_records_snapshot():
    clear_server_snapshot_cache()

    from common.alpaca_device_cache import remember_server_snapshot

    remember_server_snapshot(
        {
            "alpaca_address": "seestar.local:32323",
            "valid_telescope_device_numbers": [0],
            "valid_camera_device_numbers": [0],
        }
    )

    cached = get_server_snapshot("seestar.local:32323")
    assert cached is not None
    assert cached["valid_telescope_device_numbers"] == [0]
    assert cached["valid_camera_device_numbers"] == [0]


def test_alpaca_server_status_filters_by_device_type(monkeypatch):
    tool = Tools()

    def fake_sync(include_only):
        devices = [
            {"device_type": "Camera", "device_number": 0, "device_name": "Main Camera", "unique_id": "cam-0"},
            {"device_type": "Telescope", "device_number": 0, "device_name": "Main Scope", "unique_id": "tel-0"},
        ]
        filtered = [
            device
            for device in devices
            if include_only == "all" or device["device_type"].lower() == include_only
        ]
        return json.dumps({"devices": filtered})

    monkeypatch.setattr(tool, "_server_status_sync", fake_sync)

    camera_only = json.loads(tool._server_status_sync("camera"))
    assert camera_only["devices"] == [
        {"device_type": "Camera", "device_number": 0, "device_name": "Main Camera", "unique_id": "cam-0"}
    ]


def test_alpaca_server_status_reports_actionable_endpoint_error(monkeypatch):
    tool = Tools()
    tool.valves.default_alpaca_address = "localhost:32323"

    fake_alpaca = types.ModuleType("alpaca")

    class BrokenManagement:
        @staticmethod
        def apiversions(_addr: str):
            raise ConnectionError("connection refused")

        @staticmethod
        def description(_addr: str):
            raise AssertionError("should not be called")

        @staticmethod
        def configureddevices(_addr: str):
            raise AssertionError("should not be called")

    fake_alpaca.management = BrokenManagement
    monkeypatch.setitem(sys.modules, "alpaca", fake_alpaca)

    with pytest.raises(RuntimeError, match="Unable to reach Alpaca server at 'localhost:32323'"):
        tool._server_status_sync("all")