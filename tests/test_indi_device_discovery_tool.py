from __future__ import annotations

import json

from mcp_server.tools.indi_device_discovery_tool import Tools


def test_indi_server_status_filters_capabilities(monkeypatch):
    tool = Tools()
    tool.valves.default_indi_address = "spao-s30.local"

    def fake_discover(address: str, timeout_seconds: float):
        assert address == "spao-s30.local:7624"
        assert timeout_seconds == 5.0
        return {
            "indi_address": address,
            "devices": [
                {
                    "device_type": "camera",
                    "device_number": 0,
                    "device_name": "Main Cam",
                    "capabilities": ["camera"],
                },
                {
                    "device_type": "telescope",
                    "device_number": 1,
                    "device_name": "Main Mount",
                    "capabilities": ["telescope"],
                },
            ],
        }

    import mcp_server.tools.indi_device_discovery_tool as mod

    monkeypatch.setattr(mod, "discover_indi_devices", fake_discover)

    payload = json.loads(tool._server_status_sync("telescope"))
    assert payload["indi_address"] == "spao-s30.local:7624"
    assert payload["devices"] == [
        {
            "device_type": "telescope",
            "device_number": 1,
            "device_name": "Main Mount",
            "capabilities": ["telescope"],
        }
    ]
