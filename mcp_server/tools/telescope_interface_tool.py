from __future__ import annotations

import asyncio
import json
import time
from typing import Any, Literal

from pydantic import BaseModel, Field

from common.indi_inventory import discover_indi_devices, parse_indi_address
from common.valves_store import ValvesStore
from mcp_server.tools.alpaca_device_discovery_tool import Tools as AlpacaDiscoveryTools
from mcp_server.tools.alpaca_telescope_imaging_tool import Tools as AlpacaImagingTools
from mcp_server.tools.indi_device_discovery_tool import Tools as IndiDiscoveryTools


class Tools:
    """Platform-agnostic telescope interface that routes to active Alpaca or INDI profile."""

    class Valves(BaseModel):
        indi_command_timeout_seconds: float = Field(
            default=8.0,
            ge=1.0,
            le=60.0,
            description="Timeout for INDI control command completion.",
        )

    def __init__(self, valves: "Tools.Valves | None" = None, store: ValvesStore | None = None) -> None:
        self.valves = valves or self.Valves()
        self._store = store or ValvesStore()

    async def telescope_server_status(
        self,
        include_only: Literal["all", "camera", "telescope", "focuser", "filterwheel"] = "all",
    ) -> str:
        """Get server inventory for the active telescope platform.

        :param include_only: Optional device-type filter.
        :returns: Inventory payload from Alpaca or INDI.
        """
        profile = self._require_active_profile()
        if profile["platform"] == "alpaca":
            alpaca_include = include_only if include_only in {"all", "camera", "telescope"} else "all"
            return await self._alpaca_server_status(alpaca_include)
        return await self._indi_server_status(include_only)

    async def telescope_park(self) -> str:
        """Park the active telescope for either Alpaca or INDI platform."""
        profile = self._require_active_profile()
        if profile["platform"] == "alpaca":
            return await self._alpaca_park()
        return await asyncio.to_thread(self._indi_set_park_state, profile["address"], True)

    async def telescope_unpark(self) -> str:
        """Unpark the active telescope for either Alpaca or INDI platform."""
        profile = self._require_active_profile()
        if profile["platform"] == "alpaca":
            return await self._alpaca_unpark()
        return await asyncio.to_thread(self._indi_set_park_state, profile["address"], False)

    async def telescope_camera_diagnostics(self) -> str:
        """Return camera diagnostics for active platform."""
        profile = self._require_active_profile()
        if profile["platform"] == "alpaca":
            return await self._alpaca_camera_diagnostics()
        return await asyncio.to_thread(self._indi_camera_diagnostics_sync, profile["address"])

    def _require_active_profile(self) -> dict[str, Any]:
        registry = self._store.get("telescope_registry_tool")
        active_id = str(registry.get("active_telescope_id") or "").strip()
        telescopes = registry.get("telescopes") or []
        if not active_id:
            raise ValueError("No active telescope selected. Register/select a telescope first.")
        for item in telescopes:
            if str(item.get("telescope_id")) == active_id:
                return {
                    "telescope_id": active_id,
                    "platform": str(item.get("platform") or "").lower(),
                    "address": str(item.get("address") or "").strip(),
                }
        raise ValueError("Active telescope was not found in registry. Re-register the telescope.")

    async def _alpaca_server_status(self, include_only: Literal["all", "camera", "telescope"]) -> str:
        tool = AlpacaDiscoveryTools()
        vals = self._store.get("alpaca_device_discovery_tool")
        if vals:
            merged = tool.valves.model_dump()
            merged.update(vals)
            tool.valves = tool.valves.__class__.model_validate(merged)
        return await tool.alpaca_server_status(include_only=include_only)

    async def _indi_server_status(
        self,
        include_only: Literal["all", "camera", "telescope", "focuser", "filterwheel"],
    ) -> str:
        tool = IndiDiscoveryTools()
        vals = self._store.get("indi_device_discovery_tool")
        if vals:
            merged = tool.valves.model_dump()
            merged.update(vals)
            tool.valves = tool.valves.__class__.model_validate(merged)
        return await tool.indi_server_status(include_only=include_only)

    async def _alpaca_park(self) -> str:
        tool = AlpacaImagingTools()
        vals = self._store.get("alpaca_telescope_imaging_tool")
        if vals:
            merged = tool.valves.model_dump()
            merged.update(vals)
            tool.valves = tool.valves.__class__.model_validate(merged)
        return await tool.alpaca_park_telescope()

    async def _alpaca_unpark(self) -> str:
        tool = AlpacaImagingTools()
        vals = self._store.get("alpaca_telescope_imaging_tool")
        if vals:
            merged = tool.valves.model_dump()
            merged.update(vals)
            tool.valves = tool.valves.__class__.model_validate(merged)
        return await tool.alpaca_unpark_telescope()

    async def _alpaca_camera_diagnostics(self) -> str:
        tool = AlpacaImagingTools()
        vals = self._store.get("alpaca_telescope_imaging_tool")
        if vals:
            merged = tool.valves.model_dump()
            merged.update(vals)
            tool.valves = tool.valves.__class__.model_validate(merged)
        return await tool.alpaca_camera_diagnostics()

    def _indi_camera_diagnostics_sync(self, address: str) -> str:
        payload = discover_indi_devices(address, timeout_seconds=float(self.valves.indi_command_timeout_seconds))
        camera_devices = [
            device
            for device in list(payload.get("devices") or [])
            if "camera" in list(device.get("capabilities") or [])
        ]
        return json.dumps(
            {
                "status": "ok",
                "platform": "indi",
                "endpoint": payload.get("indi_address"),
                "camera_count": len(camera_devices),
                "cameras": camera_devices,
            },
            indent=2,
        )

    def _indi_set_park_state(self, address: str, park: bool) -> str:
        host, port = parse_indi_address(address)
        try:
            import PyIndi  # type: ignore
        except Exception as exc:
            raise RuntimeError(
                "PyIndi client library is unavailable. Install it with: pip install git+https://github.com/indilib/pyindi-client.git"
            ) from exc

        class _Client(PyIndi.BaseClient):  # type: ignore[misc]
            def newDevice(self, _d):
                return None

            def removeDevice(self, _d):
                return None

            def newProperty(self, _p):
                return None

            def removeProperty(self, _p):
                return None

            def newBLOB(self, _bp):
                return None

            def newSwitch(self, _svp):
                return None

            def newNumber(self, _nvp):
                return None

            def newText(self, _tvp):
                return None

            def newLight(self, _lvp):
                return None

            def newMessage(self, _d, _m):
                return None

            def serverConnected(self):
                return None

            def serverDisconnected(self, _code):
                return None

        inventory = discover_indi_devices(f"{host}:{port}", timeout_seconds=float(self.valves.indi_command_timeout_seconds))
        telescope_indexes = list(inventory.get("valid_telescope_device_numbers") or [])
        if not telescope_indexes:
            raise RuntimeError("No INDI telescope devices were discovered on that server.")
        telescope_idx = int(telescope_indexes[0])

        client = _Client()
        client.setServer(host, int(port))
        if not client.connectServer():
            raise RuntimeError(f"Unable to connect to INDI server at '{host}:{port}'.")

        deadline = time.monotonic() + float(self.valves.indi_command_timeout_seconds)
        try:
            device = None
            while time.monotonic() < deadline:
                devices = list(client.getDevices() or [])
                if len(devices) > telescope_idx:
                    device = devices[telescope_idx]
                    break
                time.sleep(0.2)
            if device is None:
                raise RuntimeError("Could not resolve INDI telescope device instance.")

            connection = device.getSwitch("CONNECTION")
            if connection is not None:
                for sw in connection:
                    sw.s = getattr(PyIndi, "ISS_OFF", 0)
                for sw in connection:
                    if str(getattr(sw, "name", "")).upper() == "CONNECT":
                        sw.s = getattr(PyIndi, "ISS_ON", 1)
                        break
                client.sendNewSwitch(connection)

            park_switch = device.getSwitch("TELESCOPE_PARK")
            if park_switch is None:
                raise RuntimeError("INDI telescope does not expose TELESCOPE_PARK control.")

            for sw in park_switch:
                sw.s = getattr(PyIndi, "ISS_OFF", 0)
            desired = "PARK" if park else "UNPARK"
            selected = False
            for sw in park_switch:
                if str(getattr(sw, "name", "")).upper().endswith(desired):
                    sw.s = getattr(PyIndi, "ISS_ON", 1)
                    selected = True
                    break

            if not selected:
                raise RuntimeError(f"INDI telescope park switch '{desired}' is not available.")

            client.sendNewSwitch(park_switch)
            return json.dumps(
                {
                    "status": "ok",
                    "platform": "indi",
                    "endpoint": f"{host}:{port}",
                    "action": "park" if park else "unpark",
                    "message": f"Sent INDI telescope {desired.lower()} command.",
                }
            )
        finally:
            try:
                client.disconnectServer()
            except Exception:
                pass
