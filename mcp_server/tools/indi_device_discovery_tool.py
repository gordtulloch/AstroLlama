from __future__ import annotations

import asyncio
import json
from typing import Literal

from pydantic import BaseModel, Field

from common.indi_inventory import discover_indi_devices, normalize_indi_address


class Tools:
    """OpenWebUI-compatible INDI discovery and status tool."""

    class Valves(BaseModel):
        default_indi_address: str = Field(
            default="localhost:7624",
            description="Default INDI host:port endpoint for discovery/status.",
        )
        discovery_timeout_seconds: float = Field(
            default=5.0,
            ge=0.5,
            le=30.0,
            description="Timeout for INDI device discovery.",
        )

    def __init__(self, valves: "Tools.Valves | None" = None) -> None:
        self.valves = valves or self.Valves()

    async def indi_server_status(
        self,
        include_only: Literal["all", "camera", "telescope", "focuser", "filterwheel"] = "all",
    ) -> str:
        """List devices exposed by an INDI server and classify capabilities.

        Use this before INDI telescope/camera operations to inspect available devices.

        :param include_only: Filter to one capability class or include all devices.
        :returns: Structured INDI server/device inventory.
        """
        return await asyncio.to_thread(self._server_status_sync, include_only)

    def _server_status_sync(
        self,
        include_only: Literal["all", "camera", "telescope", "focuser", "filterwheel"],
    ) -> str:
        endpoint = normalize_indi_address(self.valves.default_indi_address.strip())
        payload = discover_indi_devices(endpoint, timeout_seconds=float(self.valves.discovery_timeout_seconds))

        if include_only != "all":
            devices = payload.get("devices", [])
            filtered = [
                device
                for device in devices
                if include_only in list(device.get("capabilities") or [])
            ]
            payload = dict(payload)
            payload["devices"] = filtered

        return json.dumps(payload, indent=2)
