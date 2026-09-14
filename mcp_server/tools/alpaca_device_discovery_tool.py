from __future__ import annotations

import asyncio
import json
from typing import Literal

from pydantic import BaseModel, Field

from common.alpaca_device_cache import remember_server_snapshot


class Tools:
    """OpenWebUI-compatible Alpaca discovery and status tool."""

    class Valves(BaseModel):
        """Global configuration for Alpaca server discovery behavior."""

        default_alpaca_address: str = Field(
            default="localhost:32323",
            description="Default Alpaca host:port endpoint for discovery/status.",
        )

    def __init__(self, valves: "Tools.Valves | None" = None) -> None:
        self.valves = valves or self.Valves()

    async def alpaca_server_status(
        self,
        include_only: Literal["all", "camera", "telescope"] = "all",
    ) -> str:
        """List devices exposed by an Alpaca server and identify valid camera/telescope instances.

        Use this before any control or capture operation to inspect the configured Alpaca server.
        This tool always uses the configured default Alpaca address and does not require user input.

        :param include_only: Filter results to all devices, only cameras, or only telescopes.
        :returns: Structured server/device inventory including valid instance numbers.
        """
        return await asyncio.to_thread(self._server_status_sync, include_only)

    def _server_status_sync(
        self,
        include_only: Literal["all", "camera", "telescope"],
    ) -> str:
        endpoint = self.valves.default_alpaca_address.strip()
        if not endpoint:
            raise ValueError("alpaca_address cannot be empty")

        try:
            from alpaca import management
        except Exception as exc:
            raise RuntimeError(
                "Alpaca client library is unavailable. Install it with: pip install alpyca"
            ) from exc

        try:
            api_versions = management.apiversions(endpoint)
            server_description = management.description(endpoint)
            configured_devices = management.configureddevices(endpoint)
        except Exception as exc:
            raise RuntimeError(
                "Unable to reach Alpaca server at "
                f"'{endpoint}'. Update valve 'default_alpaca_address' for tool "
                "'alpaca_device_discovery_tool' to your actual Alpaca host:port "
                "(for example 'seestar.local:32323')."
            ) from exc

        normalized_devices: list[dict[str, object]] = []
        for device in configured_devices:
            dtype = str(device.get("DeviceType", "")).lower()
            if include_only != "all" and dtype != include_only:
                continue
            normalized_devices.append(
                {
                    "device_type": device.get("DeviceType"),
                    "device_number": device.get("DeviceNumber"),
                    "device_name": device.get("DeviceName"),
                    "unique_id": device.get("UniqueID"),
                }
            )

        telescope_numbers = [
            int(device["device_number"])
            for device in normalized_devices
            if str(device.get("device_type", "")).lower() == "telescope"
        ]
        camera_numbers = [
            int(device["device_number"])
            for device in normalized_devices
            if str(device.get("device_type", "")).lower() == "camera"
        ]

        payload = {
            "alpaca_address": endpoint,
            "api_versions": api_versions,
            "server": server_description,
            "devices": normalized_devices,
            "valid_telescope_device_numbers": telescope_numbers,
            "valid_camera_device_numbers": camera_numbers,
            "recommended_telescope_device_number": telescope_numbers[0] if len(telescope_numbers) == 1 else None,
            "recommended_camera_device_number": camera_numbers[0] if len(camera_numbers) == 1 else None,
        }
        remember_server_snapshot(payload)
        return json.dumps(payload, indent=2)