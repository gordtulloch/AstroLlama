from __future__ import annotations

import asyncio
import json
import time
import uuid
from typing import Any, Literal

from pydantic import BaseModel, Field

from common.indi_inventory import discover_indi_devices, normalize_indi_address
from common.valves_store import ValvesStore


class TelescopeProfile(BaseModel):
    telescope_id: str
    name: str
    platform: Literal["alpaca", "indi"]
    address: str
    protocol: Literal["http", "https"] = "http"
    inventory: dict[str, Any] = Field(default_factory=dict)
    registered_epoch: float
    last_seen_epoch: float


class Tools:
    """Registry for multiple Alpaca/INDI telescope profiles."""

    class Valves(BaseModel):
        active_telescope_id: str | None = Field(
            default=None,
            description="Currently selected telescope profile id.",
        )
        telescopes: list[TelescopeProfile] = Field(
            default_factory=list,
            description="Registered telescope profiles.",
        )
        indi_discovery_timeout_seconds: float = Field(
            default=5.0,
            ge=0.5,
            le=30.0,
            description="Timeout for INDI discovery attempts.",
        )

    def __init__(self, valves: "Tools.Valves | None" = None, store: ValvesStore | None = None) -> None:
        self.valves = valves or self.Valves()
        self._store = store or ValvesStore()

    async def register_telescope(
        self,
        platform: Literal["alpaca", "indi"],
        address: str,
        name: str | None = None,
        protocol: Literal["http", "https"] = "http",
        auto_select: bool = True,
    ) -> str:
        """Register a telescope profile and inventory devices from that server.

        Use this when a user asks to register an Alpaca or INDI telescope endpoint.

        :param platform: Telescope platform type: alpaca or indi.
        :param address: Server address in host:port form.
        :param name: Optional display name.
        :param protocol: Protocol for Alpaca device connections.
        :param auto_select: If true, make this profile the active telescope.
        :returns: Registered profile and discovered device inventory.
        """
        return await asyncio.to_thread(
            self._register_telescope_sync,
            platform,
            address,
            name,
            protocol,
            auto_select,
        )

    async def list_registered_telescopes(self) -> str:
        """List all telescope profiles currently registered.

        :returns: Registered telescope profiles and active selection.
        """
        rows = [item.model_dump() for item in self.valves.telescopes]
        payload = {
            "active_telescope_id": self.valves.active_telescope_id,
            "telescopes": rows,
        }
        return json.dumps(payload, indent=2)

    async def select_telescope(self, telescope_id: str) -> str:
        """Select an existing telescope profile as the active one.

        :param telescope_id: Registered profile identifier.
        :returns: Active profile details.
        """
        key = str(telescope_id or "").strip()
        if not key:
            raise ValueError("telescope_id is required")

        profile = self._find_profile(key)
        if profile is None:
            raise ValueError(f"Unknown telescope_id: {key}")

        self.valves.active_telescope_id = key
        self._persist_valves()
        self._sync_platform_valves(profile)

        return json.dumps(
            {
                "active_telescope_id": key,
                "active_telescope": profile.model_dump(),
            },
            indent=2,
        )

    async def telescope_inventory(self, telescope_id: str | None = None, refresh: bool = False) -> str:
        """Return inventory for a registered telescope profile.

        :param telescope_id: Optional profile id. Uses active telescope if omitted.
        :param refresh: If true, probe server again before returning inventory.
        :returns: Telescope profile and inventory payload.
        """
        key = str(telescope_id or self.valves.active_telescope_id or "").strip()
        if not key:
            raise ValueError("No active telescope selected. Register and select a telescope first.")

        profile = self._find_profile(key)
        if profile is None:
            raise ValueError(f"Unknown telescope_id: {key}")

        if refresh:
            return await asyncio.to_thread(self._refresh_inventory_sync, key)

        return json.dumps(
            {
                "active_telescope_id": self.valves.active_telescope_id,
                "telescope": profile.model_dump(),
            },
            indent=2,
        )

    def _register_telescope_sync(
        self,
        platform: Literal["alpaca", "indi"],
        address: str,
        name: str | None,
        protocol: Literal["http", "https"],
        auto_select: bool,
    ) -> str:
        endpoint = str(address or "").strip()
        if not endpoint:
            raise ValueError("address is required")

        inventory_error: str | None = None
        try:
            inventory = self._discover_inventory(platform=platform, address=endpoint)
        except Exception as exc:
            inventory = {}
            inventory_error = str(exc)

        normalized_address = self._normalized_address(platform=platform, address=endpoint)

        telescope_name = (name or "").strip() or f"{platform.upper()} {normalized_address}"
        now = time.time()

        existing = self._find_profile_by_platform_address(platform=platform, address=normalized_address)
        if existing is None:
            profile = TelescopeProfile(
                telescope_id=f"tel_{uuid.uuid4().hex}",
                name=telescope_name,
                platform=platform,
                address=normalized_address,
                protocol=protocol,
                inventory=inventory,
                registered_epoch=now,
                last_seen_epoch=now,
            )
            self.valves.telescopes.append(profile)
        else:
            profile = existing
            profile.name = telescope_name
            profile.protocol = protocol
            profile.inventory = inventory
            profile.last_seen_epoch = now

        if auto_select:
            self.valves.active_telescope_id = profile.telescope_id

        self._persist_valves()
        if auto_select:
            self._sync_platform_valves(profile)

        payload = {
            "status": "registered",
            "active_telescope_id": self.valves.active_telescope_id,
            "telescope": profile.model_dump(),
            "message": (
                "Telescope registered and selected. "
                "Use telescope_inventory(refresh=true) to refresh devices later."
            ),
        }
        if inventory_error is not None:
            payload["inventory_warning"] = (
                f"Telescope registered but could not reach the server to fetch live inventory: {inventory_error}. "
                "Use telescope_inventory(refresh=true) once the server is reachable."
            )
        return json.dumps(payload, indent=2)

    def _refresh_inventory_sync(self, telescope_id: str) -> str:
        profile = self._find_profile(telescope_id)
        if profile is None:
            raise ValueError(f"Unknown telescope_id: {telescope_id}")

        profile.inventory = self._discover_inventory(platform=profile.platform, address=profile.address)
        profile.last_seen_epoch = time.time()
        self._persist_valves()

        if self.valves.active_telescope_id == telescope_id:
            self._sync_platform_valves(profile)

        return json.dumps(
            {
                "status": "refreshed",
                "active_telescope_id": self.valves.active_telescope_id,
                "telescope": profile.model_dump(),
            },
            indent=2,
        )

    def _discover_inventory(self, platform: Literal["alpaca", "indi"], address: str) -> dict[str, Any]:
        if platform == "alpaca":
            return self._discover_alpaca_inventory(address)
        return discover_indi_devices(address, timeout_seconds=float(self.valves.indi_discovery_timeout_seconds))

    def _discover_alpaca_inventory(self, address: str) -> dict[str, Any]:
        endpoint = str(address).strip()
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
                f"'{endpoint}'. Verify host:port and server availability."
            ) from exc

        normalized_devices: list[dict[str, Any]] = []
        for device in configured_devices:
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

        return {
            "platform": "alpaca",
            "alpaca_address": endpoint,
            "api_versions": api_versions,
            "server": server_description,
            "devices": normalized_devices,
            "valid_telescope_device_numbers": telescope_numbers,
            "valid_camera_device_numbers": camera_numbers,
            "recommended_telescope_device_number": telescope_numbers[0] if len(telescope_numbers) == 1 else None,
            "recommended_camera_device_number": camera_numbers[0] if len(camera_numbers) == 1 else None,
        }

    def _normalized_address(self, platform: Literal["alpaca", "indi"], address: str) -> str:
        raw = str(address or "").strip()
        if platform == "indi":
            return normalize_indi_address(raw)
        return raw

    def _find_profile(self, telescope_id: str) -> TelescopeProfile | None:
        for item in self.valves.telescopes:
            if item.telescope_id == telescope_id:
                return item
        return None

    def _find_profile_by_platform_address(self, platform: str, address: str) -> TelescopeProfile | None:
        for item in self.valves.telescopes:
            if item.platform == platform and item.address == address:
                return item
        return None

    def _persist_valves(self) -> None:
        self._store.set("telescope_registry_tool", self.valves.model_dump())

    def _sync_platform_valves(self, profile: TelescopeProfile) -> None:
        inventory = profile.inventory or {}
        if profile.platform == "alpaca":
            telescope_number = inventory.get("recommended_telescope_device_number")
            camera_number = inventory.get("recommended_camera_device_number")

            discovery_valves = self._store.get("alpaca_device_discovery_tool")
            discovery_valves["default_alpaca_address"] = profile.address
            self._store.set("alpaca_device_discovery_tool", discovery_valves)

            imaging_valves = self._store.get("alpaca_telescope_imaging_tool")
            imaging_valves["default_alpaca_address"] = profile.address
            imaging_valves["default_protocol"] = profile.protocol
            if isinstance(telescope_number, int):
                imaging_valves["default_telescope_device_number"] = telescope_number
            if isinstance(camera_number, int):
                imaging_valves["default_camera_device_number"] = camera_number
            self._store.set("alpaca_telescope_imaging_tool", imaging_valves)
            return

        if profile.platform == "indi":
            indi_valves = self._store.get("indi_device_discovery_tool")
            indi_valves["default_indi_address"] = profile.address
            self._store.set("indi_device_discovery_tool", indi_valves)
