from __future__ import annotations

import re
import time
from typing import Any


_DEFAULT_INDI_PORT = 7624


def parse_indi_address(address: str, default_port: int = _DEFAULT_INDI_PORT) -> tuple[str, int]:
    raw = str(address or "").strip()
    if not raw:
        raise ValueError("address is required")

    host = raw
    port = int(default_port)
    if ":" in raw:
        host_part, port_part = raw.rsplit(":", 1)
        host = host_part.strip()
        if not host:
            raise ValueError("address host is required")
        try:
            port = int(port_part)
        except ValueError as exc:
            raise ValueError(f"invalid INDI port in address: {raw}") from exc

    if port < 1 or port > 65535:
        raise ValueError("INDI port must be between 1 and 65535")
    return host, port


def normalize_indi_address(address: str, default_port: int = _DEFAULT_INDI_PORT) -> str:
    host, port = parse_indi_address(address, default_port=default_port)
    return f"{host}:{port}"


def _interface_map(pyindi_module: Any) -> dict[str, int]:
    out: dict[str, int] = {}
    for name in dir(pyindi_module):
        if not name.endswith("_INTERFACE"):
            continue
        value = getattr(pyindi_module, name, None)
        if isinstance(value, int):
            out[name] = value
    return out


def _capabilities_from_mask(mask: int, iface_map: dict[str, int]) -> list[str]:
    candidates = {
        "camera": ["CCD_INTERFACE"],
        "telescope": ["TELESCOPE_INTERFACE"],
        "focuser": ["FOCUSER_INTERFACE"],
        "filterwheel": ["FILTER_INTERFACE"],
        "dome": ["DOME_INTERFACE"],
        "gps": ["GPS_INTERFACE"],
        "guider": ["GUIDER_INTERFACE"],
    }
    out: list[str] = []
    for label, keys in candidates.items():
        for key in keys:
            bit = iface_map.get(key)
            if isinstance(bit, int) and bit != 0 and (mask & bit) == bit:
                out.append(label)
                break
    return out


def discover_indi_devices(address: str, timeout_seconds: float = 5.0) -> dict[str, Any]:
    host, port = parse_indi_address(address)
    try:
        import PyIndi  # type: ignore
    except Exception as exc:
        raise RuntimeError(
            "PyIndi client library is unavailable. Install it with: pip install git+https://github.com/indilib/pyindi-client.git"
        ) from exc

    iface_map = _interface_map(PyIndi)

    class _ProbeClient(PyIndi.BaseClient):  # type: ignore[misc]
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

    client = _ProbeClient()
    client.setServer(host, int(port))
    if not client.connectServer():
        raise RuntimeError(
            f"Unable to reach INDI server at '{host}:{port}'. Verify host/port and that indiserver is running."
        )

    deadline = time.monotonic() + max(float(timeout_seconds), 0.5)
    devices: list[Any] = []
    try:
        while time.monotonic() < deadline:
            current = list(client.getDevices() or [])
            if current:
                devices = current
                break
            time.sleep(0.2)
        if not devices:
            devices = list(client.getDevices() or [])
    finally:
        try:
            client.disconnectServer()
        except Exception:
            pass

    normalized: list[dict[str, Any]] = []
    type_indexes: dict[str, list[int]] = {
        "camera": [],
        "telescope": [],
        "focuser": [],
        "filterwheel": [],
        "dome": [],
        "gps": [],
        "guider": [],
    }

    for idx, device in enumerate(devices):
        name = ""
        try:
            name = str(device.getDeviceName())
        except Exception:
            name = f"INDI Device {idx}"

        mask = 0
        try:
            mask = int(device.getDriverInterface())
        except Exception:
            mask = 0

        caps = _capabilities_from_mask(mask, iface_map)
        for cap in caps:
            if cap in type_indexes:
                type_indexes[cap].append(idx)

        normalized.append(
            {
                "device_type": caps[0] if caps else "device",
                "device_number": idx,
                "device_name": name,
                "unique_id": re.sub(r"\s+", "_", name.strip()) or f"indi_{idx}",
                "capabilities": caps,
                "interface_mask": mask,
            }
        )

    return {
        "indi_address": f"{host}:{port}",
        "server": {"host": host, "port": port},
        "devices": normalized,
        "valid_telescope_device_numbers": type_indexes["telescope"],
        "valid_camera_device_numbers": type_indexes["camera"],
        "valid_focuser_device_numbers": type_indexes["focuser"],
        "valid_filterwheel_device_numbers": type_indexes["filterwheel"],
        "recommended_telescope_device_number": (
            type_indexes["telescope"][0] if len(type_indexes["telescope"]) == 1 else None
        ),
        "recommended_camera_device_number": type_indexes["camera"][0] if len(type_indexes["camera"]) == 1 else None,
    }
