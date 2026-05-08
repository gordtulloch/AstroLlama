#!/usr/bin/env python3

"""
AstroLlamaMCP Server
"""

import asyncio
import logging
import argparse
import importlib
import importlib.util
import os
import sys
from pathlib import Path
from typing import Any
import json

import httpx
import mcp.server.stdio
import mcp.types as types
from mcp import Resource, Tool
from mcp.server import NotificationOptions, Server
from mcp.server.models import InitializationOptions
from mcp.server.streamable_http_manager import StreamableHTTPSessionManager
from pydantic import AnyUrl

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize server
server = Server("mcp-server")

_OWUI_TOOL_SPECS = None
_OWUI_MCP_TOOLS: list[types.Tool] | None = None
_OWUI_TOOLS_OBJ: dict[str, Any] | None = None
_OWUI_TOOLS_FINGERPRINT: tuple[tuple[str, int], ...] | None = None


def _tools_dir() -> Path:
    return Path(__file__).resolve().parent / "tools"


def _iter_root_tool_files() -> list[Path]:
    """Return only top-level Python tool files (no subfolders)."""
    folder = _tools_dir()
    if not folder.exists():
        return []
    files: list[Path] = []
    for entry in sorted(folder.iterdir()):
        if not entry.is_file():
            continue
        if entry.suffix != ".py":
            continue
        if entry.name in {"__init__.py", "openwebui_adapter.py"}:
            continue
        files.append(entry)
    return files


def _tools_fingerprint() -> tuple[tuple[str, int], ...]:
    """Return a lightweight fingerprint of tool files for hot-reload checks."""
    rows: list[tuple[str, int]] = []
    for py_file in _iter_root_tool_files():
        try:
            mtime_ns = py_file.stat().st_mtime_ns
        except OSError:
            mtime_ns = 0
        rows.append((py_file.name, mtime_ns))
    return tuple(rows)


def _load_module_from_file(py_file: Path):
    """Load (or reload) a Python module from a tools/*.py file path."""
    module_name = f"mcp_server.tools.dynamic_{py_file.stem}_{int(py_file.stat().st_mtime_ns)}"
    spec = importlib.util.spec_from_file_location(module_name, py_file)
    if spec is None or spec.loader is None:
        raise ImportError(f"Unable to load module spec from {py_file}")
    mod = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = mod
    spec.loader.exec_module(mod)
    return mod


def _instantiate_tools_class(module_name: str, module_obj: Any) -> Any | None:
    """Instantiate module.Tools with best-effort dependency injection."""
    tools_cls = getattr(module_obj, "Tools", None)
    if tools_cls is None:
        return None

    # Drop-in OpenWebUI-compatible tools should be zero-arg constructible.
    return tools_cls()


def _build_openwebui_tool_registry() -> tuple[dict[str, Any], list[types.Tool], dict[str, Any]]:
    """Build OpenWebUI-compatible tool specs and their MCP schema projection."""
    try:
        from .tools.openwebui_adapter import build_tool_specs, specs_to_mcp_tools, install_openwebui_shims
    except ImportError:
        from tools.openwebui_adapter import build_tool_specs, specs_to_mcp_tools, install_openwebui_shims

    install_openwebui_shims(Path(__file__).resolve().parent.parent / "data" / "downloads")

    module_names_env = os.environ.get("OWUI_TOOLS_MODULES", "").strip()
    modules: list[tuple[str, Any]] = []

    if module_names_env:
        for raw_name in [m.strip() for m in module_names_env.split(",") if m.strip()]:
            try:
                modules.append((raw_name, importlib.import_module(raw_name)))
            except Exception as exc:
                logger.warning("Skipping OWUI module %s: %s", raw_name, exc)
    else:
        for py_file in _iter_root_tool_files():
            try:
                mod = _load_module_from_file(py_file)
                modules.append((py_file.stem, mod))
            except Exception as exc:
                logger.warning("Skipping tool file %s: %s", py_file.name, exc)

    merged_specs: dict[str, Any] = {}
    tools_objects: dict[str, Any] = {}

    for module_name, module_obj in modules:
        try:
            tools_obj = _instantiate_tools_class(module_name, module_obj)
        except Exception as exc:
            logger.warning("Skipping tools module %s: failed to instantiate Tools (%s)", module_name, exc)
            continue
        if tools_obj is None:
            continue

        tools_objects[module_name] = tools_obj
        module_specs = build_tool_specs(tools_obj)

        for tool_name, spec in module_specs.items():
            if tool_name in merged_specs:
                logger.warning(
                    "Duplicate tool name '%s' from module %s ignored (already provided by another module)",
                    tool_name,
                    module_name,
                )
                continue
            merged_specs[tool_name] = spec

        logger.info("OpenWebUI tools loaded from module: %s (%d tool(s))", module_name, len(module_specs))

    return merged_specs, specs_to_mcp_tools(merged_specs), tools_objects


def _get_openwebui_tool_registry() -> tuple[dict[str, Any], list[types.Tool]]:
    """Lazily initialize and return the OpenWebUI tool registry."""
    global _OWUI_TOOL_SPECS, _OWUI_MCP_TOOLS, _OWUI_TOOLS_OBJ, _OWUI_TOOLS_FINGERPRINT
    current_fingerprint = _tools_fingerprint()
    needs_reload = (
        _OWUI_TOOL_SPECS is None
        or _OWUI_MCP_TOOLS is None
        or _OWUI_TOOLS_OBJ is None
        or _OWUI_TOOLS_FINGERPRINT != current_fingerprint
    )
    if needs_reload:
        _OWUI_TOOL_SPECS, _OWUI_MCP_TOOLS, _OWUI_TOOLS_OBJ = _build_openwebui_tool_registry()
        _OWUI_TOOLS_FINGERPRINT = current_fingerprint
        logger.info("Loaded %d OpenWebUI-compatible tool(s)", len(_OWUI_MCP_TOOLS))
    return _OWUI_TOOL_SPECS, _OWUI_MCP_TOOLS

@server.list_resources()
async def handle_list_resources() -> list[types.Resource]:
    """
    List all available MCP resources for astronomical data access documentation.
    """
    return [
        types.Resource(
            uri="astro://help/overview",
            name="AstroLlama MCP Server Help",
            description="Overview of astronomical data access through the AstroLlama MCP server",
            mimeType="text/plain"
        ),
        types.Resource(
            uri="astro://info/data_sources", 
            name="Data Sources Status",
            description="Current status and availability of all astronomical data sources",
            mimeType="text/plain"
        ),
        types.Resource(
            uri="astro://info/tool_valves",
            name="Tool Valves Schema",
            description="OpenWebUI-style Valves/UserValves schema and current values for custom UI configuration",
            mimeType="application/json"
        )
    ]


@server.read_resource()
async def handle_read_resource(uri: AnyUrl) -> str:
    """
    Read and return the content of a specific astronomical documentation/status resource.
    """
    if uri.scheme != "astro":
        raise ValueError(f"Unsupported URI scheme: {uri.scheme}")
    
    path = str(uri).replace("astro://", "")
    
    if path == "help/overview":
        return """
AstroLlama MCP Server - Data Access
========================
"""
    
    elif path == "info/data_sources":
        return """
Astronomical Data Sources Status
===============================

No data sources currently configured.
"""

    elif path == "info/tool_valves":
        _get_openwebui_tool_registry()
        modules: dict[str, Any] = {}
        if _OWUI_TOOLS_OBJ is not None:
            modules = _OWUI_TOOLS_OBJ

        valves_by_module: dict[str, Any] = {}
        user_valves_by_module: dict[str, Any] = {}

        for module_name, tools_obj in modules.items():
            valves = getattr(tools_obj, "valves", None)
            user_valves = getattr(tools_obj, "user_valves", None)

            if valves is not None and hasattr(valves, "model_json_schema"):
                valves_by_module[module_name] = {
                    "schema": valves.model_json_schema(),
                    "values": valves.model_dump(),
                }
            if user_valves is not None and hasattr(user_valves, "model_json_schema"):
                user_valves_by_module[module_name] = {
                    "schema": user_valves.model_json_schema(),
                    "values": user_valves.model_dump(),
                }

        return json.dumps(
            {
                "modules": list(modules.keys()),
                "valves": valves_by_module,
                "user_valves": user_valves_by_module,
            },
            indent=2,
        )
    
    else:
        raise ValueError(f"Unknown resource: {path}")


@server.list_tools()
async def handle_list_tools() -> list[types.Tool]:
    """List tools by introspecting OpenWebUI-compatible Tools methods."""
    _, mcp_tools = _get_openwebui_tool_registry()
    return mcp_tools


@server.call_tool()
async def call_tool(name: str, arguments: dict[str, Any]) -> list[types.TextContent]:
    """
    Execute data access tools with unified modular architecture.
    
    This function routes tool calls to appropriate data source modules and
    provides unified responses with consistent file management.
    """
    
    try:
        try:
            from .tools.openwebui_adapter import invoke_spec
        except ImportError:
            from tools.openwebui_adapter import invoke_spec

        specs, _ = _get_openwebui_tool_registry()
        spec = specs.get(name)
        if spec is None:
            raise ValueError(f"Unknown tool: {name}")

        result = await invoke_spec(spec, arguments or {})
        return [types.TextContent(type="text", text=result)]

    except Exception as e:
        logger.error(f"Error in tool {name}: {str(e)}")
        return [types.TextContent(
            type="text",
            text=f"Error executing {name}: {str(e)}"
        )]


_WMO_CODES = {
    0: "Clear sky", 1: "Mainly clear", 2: "Partly cloudy", 3: "Overcast",
    45: "Fog", 48: "Depositing rime fog",
    51: "Light drizzle", 53: "Moderate drizzle", 55: "Dense drizzle",
    61: "Slight rain", 63: "Moderate rain", 65: "Heavy rain",
    71: "Slight snow", 73: "Moderate snow", 75: "Heavy snow", 77: "Snow grains",
    80: "Slight showers", 81: "Moderate showers", 82: "Violent showers",
    85: "Slight snow showers", 86: "Heavy snow showers",
    95: "Thunderstorm", 96: "Thunderstorm with slight hail", 99: "Thunderstorm with heavy hail",
}


async def _get_current_time(location: str) -> str:
    """Return the current local time for a city by geocoding it to a timezone."""
    from zoneinfo import ZoneInfo, ZoneInfoNotFoundError
    from datetime import datetime

    # Use only the city name (before first comma) for geocoding
    search_term = location.split(",")[0].strip()
    logger.info(f"get_current_time: raw input='{location}', search_term='{search_term}'")

    params = {"name": search_term, "count": 1, "language": "en", "format": "json"}
    async with httpx.AsyncClient(timeout=10.0) as client:
        resp = await client.get("https://geocoding-api.open-meteo.com/v1/search", params=params)
        resp.raise_for_status()
        data = resp.json()

    results = data.get("results", [])
    if not results:
        return f"Could not find a location matching '{location}'."

    r = results[0]
    tz_name = r.get("timezone")
    if not tz_name:
        return f"Location found but no timezone data available for '{location}'."

    name_parts = [r.get("name", "")]
    for field in ("admin1", "country"):
        v = r.get(field)
        if v and v != name_parts[0]:
            name_parts.append(v)
    full_name = ", ".join(name_parts)

    try:
        tz = ZoneInfo(tz_name)
    except ZoneInfoNotFoundError:
        return f"Unknown timezone '{tz_name}' returned for '{location}'."

    now = datetime.now(tz)
    utc_offset = now.strftime("%z")
    utc_offset_fmt = f"UTC{utc_offset[:3]}:{utc_offset[3:]}"

    lines = [
        f"Current Time: {full_name}",
        f"{'=' * (14 + len(full_name))}",
        f"Local time:  {now.strftime('%A, %B %d, %Y  %H:%M:%S')}",
        f"Timezone:    {tz_name} ({utc_offset_fmt})",
        f"Coordinates: {r['latitude']}, {r['longitude']}",
    ]
    return "\n".join(lines)


async def _fetch_open_meteo_geocode(location: str, count: int = 1) -> str:
    """Geocode a place name using Open-Meteo geocoding API (free, no API key)."""
    count = max(1, min(int(count), 5))
    params = {"name": location, "count": count, "language": "en", "format": "json"}

    async with httpx.AsyncClient(timeout=10.0) as client:
        resp = await client.get("https://geocoding-api.open-meteo.com/v1/search", params=params)
        resp.raise_for_status()
        data = resp.json()

    results = data.get("results", [])
    if not results:
        return f"No results found for '{location}'."

    lines = [f"Geocoding results for: {location}", "=" * (25 + len(location)), ""]
    for i, r in enumerate(results, 1):
        name_parts = [r.get("name", "")]
        for field in ("admin1", "admin2", "country"):
            v = r.get(field)
            if v:
                name_parts.append(v)
        full_name = ", ".join(name_parts)

        lines.append(f"{i}. {full_name}")
        lines.append(f"   Latitude:  {r['latitude']}")
        lines.append(f"   Longitude: {r['longitude']}")
        lines.append(f"   Timezone:  {r.get('timezone', 'N/A')}")
        if r.get("elevation") is not None:
            lines.append(f"   Elevation: {r['elevation']} m")
        if r.get("population"):
            lines.append(f"   Population:{r['population']:,}")
        lines.append("")

    lines.append("Source: Open-Meteo geocoding (open-meteo.com)")
    return "\n".join(lines)


async def _fetch_open_meteo_weather(
    latitude: float,
    longitude: float,
    location_name: str = None,
    temperature_unit: str = "celsius",
    wind_speed_unit: str = "kmh",
) -> str:
    """Fetch current weather from Open-Meteo (free, no API key)."""
    params = {
        "latitude": latitude,
        "longitude": longitude,
        "current": ",".join([
            "temperature_2m", "apparent_temperature", "relative_humidity_2m",
            "precipitation", "weather_code", "wind_speed_10m",
            "wind_direction_10m", "surface_pressure", "cloud_cover",
        ]),
        "temperature_unit": temperature_unit,
        "wind_speed_unit": wind_speed_unit,
        "timeformat": "iso8601",
    }

    async with httpx.AsyncClient(timeout=10.0) as client:
        resp = await client.get("https://api.open-meteo.com/v1/forecast", params=params)
        resp.raise_for_status()
        data = resp.json()

    cur = data["current"]
    units = data["current_units"]
    code = cur.get("weather_code", 0)
    condition = _WMO_CODES.get(code, f"Unknown (WMO {code})")

    t_unit = "°C" if temperature_unit == "celsius" else "°F"
    ws_unit = units.get("wind_speed_10m", wind_speed_unit)

    label = location_name or f"{latitude:.4f}, {longitude:.4f}"

    lines = [
        f"Current Weather: {label}",
        f"{'=' * (18 + len(label))}",
        f"Condition:        {condition}",
        f"Temperature:      {cur['temperature_2m']}{t_unit}  (feels like {cur['apparent_temperature']}{t_unit})",
        f"Humidity:         {cur['relative_humidity_2m']} %",
        f"Precipitation:    {cur['precipitation']} {units.get('precipitation', 'mm')}",
        f"Wind:             {cur['wind_speed_10m']} {ws_unit} from {cur['wind_direction_10m']}\u00b0",
        f"Cloud cover:      {cur['cloud_cover']} %",
        f"Pressure:         {cur['surface_pressure']} {units.get('surface_pressure', 'hPa')}",
        f"",
        f"Updated:          {cur['time']}",
        f"Source:           Open-Meteo (open-meteo.com)",
    ]
    return "\n".join(lines)


async def main():
    """
    Main entry point for running the modular Astro MCP server.
    Supports --http [port] to run as a web server instead of stdio.
    """
    parser = argparse.ArgumentParser(description="Astro MCP Server")
    parser.add_argument(
        "--http",
        metavar="PORT",
        nargs="?",
        const=8000,
        type=int,
        default=None,
        help="Run HTTP server on the given port (default: 8000). Exposes POST /mcp.",
    )
    args = parser.parse_args()

    if args.http is not None:
        await _run_http(args.http)
    else:
        await _run_stdio()


async def _run_stdio():
    """Run the server over stdin/stdout (default MCP transport)."""
    async with mcp.server.stdio.stdio_server() as (read_stream, write_stream):
        await server.run(
            read_stream,
            write_stream,
            InitializationOptions(
                server_name="astro-mcp",
                server_version="0.1.0",
                capabilities=server.get_capabilities(
                    notification_options=NotificationOptions(),
                    experimental_capabilities={}
                ),
            ),
        )


async def _run_http(port: int):
    """Run the server as an HTTP endpoint at POST /mcp."""
    try:
        import uvicorn
        from starlette.applications import Starlette
        from starlette.responses import JSONResponse
        from starlette.routing import Mount, Route
        from starlette.types import ASGIApp
    except ImportError:
        raise SystemExit(
            "HTTP mode requires 'uvicorn' and 'starlette'. "
            "Install them with: pip install uvicorn starlette"
        )

    session_manager = StreamableHTTPSessionManager(
        app=server,
        stateless=False,
    )

    async def mcp_app(scope, receive, send):
        # Plain GET without SSE accept header: return usage info instead of 400
        if scope["type"] == "http" and scope["method"] == "GET":
            accept = dict(scope.get("headers", [])).get(b"accept", b"").decode()
            if "text/event-stream" not in accept:
                from starlette.responses import JSONResponse
                response = JSONResponse({
                    "endpoint": "/mcp",
                    "transport": "streamable-http",
                    "usage": {
                        "POST /mcp": "Send MCP JSON-RPC messages",
                        "GET /mcp": "Open SSE stream (requires Accept: text/event-stream)",
                        "DELETE /mcp": "Terminate session (requires mcp-session-id header)",
                    },
                    "note": "Connect using an MCP client configured for streamable-http transport.",
                })
                await response(scope, receive, send)
                return
        await session_manager.handle_request(scope, receive, send)

    async def root(request):
        return JSONResponse({
            "server": "astrollama-mcp",
            "version": "0.1.0",
            "mcp_endpoint": f"http://{request.headers.get('host', f'localhost:{port}')}/mcp",
            "transport": "streamable-http",
        })

    starlette_app = Starlette(
        routes=[
            Route("/", endpoint=root),
            Mount("/mcp", app=mcp_app),
        ],
    )

    config = uvicorn.Config(
        app=starlette_app,
        host="localhost",
        port=port,
        log_level="info",
    )
    uv_server = uvicorn.Server(config)

    logger.info(f"Starting HTTP MCP server on http://localhost:{port}/mcp")

    async with session_manager.run():
        await uv_server.serve()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass