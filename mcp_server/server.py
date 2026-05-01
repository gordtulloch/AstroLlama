#!/usr/bin/env python3

"""
AstroLlamaMCP Server
"""

import asyncio
import logging
import argparse
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
    
    else:
        raise ValueError(f"Unknown resource: {path}")


@server.list_tools()
async def handle_list_tools() -> list[types.Tool]:
    """List all available astronomical data access tools."""
    return [ 
        types.Tool(
            name="get_latlong",
            description="Geocode a city or place name to latitude, longitude, and timezone using the free Open-Meteo geocoding API. Returns up to 5 matching results ranked by relevance. No API key required.",
            inputSchema={
                "type": "object",
                "properties": {
                    "location": {
                        "type": "string",
                        "description": "City or place name to look up, e.g. 'Winnipeg, Manitoba' or 'Paris' or 'Tokyo, Japan'"
                    },
                    "count": {
                        "type": "integer",
                        "description": "Maximum number of results to return (default: 1, max: 5)",
                        "default": 1
                    }
                },
                "required": ["location"]
            }
        ),
        types.Tool(
            name="get_current_time",
            description="Get the current local time and date for any city or place. Geocodes the city name via Open-Meteo to determine its timezone, then returns the current time. No API key required.",
            inputSchema={
                "type": "object",
                "properties": {
                    "location": {
                        "type": "string",
                        "description": "City or place name, e.g. 'Winnipeg, Manitoba' or 'Tokyo, Japan' or 'London'"
                    }
                },
                "required": ["location"]
            }
        ),
        types.Tool(
            name="simbad_search",
            description="Search the SIMBAD astronomical database for stars, nebulae, galaxies, clusters, and other celestial objects. Supports natural-language queries such as 'List the 10 brightest stars in the sky', 'Emission nebulae in Orion', or 'Globular clusters in Sagittarius'. Results are formatted for non-scientists with common names where available. If no limit is specified, returns up to 10 results.",
            inputSchema={
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": "Natural-language search query, e.g. 'List the 10 brightest stars', 'Emission nebulae in Orion', 'Globular clusters in Sagittarius'"
                    },
                    "limit": {
                        "type": "integer",
                        "description": "Maximum number of results to return (default: 10, max: 100). Overridden if a number is mentioned in the query.",
                        "default": 10
                    }
                },
                "required": ["query"]
            }
        ),
        types.Tool(
            name="get_weather",
            description="Get current weather conditions for any location using the free Open-Meteo API. Returns temperature, wind speed, humidity, precipitation, pressure, and a human-readable weather description. No API key required.",
            inputSchema={
                "type": "object",
                "properties": {
                    "latitude": {"type": "number", "description": "Latitude of the location in decimal degrees (-90 to 90)"},
                    "longitude": {"type": "number", "description": "Longitude of the location in decimal degrees (-180 to 180)"},
                    "location_name": {"type": "string", "description": "Optional display name for the location (e.g. 'Paris, France')"},
                    "temperature_unit": {"type": "string", "enum": ["celsius", "fahrenheit"], "default": "celsius"},
                    "wind_speed_unit": {"type": "string", "enum": ["kmh", "mph", "ms", "kn"], "default": "kmh"}
                },
                "required": ["latitude", "longitude"]
            }
        ),
        types.Tool(
            name="generate_map",
            description=(
                "Generate an all-sky zenith star map (sky chart) for a specific location and time. "
                "Shows stars visible from that location at that time with constellation lines and labels. "
                "Returns a link to a PNG image. "
                "Use this when the user asks to see a star chart, sky map, what the night sky looks like, "
                "or wants a visual representation of the stars visible from a given place and time. "
                "Do NOT use this when the user asks about a specific constellation — use generate_constellation_map instead."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "lat": {"type": "number", "description": "Observer latitude in decimal degrees (-90 to 90)"},
                    "lon": {"type": "number", "description": "Observer longitude in decimal degrees (-180 to 180)"},
                    "location_name": {"type": "string", "description": "Display name for the location (e.g. 'Winnipeg, Manitoba')", "default": "Unknown location"},
                    "datetime_str": {"type": "string", "description": "ISO 8601 datetime string (e.g. '2026-04-19T22:00:00'), or 'now' for the current time", "default": "now"},
                    "timezone": {"type": "string", "description": "IANA timezone name (e.g. 'America/Winnipeg', 'America/New_York', 'UTC'). Used when datetime_str is 'now' or lacks timezone info.", "default": "UTC"}
                },
                "required": ["lat", "lon"]
            }
        ),
        types.Tool(
            name="generate_constellation_map",
            description=(
                "Generate a detailed star chart centred on a specific constellation, showing its stars, "
                "deep-sky objects, and Milky Way within the constellation boundaries. "
                "Returns a link to a PNG image. "
                "Use this whenever the user mentions a specific constellation by name and wants to see it, "
                "e.g. 'show me Orion', 'map of Scorpius', 'what does Cassiopeia look like'."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "constellation": {
                        "type": "string",
                        "description": "Constellation name (full name such as 'Orion', 'Ursa Major', or IAU abbreviation such as 'ORI', 'UMA')"
                    }
                },
                "required": ["constellation"]
            }
        ),
        types.Tool(
            name="generate_aavso_map",
            description=(
                "Generate an AAVSO Variable Star Plotter (VSP) finder chart for a variable star. "
                "Fetches a PNG chart from the AAVSO VSP API showing the star field with "
                "comparison star magnitudes. Returns a link to the chart image. "
                "Use this when the user asks about a variable star chart, finder chart, "
                "or wants to observe a variable star such as 'SS Cyg', 'Mira', 'RR Lyr', "
                "'Delta Cephei', or any other variable star."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "star": {
                        "type": "string",
                        "description": "Name of the variable star, e.g. 'SS Cyg', 'Mira', 'RR Lyr'. Provide this OR ra+dec."
                    },
                    "ra": {
                        "type": "number",
                        "description": "Right Ascension in decimal degrees (0–360). Use with dec when star name is not known."
                    },
                    "dec": {
                        "type": "number",
                        "description": "Declination in decimal degrees (−90 to +90). Use with ra when star name is not known."
                    },
                    "fov": {
                        "type": "number",
                        "description": "Field of view in arcminutes (default 60).",
                        "default": 60
                    },
                    "maglimit": {
                        "type": "number",
                        "description": "Faint magnitude limit for comparison stars on the chart (default 14.5).",
                        "default": 14.5
                    }
                },
                "required": []
            }
        ),
        types.Tool(
            name="variable_comparison_stars",
            description=(
                "Retrieve a table of comparison stars with photometric magnitudes for a variable star "
                "from the AAVSO Variable Star Plotter (VSP) database. "
                "Returns AUID, coordinates, chart label, and magnitudes in V, B, Rc, Ic, and near-IR bands. "
                "Use this when the user asks for comparison stars, magnitude reference stars, "
                "photometry data, or how to measure the brightness of a variable star."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "star": {
                        "type": "string",
                        "description": "Name of the variable star, e.g. 'SS Cyg', 'Mira', 'RR Lyr'. Provide this OR ra+dec."
                    },
                    "ra": {
                        "type": "number",
                        "description": "Right Ascension in decimal degrees (0\u2013360). Use with dec when star name is not known."
                    },
                    "dec": {
                        "type": "number",
                        "description": "Declination in decimal degrees (\u221290 to +90). Use with ra when star name is not known."
                    },
                    "fov": {
                        "type": "number",
                        "description": "Field of view in arcminutes (default 60).",
                        "default": 60
                    },
                    "maglimit": {
                        "type": "number",
                        "description": "Faintest comparison star magnitude to include (default 14.5).",
                        "default": 14.5
                    }
                },
                "required": []
            }
        ),
    ]


@server.call_tool()
async def call_tool(name: str, arguments: dict[str, Any]) -> list[types.TextContent]:
    """
    Execute data access tools with unified modular architecture.
    
    This function routes tool calls to appropriate data source modules and
    provides unified responses with consistent file management.
    """
    
    try:
        if name == "simbad_search":
            try:
                from .data_sources.simbad_search import simbad_search
            except ImportError:
                from data_sources.simbad_search import simbad_search
            result = await simbad_search(**arguments)
            return [types.TextContent(type="text", text=result)]

        elif name == "get_weather":
            result = await _fetch_open_meteo_weather(**arguments)
            return [types.TextContent(type="text", text=result)]

        elif name == "get_latlong":
            result = await _fetch_open_meteo_geocode(**arguments)
            return [types.TextContent(type="text", text=result)]

        elif name == "get_current_time":
            result = await _get_current_time(**arguments)
            return [types.TextContent(type="text", text=result)]

        elif name == "generate_map":
            try:
                from .data_sources.generate_map import generate_map
            except ImportError:
                from data_sources.generate_map import generate_map
            result = await generate_map(**arguments)
            return [types.TextContent(type="text", text=result)]

        elif name == "generate_constellation_map":
            try:
                from .data_sources.generate_constellation_map import generate_constellation_map
            except ImportError:
                from data_sources.generate_constellation_map import generate_constellation_map
            result = await generate_constellation_map(**arguments)
            return [types.TextContent(type="text", text=result)]

        elif name == "generate_aavso_map":
            try:
                from .data_sources.generate_aavso_map import generate_aavso_map
            except ImportError:
                from data_sources.generate_aavso_map import generate_aavso_map
            result = await generate_aavso_map(**arguments)
            return [types.TextContent(type="text", text=result)]

        elif name == "variable_comparison_stars":
            try:
                from .data_sources.variable_comparison_stars import variable_comparison_stars
            except ImportError:
                from data_sources.variable_comparison_stars import variable_comparison_stars
            result = await variable_comparison_stars(**arguments)
            return [types.TextContent(type="text", text=result)]

        else:
            raise ValueError(f"Unknown tool: {name}")
    
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