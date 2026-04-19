#!/usr/bin/env python3

"""
MCP Server
"""

import asyncio
import logging
import argparse
from typing import Any, Dict, List
import json

import httpx
import mcp.server.stdio
import mcp.types as types
from mcp import Resource, Tool
from mcp.server import NotificationOptions, Server
from mcp.server.models import InitializationOptions
from mcp.server.streamable_http_manager import StreamableHTTPSessionManager
from pydantic import AnyUrl

# Import modular components
from data_sources import AstroqueryUniversal

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize server
server = Server("mcp-server")

class AstroMCPServer:
    """
    Unified Astronomical MCP Server with modular data source architecture.
    
    This server provides a single interface to multiple astronomical datasets
    with consistent file management, data preview, and analysis capabilities.
    
    Architecture:
    =============
    - Data Sources: Modular classes for each astronomical dataset
    - I/O Module: Unified file preview and management
    - Tools Module: Analysis and calculation tools (future expansion)
    - Utils Module: Common utilities and helpers (future expansion)
    
    """
    
    def __init__(self, base_dir: str = None):
        """
        Initialize the modular astronomical MCP server.
        
        Args:
            base_dir: Base directory for file storage (shared across all data sources)
        """
        self.base_dir = base_dir
        
        # Initialize data sources
        self.astroquery = AstroqueryUniversal(base_dir=base_dir)
        
        logger.info("MCP Server initialized with modular architecture")
        logger.info(f"Astroquery services available: {len(self.astroquery.list_services())}")

    def list_astroquery_services(self) -> List[Dict[str, Any]]:
        """List all available astroquery services."""
        return self.astroquery.list_services()
    
    def get_astroquery_service_details(self, service_name: str) -> Dict[str, Any]:
        """Get detailed information about a specific astroquery service."""
        return self.astroquery.get_service_details(service_name)
    
    def search_astroquery_services(self, **criteria) -> List[str]:
        """Search astroquery services by various criteria."""
        return self.astroquery.search_services(**criteria)
    
# Initialize server
astro_server = AstroMCPServer()

@server.list_resources()
async def handle_list_resources() -> list[types.Resource]:
    """
    List all available MCP resources for astronomical data access documentation.
    """
    return [
        types.Resource(
            uri="astro://help/overview",
            name="Astro MCP Server Help",
            description="Overview of astronomical data access through modular MCP server",
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
MCP Server - Data Access
========================
"""
    
    elif path == "info/data_sources":

        astroquery_status = f"✅ Available ({len(astro_server.astroquery._services)} services discovered)"
        
        return f"""
Astronomical Data Sources Status
===============================

Astroquery Services
- Status: {astroquery_status}
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
            name="list_astroquery_services",
            description="List all available astroquery services discovered by the server",
            inputSchema={"type": "object", "properties": {}}
        ),
        types.Tool(
            name="get_astroquery_service_details",
            description="Get detailed information about a specific astroquery service including capabilities, data types, and example queries",
            inputSchema={
                "type": "object",
                "properties": {
                    "service_name": {"type": "string", "description": "Name of the astroquery service (e.g., 'simbad', 'vizier', 'gaia')"}
                },
                "required": ["service_name"]
            }
        ),
        types.Tool(
            name="search_astroquery_services",
            description=(
                "Search for an astroquery service name when you do NOT already know it. "
                "DO NOT call this for standard star, object, or constellation queries — "
                "for those, call astroquery_query directly with service_name='simbad'. "
                "Use this ONLY when you need to discover which service handles an "
                "unfamiliar data type (e.g. radio continuum surveys, X-ray catalogues)."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "data_type": {"type": "string", "description": "Filter by data type (e.g. 'spectra', 'images', 'photometry')"},
                    "wavelength": {"type": "string", "description": "Filter by wavelength coverage (e.g. 'optical', 'radio', 'infrared', 'x-ray')"},
                    "object_type": {"type": "string", "description": "Filter by object type (e.g. 'galaxies', 'quasars')"}
                },
                "additionalProperties": False
            }
        ),
        types.Tool(
            name="astroquery_query",
            description=(
                "Perform a query against any ASTRONOMICAL astroquery service (e.g. simbad, vizier, gaia, mast, ned). "
                "DO NOT use this for weather, time, or geocoding — use get_weather, get_current_time, or get_latlong instead. "
                "\n\n"
                "FOR CONSTELLATION / MAGNITUDE QUERIES (e.g. 'stars in Orion brighter than magnitude 5'):\n"
                "  Call THIS tool directly — do NOT call search_astroquery_services or get_astroquery_service_details first.\n"
                "  Use service_name='simbad', query_type='query_region', object_name='Ori', "
                "radius=12 (Orion spans ~20 deg — use 10-15), vmag_max=5.\n"
                "  Do NOT also pass ra/dec when using object_name — pick one or the other.\n"
                "  Do NOT pass data_type, wavelength, magnitude, capability, requires_auth, "
                "or other non-SIMBAD parameters.\n\n"
                "  Alternatively use query_type='query_criteria' with "
                "criteria=\"region(Circle, Ori, 12d) & Vmag <= 5\".\n\n"
                "Call 'get_astroquery_service_details' only when you need to explore an unfamiliar service."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "service_name": {
                        "type": "string",
                        "description": "The astroquery service to use (e.g., 'simbad', 'vizier', 'mast', 'gaia')."
                    },
                    "query_type": {
                        "type": "string",
                        "description": "Query method: 'query_object' (single named object), 'query_region' (cone search by name or RA/Dec), 'query_criteria' (SIMBAD constraint language). Defaults to 'auto'.",
                        "default": "auto"
                    },
                    "auto_save": {
                        "type": "boolean",
                        "description": "Automatically save tabular results to a file.",
                        "default": True
                    },
                    "object_name": {
                        "type": "string",
                        "description": "Named object or constellation for the search centre (e.g. 'Ori', 'M42', 'Betelgeuse'). Use this OR ra+dec, not both."
                    },
                    "ra": {
                        "type": "number",
                        "description": "Right Ascension in decimal degrees (0-360). Use this with dec instead of object_name when you have explicit coordinates."
                    },
                    "dec": {
                        "type": "number",
                        "description": "Declination in decimal degrees (-90 to +90). Use with ra."
                    },
                    "radius": {
                        "type": "number",
                        "description": "Search radius in decimal degrees. Orion spans ~20 deg — use 10-15. Default 0.1 is far too small for constellation searches."
                    },
                    "vmag_max": {
                        "type": "number",
                        "description": "SIMBAD only: keep only objects with V-magnitude <= this value. Use this for magnitude-limited star queries (e.g. vmag_max=5 for mag 5 or brighter)."
                    },
                    "criteria": {
                        "type": "string",
                        "description": "SIMBAD query_criteria string, e.g. \"region(Circle, Ori, 12d) & Vmag <= 5\". Use with query_type='query_criteria'."
                    }
                },
                "required": ["service_name"],
                "additionalProperties": False
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
    ]


@server.call_tool()
async def call_tool(name: str, arguments: dict[str, Any]) -> list[types.TextContent]:
    """
    Execute data access tools with unified modular architecture.
    
    This function routes tool calls to appropriate data source modules and
    provides unified responses with consistent file management.
    """
    
    try:
        if name == "get_weather":
            result = await _fetch_open_meteo_weather(**arguments)
            return [types.TextContent(type="text", text=result)]

        elif name == "get_latlong":
            result = await _fetch_open_meteo_geocode(**arguments)
            return [types.TextContent(type="text", text=result)]

        elif name == "get_current_time":
            result = await _get_current_time(**arguments)
            return [types.TextContent(type="text", text=result)]

        elif name == "list_astroquery_services":
            services = astro_server.list_astroquery_services()
            
            if not services:
                return [types.TextContent(type="text", text="No astroquery services found.")]
            
            response = "Available Astroquery Services:\n"
            response += "==============================\n\n"
            for service in services:
                response += f"- {service['full_name']} (service name: '{service['service']}')\n"
                response += f"  Description: {service['description']}\n\n"
            
            response += "Use `get_astroquery_service_details` with a service name for more information."
            return [types.TextContent(type="text", text=response)]
        
        elif name == "get_astroquery_service_details":
            service_name = arguments["service_name"]
            details = astro_server.get_astroquery_service_details(service_name)

            if not details:
                return [types.TextContent(type="text", text=f"Service '{service_name}' not found.")]

            response = f"Details for: {details['full_name']} (service: '{details['service']}')\n"
            response += "=" * (len(response) - 1) + "\n\n"
            response += f"Description: {details['description']}\n\n"
            
            response += "Capabilities:\n"
            for cap in details['capabilities']:
                response += f"- {cap}\n"
            response += "\n"

            response += "Data Types:\n"
            for dt in details['data_types']:
                response += f"- {dt}\n"
            response += "\n"

            response += "Wavelength Coverage:\n"
            # Handle the case where wavelength_coverage might be a string or list
            wavelength_coverage = details['wavelength_coverage']
            if isinstance(wavelength_coverage, list):
                for wl in wavelength_coverage:
                    response += f"- {wl}\n"
            else:
                response += f"- {wavelength_coverage}\n"
            response += "\n"

            if details['example_queries']:
                response += "Example Queries:\n"
                for i, ex in enumerate(details['example_queries'], 1):
                    response += f"{i}. {ex['description']}\n"
                    response += f"   `{ex['query']}`\n"
            
            return [types.TextContent(type="text", text=response)]
        
        elif name == "search_astroquery_services":
            criteria = {k: v for k, v in arguments.items() if k != "service_name"}
            services = astro_server.search_astroquery_services(**criteria)
            
            if not services:
                return [types.TextContent(type="text", text="No matching services found.")]
            
            response = "Found services matching your criteria:\n\n"
            for service in services:
                response += f"- {service['full_name']} ({service['service']}) - Score: {service['score']}\n"
                response += f"  Description: {service['description']}\n"
                response += f"  Reasons: {', '.join(service['reasons'])}\n\n"
            
            return [types.TextContent(type="text", text=response)]
        
        elif name == "astroquery_query":
            # Guard: redirect clearly misrouted calls
            service = arguments.get('service_name', '').lower()
            non_astroquery = {
                'weather': 'get_weather',
                'time': 'get_current_time',
                'latlong': 'get_latlong',
                'geocoding': 'get_latlong',
            }
            if service in non_astroquery:
                return [types.TextContent(
                    type="text",
                    text=f"'{service}' is not an astroquery service. Use the '{non_astroquery[service]}' tool instead."
                )]

            # Backward compatibility: user might still use 'object'
            if 'object' in arguments and 'object_name' not in arguments:
                arguments['object_name'] = arguments.pop('object')

            result = astro_server.astroquery.universal_query(**arguments)
            
            if result['status'] in ['error', 'auth_required']:
                # The help text is already pre-formatted
                return [types.TextContent(type="text", text=result['help'])]

            # Success case — always return inline data so the LLM can answer directly
            response = f"Successfully executed '{result['query_type']}' on '{result['service']}'.\n"
            response += f"Found {result['num_results']} results.\n\n"

            results_data = result['results']
            if isinstance(results_data, list) and len(results_data) > 0:
                # Sort by V magnitude ascending (brightest first) when available
                vmag_key = next((k for k in results_data[0] if k.upper() == 'FLUX_V'), None)
                if vmag_key:
                    def _vmag_sort(row):
                        v = row.get(vmag_key)
                        try:
                            return float(v)
                        except (TypeError, ValueError):
                            return 999.0
                    results_data = sorted(results_data, key=_vmag_sort)

                # Select the most useful columns; fall back to all columns
                KEY_COLS = ['MAIN_ID', 'FLUX_V', 'RA', 'DEC', 'SP_TYPE', 'OTYPE']
                sample = results_data[0]
                cols = [c for c in KEY_COLS if c in sample] or list(sample.keys())

                MAX_INLINE = 25
                preview = [{c: row.get(c) for c in cols} for row in results_data[:MAX_INLINE]]
                label = "sorted by V magnitude (brightest first)" if vmag_key else "first results"
                response += f"Results ({label}):\n"
                response += json.dumps(preview, indent=2)
                if result['num_results'] > MAX_INLINE:
                    response += f"\n\n... {result['num_results'] - MAX_INLINE} more rows not shown."
            elif isinstance(results_data, str):
                response += results_data

            # Footnote: mention saved file without directing LLM to call another tool
            save_result = result.get('save_result')
            if save_result and save_result.get('status') == 'success':
                response += f"\n\nFull dataset saved to: {save_result['filename']}"

            logger.info(
                f"[astroquery_query] Tool response ({len(response)} chars):\n"
                f"{'─' * 72}\n{response}\n{'─' * 72}"
            )

            return [types.TextContent(type="text", text=response)]
        
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
            "server": "astro-mcp",
            "version": "0.1.0",
            "mcp_endpoint": f"http://{request.headers.get('host', f'localhost:{port}')}/mcp",
            "transport": "streamable-http",
        })

    starlette_app = Starlette(
        routes=[
            Route("/", endpoint=root),
            Mount("/mcp", app=mcp_app),
        ]
    )

    config = uvicorn.Config(
        app=starlette_app,
        host="0.0.0.0",
        port=port,
        log_level="info",
    )
    uv_server = uvicorn.Server(config)

    logger.info(f"Starting HTTP MCP server on http://0.0.0.0:{port}/mcp")

    async with session_manager.run():
        await uv_server.serve()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass