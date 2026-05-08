from __future__ import annotations

import httpx


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


async def get_current_time(location: str) -> str:
    """Return the current local time for a city by geocoding it to a timezone."""
    from zoneinfo import ZoneInfo, ZoneInfoNotFoundError
    from datetime import datetime

    search_term = location.split(",")[0].strip()
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


async def fetch_open_meteo_geocode(location: str, count: int = 1) -> str:
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


async def fetch_open_meteo_weather(
    latitude: float,
    longitude: float,
    location_name: str | None = None,
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
        f"Wind:             {cur['wind_speed_10m']} {ws_unit} from {cur['wind_direction_10m']}°",
        f"Cloud cover:      {cur['cloud_cover']} %",
        f"Pressure:         {cur['surface_pressure']} {units.get('surface_pressure', 'hPa')}",
        "",
        f"Updated:          {cur['time']}",
        "Source:           Open-Meteo (open-meteo.com)",
    ]
    return "\n".join(lines)
