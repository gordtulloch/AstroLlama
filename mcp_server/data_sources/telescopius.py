"""
Telescopius RESTful API v2.2 data source.
https://api.telescopius.com/

Endpoints covered:
  GET /solar-system/times      — rise/transit/set for Sun, Moon and planets
  GET /targets/search          — advanced DSO/star target search
  GET /targets/highlights      — curated "best tonight" targets
  GET /targets/lists           — user's saved observation lists
  GET /targets/lists/{id}      — targets in a specific list
  GET /equipment/user          — user's telescope/equipment
  GET /quote-of-the-day/       — astronomy quote of the day
  GET /news/global             — latest astronomy news feeds

Authentication: Authorization: Key <api_key>  (all endpoints)
"""
from __future__ import annotations

import logging
from typing import Any

import httpx

logger = logging.getLogger(__name__)

_BASE_URL = "https://api.telescopius.com/v2.2"
_TIMEOUT = 15.0

# Ordered list used when formatting the solar-system table so bodies always
# appear in a sensible sequence regardless of dict ordering.
_SOLAR_BODY_ORDER = [
    "sun", "moon",
    "mercury", "venus", "mars",
    "jupiter", "saturn", "uranus", "neptune",
]


# ── HTTP helpers ──────────────────────────────────────────────────────────────

def _auth_headers(api_key: str) -> dict[str, str]:
    return {
        "Authorization": f"Key {api_key}",
        "Accept": "application/json",
    }


async def _get(
    path: str,
    api_key: str,
    params: dict[str, Any] | None = None,
) -> dict[str, Any] | list:
    """Authenticated GET to the Telescopius API.  Returns parsed JSON."""
    clean_params = {k: v for k, v in (params or {}).items() if v is not None}
    async with httpx.AsyncClient(timeout=_TIMEOUT) as client:
        resp = await client.get(
            f"{_BASE_URL}{path}",
            headers=_auth_headers(api_key),
            params=clean_params,
        )
        resp.raise_for_status()
        return resp.json()


def _handle_error(exc: Exception, endpoint: str) -> str:
    if isinstance(exc, httpx.HTTPStatusError):
        code = exc.response.status_code
        if code == 401:
            return "Telescopius API error 401: Unauthorized — check your API key."
        if code == 429:
            return "Telescopius API error 429: Rate limit exceeded — please wait and try again."
        try:
            detail = exc.response.json()
        except Exception:
            detail = exc.response.text[:300]
        return f"Telescopius API error {code} on {endpoint}: {detail}"
    return f"Error contacting Telescopius ({endpoint}): {exc}"


# ── Solar system times ────────────────────────────────────────────────────────

def _fmt_time(val: Any) -> str:
    """Trim seconds from HH:MM:SS timestamps; pass through anything else."""
    if val is None:
        return "—"
    s = str(val)
    # "HH:MM:SS" → "HH:MM"
    parts = s.split(":")
    if len(parts) == 3 and all(p.isdigit() for p in parts):
        return f"{parts[0]}:{parts[1]}"
    return s


async def get_solar_system_times(
    lat: float,
    lon: float,
    timezone: str,
    api_key: str,
    date: str | None = None,
) -> str:
    """Return rise / transit / set times for the Sun, Moon and planets."""
    params: dict[str, Any] = {"lat": lat, "lon": lon, "timezone": timezone}
    if date:
        params["date"] = date

    try:
        data = await _get("/solar-system/times", api_key=api_key, params=params)
    except Exception as exc:
        return _handle_error(exc, "/solar-system/times")

    if not isinstance(data, dict):
        return f"Unexpected response from Telescopius: {str(data)[:400]}"

    date_label = f" — {date}" if date else ""
    lines = [
        f"Solar System Rise / Transit / Set{date_label}",
        f"Location: {lat:.4f}°, {lon:.4f}°  |  Timezone: {timezone}",
        "=" * 60,
        f"{'Body':<12}  {'Rise':>8}  {'Transit':>8}  {'Set':>8}  Notes",
        "-" * 60,
    ]

    # Iterate in a predictable order, then append any extra bodies the API returns
    seen: set[str] = set()
    ordered_keys = [k for k in _SOLAR_BODY_ORDER if k in data]
    extra_keys = [k for k in data if k not in _SOLAR_BODY_ORDER]
    for key in ordered_keys + extra_keys:
        body = data[key]
        if not isinstance(body, dict):
            continue
        seen.add(key)
        name = key.capitalize()
        rise = _fmt_time(body.get("rise") or body.get("rise_time"))
        transit = _fmt_time(body.get("transit") or body.get("transit_time"))
        sets = _fmt_time(body.get("set") or body.get("set_time") or body.get("sets"))

        notes_parts: list[str] = []
        phase = body.get("phase") or body.get("phase_name") or body.get("moon_phase")
        illum = body.get("illumination") or body.get("illuminated") or body.get("illumination_pct")
        dawn = body.get("dawn_astronomical") or body.get("astronomical_dawn")
        dusk = body.get("dusk_astronomical") or body.get("astronomical_dusk")

        if phase:
            notes_parts.append(str(phase))
        if illum is not None:
            notes_parts.append(f"{illum}% lit")
        if dawn:
            notes_parts.append(f"Astro dawn {_fmt_time(dawn)}")
        if dusk:
            notes_parts.append(f"Astro dusk {_fmt_time(dusk)}")

        lines.append(
            f"{name:<12}  {rise:>8}  {transit:>8}  {sets:>8}  "
            + (", ".join(notes_parts) if notes_parts else "")
        )

    if not seen:
        lines.append("No solar system data returned.")

    lines += ["", "Source: Telescopius (telescopius.com)"]
    return "\n".join(lines)


# ── Target formatting helper ──────────────────────────────────────────────────

def _extract_list(data: dict | list, *keys: str) -> list[dict]:
    """Pull a list of dicts from a response, trying multiple candidate keys."""
    if isinstance(data, list):
        return [x for x in data if isinstance(x, dict)]
    if isinstance(data, dict):
        for k in keys:
            candidate = data.get(k)
            if isinstance(candidate, list):
                return [x for x in candidate if isinstance(x, dict)]
    return []


def _format_targets(data: Any, title: str, max_items: int = 20) -> str:
    targets = _extract_list(data, "targets", "results", "data", "items", "objects")

    if not targets:
        return (
            f"{title}\n{'=' * len(title)}\n\n"
            "No targets found.\n\n"
            f"Source: Telescopius (telescopius.com)"
        )

    lines = [title, "=" * len(title), ""]

    for i, t in enumerate(targets[:max_items], 1):
        # Name / alias
        name = (
            t.get("name") or t.get("target_name") or
            t.get("designation") or f"Target {i}"
        )
        aka = (
            t.get("also_known_as") or t.get("other_name") or
            t.get("catalog_name") or t.get("common_name") or ""
        )
        header = f"{i:>2}. {name}"
        if aka and aka.lower() != name.lower():
            header += f"  ({aka})"
        lines.append(header)

        # Metadata row
        obj_type = t.get("type_label") or t.get("type") or t.get("object_type") or ""
        constellation = t.get("constellation") or t.get("con") or ""
        magnitude = t.get("magnitude") or t.get("mag") or t.get("visual_magnitude")
        size = t.get("size") or t.get("angular_size") or t.get("size_arcmin")
        details: list[str] = []
        if obj_type:
            details.append(obj_type)
        if constellation:
            details.append(f"Con: {constellation}")
        if magnitude is not None:
            details.append(f"Mag: {magnitude}")
        if size is not None:
            details.append(f"Size: {size}′")

        # Observability
        alt = t.get("altitude") or t.get("alt") or t.get("max_altitude") or t.get("max_alt")
        rise = t.get("rise") or t.get("rise_time") or ""
        transit = t.get("transit") or t.get("transit_time") or ""
        sets = t.get("set") or t.get("set_time") or t.get("sets") or ""
        if alt is not None:
            details.append(f"Peak alt: {alt}°")
        timing = [p for p in (
            (f"Rise {_fmt_time(rise)}" if rise else ""),
            (f"Transit {_fmt_time(transit)}" if transit else ""),
            (f"Set {_fmt_time(sets)}" if sets else ""),
        ) if p]
        if timing:
            details.append("  ".join(timing))

        if details:
            lines.append("    " + "  ·  ".join(details))

        url = t.get("url") or t.get("link") or t.get("telescopius_url") or ""
        if url:
            lines.append(f"    {url}")

        lines.append("")

    total = None
    if isinstance(data, dict):
        total = data.get("total") or data.get("count") or data.get("total_count")
    if total and total > max_items:
        lines.append(f"(Showing {min(len(targets), max_items)} of {total} results)")
        lines.append("")

    lines.append("Source: Telescopius (telescopius.com)")
    return "\n".join(lines)


# ── Target search ─────────────────────────────────────────────────────────────

async def search_targets(
    lat: float,
    lon: float,
    timezone: str,
    api_key: str,
    object_types: str | None = None,
    min_altitude: int | None = None,
    max_magnitude: float | None = None,
    min_magnitude: float | None = None,
    date: str | None = None,
    limit: int = 15,
) -> str:
    """Advanced search for astronomical targets visible from a given location."""
    params: dict[str, Any] = {
        "lat": lat,
        "lon": lon,
        "timezone": timezone,
        "limit": min(int(limit), 50),
    }
    if object_types:
        params["types"] = object_types
    if min_altitude is not None:
        params["min_alt"] = int(min_altitude)
    if max_magnitude is not None:
        params["mag_max"] = max_magnitude
    if min_magnitude is not None:
        params["mag_min"] = min_magnitude
    if date:
        params["date"] = date

    try:
        data = await _get("/targets/search", api_key=api_key, params=params)
    except Exception as exc:
        return _handle_error(exc, "/targets/search")

    filters: list[str] = []
    if object_types:
        filters.append(f"types={object_types}")
    if min_altitude is not None:
        filters.append(f"min alt={min_altitude}°")
    if max_magnitude is not None:
        filters.append(f"mag≤{max_magnitude}")
    filter_str = f"  [{', '.join(filters)}]" if filters else ""

    return _format_targets(data, f"Telescopius Target Search{filter_str}")


# ── Tonight's highlights ──────────────────────────────────────────────────────

async def get_target_highlights(
    lat: float,
    lon: float,
    timezone: str,
    api_key: str,
    min_altitude: int | None = None,
    date: str | None = None,
) -> str:
    """Simplified target search — most popular targets best seen tonight."""
    params: dict[str, Any] = {"lat": lat, "lon": lon, "timezone": timezone}
    if min_altitude is not None:
        params["min_alt"] = int(min_altitude)
    if date:
        params["date"] = date

    try:
        data = await _get("/targets/highlights", api_key=api_key, params=params)
    except Exception as exc:
        return _handle_error(exc, "/targets/highlights")

    date_label = f" ({date})" if date else " (Tonight)"
    return _format_targets(data, f"Telescopius Target Highlights{date_label}")


# ── Target lists ──────────────────────────────────────────────────────────────

async def get_target_lists(api_key: str) -> str:
    """Return all of the user's Telescopius observation lists."""
    try:
        data = await _get("/targets/lists", api_key=api_key)
    except Exception as exc:
        return _handle_error(exc, "/targets/lists")

    lists = _extract_list(data, "lists", "data", "results", "items")
    if not lists:
        return (
            "No target lists found.  Make sure your API key is valid and "
            "you have saved lists on Telescopius."
        )

    lines = ["My Telescopius Observation Lists", "=" * 34, ""]
    for lst in lists:
        list_id = lst.get("id") or lst.get("list_id") or "?"
        name = lst.get("name") or lst.get("title") or f"List {list_id}"
        count = lst.get("count") or lst.get("targets_count") or lst.get("num_targets")
        desc = lst.get("description") or ""
        entry = f"• {name}  [ID: {list_id}]"
        if count is not None:
            entry += f"  — {count} target(s)"
        lines.append(entry)
        if desc:
            lines.append(f"  {desc}")

    lines += ["", "Source: Telescopius (telescopius.com)"]
    return "\n".join(lines)


async def get_target_list_by_id(
    list_id: int | str,
    api_key: str,
    lat: float | None = None,
    lon: float | None = None,
    timezone: str | None = None,
) -> str:
    """Return the targets inside a specific user list."""
    params: dict[str, Any] = {}
    if lat is not None:
        params["lat"] = lat
    if lon is not None:
        params["lon"] = lon
    if timezone:
        params["timezone"] = timezone

    try:
        data = await _get(f"/targets/lists/{list_id}", api_key=api_key, params=params or None)
    except Exception as exc:
        return _handle_error(exc, f"/targets/lists/{list_id}")

    name = ""
    if isinstance(data, dict):
        name = data.get("name") or data.get("title") or ""

    title = f"Target List: {name}" if name else f"Target List #{list_id}"
    return _format_targets(data, title)


# ── Equipment ─────────────────────────────────────────────────────────────────

async def get_equipment(api_key: str) -> str:
    """Return the user's telescope and equipment registered on Telescopius."""
    try:
        data = await _get("/equipment/user", api_key=api_key)
    except Exception as exc:
        return _handle_error(exc, "/equipment/user")

    equipment = _extract_list(data, "equipment", "telescopes", "cameras", "data", "items")
    if not equipment:
        # Fall back to treating the whole response as a single equipment item
        if isinstance(data, dict) and data:
            equipment = [data]
        else:
            return "No equipment found for this Telescopius account."

    lines = ["My Telescopius Equipment", "=" * 25, ""]

    for eq in equipment:
        name = eq.get("name") or eq.get("model") or eq.get("title") or "Unknown"
        eq_type = eq.get("type") or eq.get("equipment_type") or eq.get("category") or ""
        brand = eq.get("brand") or eq.get("manufacturer") or ""
        aperture = eq.get("aperture") or eq.get("aperture_mm") or eq.get("diameter_mm")
        focal_len = eq.get("focal_length") or eq.get("focal_length_mm") or eq.get("fl_mm")
        focal_ratio = eq.get("focal_ratio") or eq.get("f_ratio") or eq.get("f_number")
        sensor = eq.get("sensor") or eq.get("sensor_name") or ""
        pixel = eq.get("pixel_size") or eq.get("pixel_size_um")
        res_w = eq.get("resolution_width") or eq.get("width_px")
        res_h = eq.get("resolution_height") or eq.get("height_px")

        header = f"• {name}"
        if eq_type:
            header += f"  [{eq_type}]"
        lines.append(header)
        if brand:
            lines.append(f"  Brand:        {brand}")
        if aperture is not None:
            lines.append(f"  Aperture:     {aperture} mm")
        if focal_len is not None:
            lines.append(f"  Focal length: {focal_len} mm")
        if focal_ratio is not None:
            lines.append(f"  Focal ratio:  f/{focal_ratio}")
        if sensor:
            lines.append(f"  Sensor:       {sensor}")
        if pixel is not None:
            lines.append(f"  Pixel size:   {pixel} µm")
        if res_w and res_h:
            lines.append(f"  Resolution:   {res_w} × {res_h} px")
        lines.append("")

    lines.append("Source: Telescopius (telescopius.com)")
    return "\n".join(lines)


# ── Quote of the day ──────────────────────────────────────────────────────────

async def get_quote_of_day(api_key: str) -> str:
    """Return today's astronomy quote of the day."""
    try:
        data = await _get("/quote-of-the-day/", api_key=api_key)
    except Exception as exc:
        return _handle_error(exc, "/quote-of-the-day")

    if not isinstance(data, dict):
        return str(data)

    text = data.get("text") or data.get("quote") or data.get("content") or str(data)
    author = data.get("author") or data.get("author_name") or ""

    if author:
        return f'"{text}"\n\n— {author}\n\nSource: Telescopius Quote of the Day'
    return f'"{text}"\n\nSource: Telescopius Quote of the Day'


# ── Global astronomy news ─────────────────────────────────────────────────────

async def get_global_news(api_key: str, limit: int = 15) -> str:
    """Return the latest astronomy headlines from Telescopius global news feeds."""
    try:
        data = await _get("/news/global", api_key=api_key)
    except Exception as exc:
        return _handle_error(exc, "/news/global")

    articles = _extract_list(data, "news", "articles", "items", "data", "results")
    if not articles:
        return (
            "No astronomy news available at this time.\n\n"
            "Source: Telescopius (telescopius.com)"
        )

    lines = ["Latest Astronomy News — Telescopius", "=" * 37, ""]

    for i, art in enumerate(articles[:limit], 1):
        title = (
            art.get("title") or art.get("headline") or
            art.get("name") or f"Article {i}"
        )
        source = art.get("source") or art.get("feed") or art.get("publisher") or ""
        date = (
            art.get("date") or art.get("published") or
            art.get("pub_date") or art.get("published_at") or ""
        )
        url = art.get("url") or art.get("link") or art.get("href") or ""
        summary = (
            art.get("summary") or art.get("description") or
            art.get("excerpt") or art.get("content") or ""
        )

        meta: list[str] = []
        if source:
            meta.append(source)
        if date:
            meta.append(str(date)[:10])  # trim to YYYY-MM-DD if ISO

        lines.append(f"{i:>2}. {title}")
        if meta:
            lines.append(f"    [{' | '.join(meta)}]")
        if url:
            lines.append(f"    {url}")
        if summary:
            snippet = summary.strip()[:220]
            if len(summary.strip()) > 220:
                snippet += "…"
            lines.append(f"    {snippet}")
        lines.append("")

    lines.append("Source: Telescopius (telescopius.com)")
    return "\n".join(lines)
