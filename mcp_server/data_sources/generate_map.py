"""
generate_map — Starplot-based all-sky zenith chart generator.

Runs the CPU-bound rendering in a thread executor so it does not
block the async event loop.
"""
from __future__ import annotations

import asyncio
import logging
import os
import uuid
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

# Force the non-interactive Agg backend before matplotlib (or starplot) is
# imported anywhere.  Must happen before any pyplot import.
os.environ.setdefault("MPLBACKEND", "Agg")

logger = logging.getLogger(__name__)

_DOWNLOADS_DIR = (
    Path(__file__).resolve().parent.parent.parent / "data" / "downloads"
)

logger.info("generate_map: _DOWNLOADS_DIR resolved to %s", _DOWNLOADS_DIR)


def _render_zenith(filepath: str, lat: float, lon: float, dt: datetime) -> None:
    """CPU-bound starplot rendering — executed in a thread-pool worker."""
    import matplotlib
    matplotlib.use("Agg")  # no-op if already set; harmless duplicate

    from starplot import ZenithPlot, Observer, styles
    from starplot import _  # query-builder expression object

    observer = Observer(dt=dt, lat=lat, lon=lon)
    p = ZenithPlot(
        observer=observer,
        style=styles.PlotStyle().extend(styles.extensions.BLUE_MEDIUM),
        resolution=2048,
        autoscale=True,
    )
    p.constellations()
    p.stars(where=[_.magnitude < 4.6])
    p.constellation_labels()
    p.horizon()
    p.export(filepath)
    p.close_fig()


async def generate_map(
    lat: float,
    lon: float,
    location_name: str = "Unknown location",
    datetime_str: str = "now",
    timezone: str = "UTC",
) -> str:
    """
    Generate an all-sky zenith star map for *lat*/*lon* at *datetime_str*.

    Returns a short description with an image URL in the form
    ``Image: /api/files/<filename>.png`` that the orchestrator can detect.
    """
    # Resolve timezone
    try:
        tz = ZoneInfo(timezone)
    except (ZoneInfoNotFoundError, KeyError):
        tz = ZoneInfo("UTC")
        timezone = "UTC"

    # Resolve datetime
    if datetime_str.strip().lower() == "now":
        dt = datetime.now(tz)
    else:
        try:
            dt = datetime.fromisoformat(datetime_str)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=tz)
        except ValueError:
            dt = datetime.now(tz)

    _DOWNLOADS_DIR.mkdir(parents=True, exist_ok=True)
    filename = f"{uuid.uuid4().hex[:8]}_starmap.png"
    filepath = str(_DOWNLOADS_DIR / filename)
    url = f"/api/files/{filename}"

    logger.info("generate_map: saving to %s", filepath)

    loop = asyncio.get_event_loop()
    await loop.run_in_executor(None, _render_zenith, filepath, lat, lon, dt)

    if not Path(filepath).is_file():
        raise RuntimeError(f"Starplot rendering failed — file not created: {filepath}")

    date_str = dt.strftime("%Y-%m-%d")
    time_str = dt.strftime("%H:%M %Z").strip()
    lat_str = f"{abs(lat):.2f}\u00b0{'N' if lat >= 0 else 'S'}"
    lon_str = f"{abs(lon):.2f}\u00b0{'E' if lon >= 0 else 'W'}"

    return (
        f"Star map generated for {location_name}!\n"
        f"Date: {date_str}, Time: {time_str}\n"
        f"Coordinates: {lat_str}, {lon_str}\n"
        f"Image: {url}"
    )
