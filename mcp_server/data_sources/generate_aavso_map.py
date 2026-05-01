"""
generate_aavso_map — Variable star finder chart via the AAVSO VSP API.

Calls the AAVSO Variable Star Plotter (VSP) REST API to obtain a PNG finder
chart for a given variable star (or RA/Dec position), then saves the image
to the shared downloads directory so the orchestrator can serve it as a
local /api/files/ URL.

API reference: https://app.aavso.org/vsp/api/chart/
"""
from __future__ import annotations

import logging
import uuid
from pathlib import Path

import httpx

logger = logging.getLogger(__name__)

_VSP_API_URL = "https://app.aavso.org/vsp/api/chart/"

_DOWNLOADS_DIR = (
    Path(__file__).resolve().parent.parent.parent / "data" / "downloads"
)


async def generate_aavso_map(
    star: str | None = None,
    ra: float | None = None,
    dec: float | None = None,
    fov: float = 60.0,
    maglimit: float = 14.5,
) -> str:
    """
    Generate a variable-star finder chart using the AAVSO VSP API.

    Provide *star* **or** both *ra* and *dec* to identify the target.

    Parameters
    ----------
    star:     Name of the variable star (e.g. "SS Cyg", "Mira", "RR Lyr").
    ra:       Right Ascension in decimal degrees (0–360).
    dec:      Declination in decimal degrees (−90 to +90).
    fov:      Field of view in arcminutes (default 60).
    maglimit: Faint magnitude limit for comparison stars (default 14.5).

    Returns a text summary that includes an ``Image: /api/files/<name>.png``
    line so the orchestrator can render the chart inline.
    """
    if not star and (ra is None or dec is None):
        raise ValueError(
            "Provide either 'star' (variable star name) or both 'ra' and 'dec'."
        )

    # Build query parameters
    params: dict[str, str | float] = {
        "fov": fov,
        "maglimit": maglimit,
        "format": "json",
    }
    if star:
        # The API expects spaces encoded as '+' in the star name, but httpx
        # will percent-encode the value, which the API also accepts.
        params["star"] = star
    else:
        params["ra"] = ra
        params["dec"] = dec

    logger.info("generate_aavso_map: requesting VSP chart with params=%s", params)

    async with httpx.AsyncClient(timeout=30.0, follow_redirects=True) as client:
        resp = await client.get(
            _VSP_API_URL,
            params=params,
            headers={"Accept": "application/json"},
        )
        if resp.status_code == 400:
            try:
                detail = resp.json()
                errors = detail.get("errors") or [detail.get("detail", "unknown error")]
                raise ValueError(
                    f"AAVSO VSP does not recognise this star: {'; '.join(errors)}. "
                    "Only stars listed in the AAVSO Variable Star Index (VSX) are supported."
                )
            except (KeyError, ValueError):
                raise
        resp.raise_for_status()
        data = resp.json()

    image_uri: str | None = data.get("image_uri")
    if not image_uri:
        raise RuntimeError(
            f"AAVSO VSP API returned no image_uri. Response: {data}"
        )

    chart_id: str = data.get("chartid", "unknown")
    star_name: str = data.get("star", star or f"RA {ra}, Dec {dec}")

    # Download the PNG and store it locally so the orchestrator can serve it.
    logger.info("generate_aavso_map: downloading chart image from %s", image_uri)
    async with httpx.AsyncClient(timeout=30.0, follow_redirects=True) as client:
        img_resp = await client.get(image_uri)
        img_resp.raise_for_status()

    _DOWNLOADS_DIR.mkdir(parents=True, exist_ok=True)
    filename = f"{uuid.uuid4().hex[:8]}_aavso.png"
    filepath = _DOWNLOADS_DIR / filename
    filepath.write_bytes(img_resp.content)
    logger.info("generate_aavso_map: saved chart to %s", filepath)

    local_url = f"/api/files/{filename}"

    lines = [
        f"AAVSO Variable Star Finder Chart: {star_name}",
        f"Chart ID:   {chart_id}",
        f"Field of view: {fov}\u2032",
        f"Magnitude limit: {maglimit}",
        f"Image: {local_url}",
    ]
    return "\n".join(lines)
