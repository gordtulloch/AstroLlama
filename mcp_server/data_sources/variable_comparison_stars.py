"""
variable_comparison_stars — Fetch comparison star photometry from the AAVSO VSP API.

Uses the same endpoint as generate_aavso_map but returns the photometry table
rather than the chart image, giving the user (and LLM) a structured list of
comparison stars with their magnitudes in multiple photometric bands.

API reference: https://app.aavso.org/vsp/api/chart/
"""
from __future__ import annotations

import logging

import httpx

logger = logging.getLogger(__name__)

_VSP_API_URL = "https://app.aavso.org/vsp/api/chart/"

# Bands to show in the summary table (ordered by usefulness to visual observers)
_PREFERRED_BANDS = ["V", "B", "Rc", "Ic", "J", "H", "K", "U"]


async def variable_comparison_stars(
    star: str | None = None,
    ra: float | None = None,
    dec: float | None = None,
    fov: float = 60.0,
    maglimit: float = 14.5,
) -> str:
    """
    Retrieve comparison star photometry for a variable star from the AAVSO VSP API.

    Provide *star* **or** both *ra* and *dec* to identify the target field.

    Parameters
    ----------
    star:     Name of the variable star (e.g. "SS Cyg", "Mira", "RR Lyr").
    ra:       Right Ascension in decimal degrees (0–360).
    dec:      Declination in decimal degrees (−90 to +90).
    fov:      Field of view in arcminutes (default 60).
    maglimit: Faintest magnitude to include (default 14.5).

    Returns a formatted table of comparison stars with their AUID, coordinates,
    chart label, and magnitudes in available photometric bands.
    """
    if not star and (ra is None or dec is None):
        raise ValueError(
            "Provide either 'star' (variable star name) or both 'ra' and 'dec'."
        )

    params: dict[str, str | float] = {
        "fov": fov,
        "maglimit": maglimit,
        "format": "json",
    }
    if star:
        params["star"] = star
    else:
        params["ra"] = ra
        params["dec"] = dec

    logger.info("variable_comparison_stars: querying VSP with params=%s", params)

    async with httpx.AsyncClient(timeout=30.0, follow_redirects=True) as client:
        resp = await client.get(
            _VSP_API_URL,
            params=params,
            headers={"Accept": "application/json"},
        )
        if resp.status_code == 400:
            detail = resp.json()
            errors = detail.get("errors") or [detail.get("detail", "unknown error")]
            raise ValueError(
                f"AAVSO VSP does not recognise this star: {'; '.join(errors)}. "
                "Only stars in the AAVSO Variable Star Index (VSX) are supported."
            )
        resp.raise_for_status()
        data = resp.json()

    chart_id: str = data.get("chartid", "unknown")
    star_name: str = data.get("star", star or f"RA {ra}, Dec {dec}")
    photometry: list = data.get("photometry") or []

    if not photometry:
        return (
            f"No comparison stars found for {star_name} "
            f"(chart {chart_id}, FOV {fov}′, maglimit {maglimit}).\n"
            "You can request a comparison sequence from the AAVSO Sequence Team "
            "at compstars@aavso.org, providing the star name, coordinates, "
            "magnitude range, and variable type."
        )

    # Collect which bands are actually present across all comparison stars
    present_bands: list[str] = []
    for band in _PREFERRED_BANDS:
        for comp in photometry:
            if any(b["band"] == band for b in (comp.get("bands") or [])):
                present_bands.append(band)
                break

    # Build header
    target_label = star_name
    lines = [
        f"AAVSO Comparison Stars for {target_label}",
        f"{'=' * (27 + len(target_label))}",
        f"Chart ID:  {chart_id}",
        f"Field of view: {fov}\u2032   Magnitude limit: {maglimit}",
        f"Comparison stars found: {len(photometry)}",
        "",
    ]

    # Column header
    band_header = "  ".join(f"{b:>6}" for b in present_bands)
    lines.append(f"{'Label':<6}  {'AUID':<14}  {'RA (J2000)':<13}  {'Dec (J2000)':<12}  {band_header}")
    lines.append("-" * (6 + 2 + 14 + 2 + 13 + 2 + 12 + 2 + 8 * len(present_bands)))

    for comp in sorted(photometry, key=lambda c: _sort_key(c)):
        label = comp.get("label", "?")
        auid  = comp.get("auid", "")
        ra_s  = comp.get("ra", "")
        dec_s = comp.get("dec", "")

        band_map: dict[str, str] = {}
        for b in (comp.get("bands") or []):
            mag   = b.get("mag")
            err   = b.get("error")
            if mag is None:
                band_map[b["band"]] = "  —  "
            elif err:
                band_map[b["band"]] = f"{mag:.3f}"
            else:
                band_map[b["band"]] = f"{mag:.3f}"

        band_cols = "  ".join(f"{band_map.get(b, '  —  '):>6}" for b in present_bands)
        lines.append(f"{label:<6}  {auid:<14}  {ra_s:<13}  {dec_s:<12}  {band_cols}")

    lines += [
        "",
        "Notes:",
        "  • Label = magnitude label printed on the AAVSO finder chart (V-band × 10, no decimal).",
        "  • Bands: V=Visual, B=Blue, Rc=Red-Cousins, Ic=Infrared-Cousins, J/H/K=near-IR.",
        f"  • Source: AAVSO Variable Star Plotter (app.aavso.org/vsp) — Chart {chart_id}",
    ]

    return "\n".join(lines)


def _sort_key(comp: dict) -> float:
    """Sort comparison stars by V magnitude, falling back to label value."""
    for b in (comp.get("bands") or []):
        if b.get("band") == "V" and b.get("mag") is not None:
            return float(b["mag"])
    try:
        return float(comp.get("label", "999")) / 10.0
    except (TypeError, ValueError):
        return 999.0
