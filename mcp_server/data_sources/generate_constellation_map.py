"""
generate_constellation_map — Starplot MapPlot chart centred on a single constellation.

Runs the CPU-bound rendering in a thread executor so it does not
block the async event loop.
"""
from __future__ import annotations

import asyncio
import logging
import os
import uuid
from pathlib import Path

os.environ.setdefault("MPLBACKEND", "Agg")

logger = logging.getLogger(__name__)

_DOWNLOADS_DIR = (
    Path(__file__).resolve().parent.parent.parent / "data" / "downloads"
)

# ---------------------------------------------------------------------------
# Constellation bounds lookup
# (ra_min_h, ra_max_h, dec_min_deg, dec_max_deg)  — RA in hours, Dec in °
# Where ra_min_h > ra_max_h the constellation wraps through 0 h RA; the
# renderer handles this by shifting ra_min_h to a negative value.
# ---------------------------------------------------------------------------
_BOUNDS: dict[str, tuple[float, float, float, float]] = {
    "AND": (22.5, 2.5,  21.0,  53.0),
    "ANT": ( 9.5,11.0, -40.0, -24.0),
    "APS": (13.5,18.0, -83.0, -67.0),
    "AQL": (18.5,20.5, -12.0,  19.0),
    "AQR": (20.5,23.5, -26.0,   4.0),
    "ARA": (16.5,18.0, -67.0, -45.0),
    "ARI": ( 1.7, 3.5,  10.0,  31.0),
    "AUR": ( 4.5, 7.5,  27.0,  57.0),
    "BOO": (13.5,15.8,   7.0,  55.0),
    "CAE": ( 4.2, 5.1, -49.0, -27.0),
    "CAM": ( 3.0,14.5,  52.0,  87.0),
    "CNC": ( 7.8, 9.3,   6.0,  33.0),
    "CVN": (12.0,14.1,  28.0,  53.0),
    "CMA": ( 6.0, 7.5, -33.0, -10.0),
    "CMI": ( 7.1, 8.2,   0.0,  13.0),
    "CAP": (19.8,21.8, -27.0,  -8.0),
    "CAR": ( 6.0,11.2, -75.0, -51.0),
    "CAS": (22.5, 3.5,  46.0,  77.0),   # wraps
    "CEN": (11.0,15.0, -65.0, -28.0),
    "CEP": (20.0, 8.0,  53.0,  88.0),   # wraps
    "CET": (23.0, 3.5, -25.0,  11.0),   # wraps
    "CHA": ( 7.5,13.5, -83.0, -74.0),
    "CIR": (13.5,15.5, -65.0, -55.0),
    "COL": ( 5.0, 6.7, -43.0, -27.0),
    "COM": (11.9,13.5,  13.0,  33.0),
    "CRA": (17.8,19.3, -44.0, -36.0),
    "CRB": (15.2,16.4,  26.0,  40.0),
    "CRV": (11.8,12.9, -25.0, -11.0),
    "CRT": (10.7,12.0, -24.0,  -7.0),
    "CRU": (11.9,12.9, -65.0, -55.0),
    "CYG": (19.0,22.0,  27.0,  62.0),
    "DEL": (20.3,21.2,   2.0,  21.0),
    "DOR": ( 3.8, 5.6, -70.0, -48.0),
    "DRA": ( 9.0,20.0,  51.0,  87.0),
    "EQU": (20.9,21.3,   3.0,  12.0),
    "ERI": ( 1.4, 5.1, -58.0,   0.0),
    "FOR": ( 1.8, 4.0, -37.0, -23.0),
    "GEM": ( 6.0, 8.1,  10.0,  35.0),
    "GRU": (21.5,23.5, -57.0, -36.0),
    "HER": (15.8,18.8,   3.0,  52.0),
    "HOR": ( 2.5, 4.3, -67.0, -40.0),
    "HYA": ( 8.0,15.0, -36.0,   7.0),
    "HYI": ( 0.0, 4.5, -82.0, -57.0),
    "IND": (20.5,23.5, -75.0, -44.0),
    "LAC": (21.8,22.9,  35.0,  57.0),
    "LEO": ( 9.5,11.9,  -6.0,  34.0),
    "LMI": ( 9.6,11.1,  23.0,  41.0),
    "LEP": ( 4.8, 6.2, -27.0, -11.0),
    "LIB": (14.2,16.1, -30.0,   1.0),
    "LUP": (14.2,16.1, -55.0, -33.0),
    "LYN": ( 6.2, 9.8,  33.0,  62.0),
    "LYR": (18.1,19.5,  25.0,  48.0),
    "MEN": ( 3.0, 8.0, -85.0, -65.0),
    "MIC": (20.4,21.4, -45.0, -27.0),
    "MON": ( 5.9, 8.2, -11.0,  12.0),
    "MUS": (11.3,13.8, -75.0, -60.0),
    "NOR": (15.2,16.6, -60.0, -42.0),
    "OCT": ( 0.0,24.0, -90.0, -75.0),
    "OPH": (16.0,18.7, -30.0,  15.0),
    "ORI": ( 4.5, 6.4, -11.0,  23.0),
    "PAV": (17.5,21.5, -75.0, -56.0),
    "PEG": (21.0, 0.3,   3.0,  36.0),   # wraps
    "PER": ( 1.5, 4.8,  31.0,  59.0),
    "PHE": (23.0, 2.5, -57.0, -39.0),   # wraps
    "PIC": ( 4.5, 6.9, -65.0, -43.0),
    "PSC": (22.5, 2.5,  -7.0,  34.0),   # wraps
    "PSA": (21.5,23.1, -36.0, -25.0),
    "PUP": ( 6.0, 8.3, -51.0, -11.0),
    "PYX": ( 8.5, 9.3, -38.0, -17.0),
    "RET": ( 3.4, 4.7, -67.0, -52.0),
    "SGE": (18.9,20.4,  16.0,  22.0),
    "SGR": (17.5,20.5, -45.0, -12.0),
    "SCO": (15.8,18.1, -45.0,  -8.0),
    "SCL": (23.0, 2.0, -39.0, -24.0),   # wraps
    "SCT": (18.3,18.9, -16.0,  -3.0),
    "SER": (15.2,18.9, -16.0,  25.0),
    "SEX": ( 9.7,10.9, -11.0,   7.0),
    "TAU": ( 3.3, 6.0,  -1.0,  31.0),
    "TEL": (18.0,20.4, -57.0, -45.0),
    "TRI": ( 1.5, 2.8,  25.0,  37.0),
    "TRA": (15.1,16.9, -70.0, -60.0),
    "TUC": (22.0, 1.5, -75.0, -56.0),   # wraps
    "UMA": ( 8.5,14.5,  29.0,  73.0),
    "UMI": ( 0.0,24.0,  65.0,  90.0),
    "VEL": ( 8.0,11.5, -57.0, -37.0),
    "VIR": (11.5,15.2, -22.0,  14.0),
    "VOL": ( 6.6, 9.1, -73.0, -60.0),
    "VUL": (18.9,21.5,  19.0,  29.0),
}

_NAME_TO_ABBREV: dict[str, str] = {
    "andromeda": "AND", "antlia": "ANT", "apus": "APS", "aquila": "AQL",
    "aquarius": "AQR", "ara": "ARA", "aries": "ARI", "auriga": "AUR",
    "bootes": "BOO", "boötes": "BOO", "caelum": "CAE", "camelopardalis": "CAM",
    "cancer": "CNC", "canes venatici": "CVN", "canis major": "CMA",
    "canis minor": "CMI", "capricornus": "CAP", "carina": "CAR",
    "cassiopeia": "CAS", "centaurus": "CEN", "cepheus": "CEP", "cetus": "CET",
    "chamaeleon": "CHA", "circinus": "CIR", "columba": "COL",
    "coma berenices": "COM", "corona australis": "CRA", "corona borealis": "CRB",
    "corvus": "CRV", "crater": "CRT", "crux": "CRU", "cygnus": "CYG",
    "delphinus": "DEL", "dorado": "DOR", "draco": "DRA", "equuleus": "EQU",
    "eridanus": "ERI", "fornax": "FOR", "gemini": "GEM", "grus": "GRU",
    "hercules": "HER", "horologium": "HOR", "hydra": "HYA", "hydrus": "HYI",
    "indus": "IND", "lacerta": "LAC", "leo": "LEO", "leo minor": "LMI",
    "lepus": "LEP", "libra": "LIB", "lupus": "LUP", "lynx": "LYN",
    "lyra": "LYR", "mensa": "MEN", "microscopium": "MIC", "monoceros": "MON",
    "musca": "MUS", "norma": "NOR", "octans": "OCT", "ophiuchus": "OPH",
    "orion": "ORI", "pavo": "PAV", "pegasus": "PEG", "perseus": "PER",
    "phoenix": "PHE", "pictor": "PIC", "pisces": "PSC",
    "piscis austrinus": "PSA", "piscis austrina": "PSA",
    "puppis": "PUP", "pyxis": "PYX", "reticulum": "RET",
    "sagitta": "SGE", "sagittarius": "SGR",
    "scorpius": "SCO", "scorpio": "SCO", "sculptor": "SCL", "scutum": "SCT",
    "serpens": "SER", "sextans": "SEX", "taurus": "TAU", "telescopium": "TEL",
    "triangulum": "TRI", "triangulum australe": "TRA", "tucana": "TUC",
    "ursa major": "UMA", "ursa minor": "UMI", "vela": "VEL", "virgo": "VIR",
    "volans": "VOL", "vulpecula": "VUL",
}


def _resolve_constellation(name: str) -> tuple[str, str]:
    """
    Accept a full name, common variant, or IAU abbreviation.
    Returns (abbrev, display_name).  Raises ValueError if unknown.
    """
    stripped = name.strip()
    upper = stripped.upper()

    # Direct IAU abbreviation match
    if upper in _BOUNDS:
        display = stripped.title()
        return upper, display

    # Full / common name match
    lower = stripped.lower()
    if lower in _NAME_TO_ABBREV:
        abbrev = _NAME_TO_ABBREV[lower]
        return abbrev, stripped.title()

    # Partial / fuzzy: try startswith on full names
    for key, abbrev in _NAME_TO_ABBREV.items():
        if lower.startswith(key) or key.startswith(lower):
            return abbrev, key.title()

    known = ", ".join(sorted(_NAME_TO_ABBREV.keys()))
    raise ValueError(
        f"Unknown constellation '{name}'. Known constellations: {known}"
    )


def _render_constellation_map(
    filepath: str,
    ra_min_deg: float,
    ra_max_deg: float,
    dec_min: float,
    dec_max: float,
) -> None:
    """CPU-bound MapPlot rendering — executed in a thread-pool worker."""
    import matplotlib
    matplotlib.use("Agg")

    from starplot import MapPlot, _
    from starplot.projections import Mercator
    from starplot.styles import PlotStyle, extensions

    style = PlotStyle().extend(
        extensions.BLUE_LIGHT,
        extensions.MAP,
    )

    p = MapPlot(
        projection=Mercator(),
        ra_min=ra_min_deg,
        ra_max=ra_max_deg,
        dec_min=dec_min,
        dec_max=dec_max,
        style=style,
        resolution=2048,
        autoscale=True,
    )

    p.gridlines()
    p.constellations()
    p.stars(
        where=[_.magnitude < 8],
        where_labels=[_.magnitude < 4],
        bayer_labels=True,
    )
    p.nebula(where=[(_.magnitude < 9) | (_.magnitude.isnull())], where_labels=[False])
    p.open_clusters(
        where=[(_.magnitude < 9) | (_.magnitude.isnull())], where_labels=[False]
    )
    p.milky_way()
    p.constellation_labels()

    p.export(filepath, padding=0.2)
    p.close_fig()


async def generate_constellation_map(constellation: str) -> str:
    """
    Generate a detailed star chart centred on the requested constellation.

    Returns a short description with an image URL in the form
    ``Image: /api/files/<filename>.png`` that the orchestrator can detect.
    """
    abbrev, display_name = _resolve_constellation(constellation)
    ra_min_h, ra_max_h, dec_min, dec_max = _BOUNDS[abbrev]

    # Handle constellations that wrap through 0 h RA by shifting ra_min negative.
    if ra_min_h > ra_max_h:
        ra_min_deg = (ra_min_h - 24.0) * 15.0
    else:
        ra_min_deg = ra_min_h * 15.0
    ra_max_deg = ra_max_h * 15.0

    _DOWNLOADS_DIR.mkdir(parents=True, exist_ok=True)
    filename = f"{uuid.uuid4().hex[:8]}_{abbrev.lower()}_map.png"
    filepath = str(_DOWNLOADS_DIR / filename)
    url = f"/api/files/{filename}"

    logger.info("generate_constellation_map: saving to %s", filepath)

    loop = asyncio.get_event_loop()
    await loop.run_in_executor(
        None, _render_constellation_map, filepath, ra_min_deg, ra_max_deg, dec_min, dec_max
    )

    if not Path(filepath).is_file():
        raise RuntimeError(
            f"Constellation map rendering failed — file not created: {filepath}"
        )

    return (
        f"Constellation map generated for {display_name} ({abbrev}).\n"
        f"Image: {url}"
    )
