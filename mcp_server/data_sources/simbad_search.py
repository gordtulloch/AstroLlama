"""
SIMBAD Search Data Source

Builds structured ADQL / astroquery calls against SIMBAD TAP.
Natural-language parsing is done locally; only structured queries are sent.
Results are formatted for non-scientific users.

Verified against SIMBAD TAP schema (astroquery 0.4.11):
  - allfluxes: columns V, B, G, H, … one row per object
  - ids:       columns oidref, ids  (pipe-separated identifier string)
  - basic:     columns main_id, otype, otype_txt, ra, dec, oid …
               NOTE: no 'con' (constellation) column in this schema
  - Constellation queries use query_region() + astropy boundary check
"""

import functools
import logging
import re
import ssl
from typing import Optional

logger = logging.getLogger(__name__)

# Apply SSL patch immediately at module import time so that pyvo / astroquery
# can reach SIMBAD on systems where the local CA bundle is incomplete.
# (pyvo uses http.client which reads the default SSL context at connection time,
#  so this must be set before any connections are made.)
ssl._create_default_https_context = ssl._create_unverified_context  # noqa: SLF001
try:
    import urllib3
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
except Exception:
    pass


# ── Object-type translation table (SIMBAD short otype → friendly label) ──────
_OTYPE_LABELS = {
    "*": "Star",
    "**": "Double Star",
    "SB*": "Spectroscopic Binary Star",
    "V*": "Variable Star",
    "dS*": "Delta Scuti Variable Star",
    "RRLyr": "RR Lyrae Variable Star",
    "Cepheid": "Cepheid Variable Star",
    "Nova": "Nova",
    "SN": "Supernova",
    "Pulsar": "Pulsar",
    "WD*": "White Dwarf",
    "NS": "Neutron Star",
    "BH": "Black Hole",
    "sg*": "Supergiant Star",
    "s*b": "Blue Supergiant Star",
    "s*r": "Red Supergiant Star",
    "s*y": "Yellow Supergiant Star",
    "RG*": "Red Giant Star",
    "HB*": "Horizontal Branch Star",
    "Em*": "Emission-Line Star",
    "Be*": "Be Star",
    "Ae*": "Herbig Ae/Be Star",
    "TTau*": "T Tauri Star",
    "YSO": "Young Stellar Object",
    "PM*": "High Proper-Motion Star",
    "AB*": "Asymptotic Giant Branch Star",
    "LP*": "Long-Period Variable Star",
    "Mi*": "Mira Variable Star",
    "OH*": "OH/IR Star",
    "X": "X-ray Source",
    "HII": "Emission Nebula (HII Region)",
    "PN": "Planetary Nebula",
    "pPN": "Pre-Planetary Nebula",
    "SNR": "Supernova Remnant",
    "RNe": "Reflection Nebula",
    "DNe": "Dark Nebula",
    "GNe": "Gaseous Nebula",
    "MoC": "Molecular Cloud",
    "CGb": "Cometary Globule",
    "SFR": "Star-Forming Region",
    "GlC": "Globular Cluster",
    "OC": "Open Cluster",
    "As*": "Stellar Association",
    "G": "Galaxy",
    "GiG": "Galaxy in Group",
    "GiC": "Galaxy in Cluster",
    "Sy1": "Seyfert 1 Galaxy",
    "Sy2": "Seyfert 2 Galaxy",
    "AGN": "Active Galactic Nucleus",
    "QSO": "Quasar",
    "BLL": "BL Lac Object",
    "GrG": "Group of Galaxies",
    "ClG": "Cluster of Galaxies",
    "GiP": "Galaxy in Pair",
    "GiF": "Galaxy in Filament",
    "LINER": "LINER Galaxy",
    "LIN": "LINER Galaxy",
    "EmG": "Emission-Line Galaxy",
    "SBG": "Starburst Galaxy",
    "bCG": "Blue Compact Galaxy",
    "H2G": "HII Galaxy",
    "LSB": "Low Surface Brightness Galaxy",
    "rG": "Radio Galaxy",
    "Bla": "Blazar",
    "SyG": "Seyfert Galaxy",
}

# Groups of related otypes (for broadened NL queries)
_OTYPE_GROUPS = {
    "*":   ("*", "**", "SB*", "V*", "PM*", "RG*", "sg*", "s*b", "s*r", "s*y",
            "HB*", "Em*", "Be*", "Ae*", "TTau*", "AB*", "LP*", "Mi*", "OH*",
            "WD*", "NS", "Pulsar", "BH", "Nova", "dS*", "RRLyr", "Cepheid"),
    "V*":  ("V*", "Mi*", "RRLyr", "Cepheid", "dS*", "LP*", "EB*", "RS*",
            "BY*", "Nova", "Be*", "SR*", "CW*", "RR*", "a2*"),
    "G":   ("G", "GiG", "GiC", "GiP", "GiF", "Sy1", "Sy2", "AGN", "LINER",
            "LIN", "BLL", "QSO", "Bla", "EmG", "SBG", "bCG", "H2G", "LSB", "rG"),
    "HII": ("HII",),
    "PN":  ("PN", "pPN", "RNe"),
    "GlC": ("GlC",),
    "OC":  ("OC",),
    "GNe": ("GNe", "HII", "RNe", "DNe", "MoC", "SFR", "CGb"),
}
_NON_STELLAR_OTYPES = {
    "G", "GiG", "GiC", "LIN", "Sy1", "Sy2", "AGN", "QSO", "BLL", "BiC",
    "GrG", "ClG", "CIG",
    "PN", "HII", "RNe", "DNe", "GNe", "SNR", "MoC", "CGb", "SFR", "Cld",
    "GlC", "OC", "As*",
    "X", "Rad", "IR", "UV", "gam", "ISM",
}

# ── Common names for well-known objects ──────────────────────────────────────
_COMMON_NAMES: dict[str, str] = {
    "* alf CMa": "Sirius",          "* alf Car": "Canopus",
    "* alf Cen": "Alpha Centauri",   "* alf Cen A": "Alpha Centauri A",
    "* alf Boo": "Arcturus",         "* alf Lyr": "Vega",
    "* alf Aur": "Capella",          "* bet Ori": "Rigel",
    "* alf CMi": "Procyon",          "* alf Eri": "Achernar",
    "* bet Cen": "Hadar",            "* alf Aql": "Altair",
    "* alf Tau": "Aldebaran",        "* alf Vir": "Spica",
    "* alf Sco": "Antares",          "* alf PsA": "Fomalhaut",
    "* bet Gem": "Pollux",           "* alf Leo": "Regulus",
    "* alf Cru": "Acrux",            "* alf Gem": "Castor",
    "* eps CMa": "Adhara",           "* eta Car": "Eta Carinae",
    "* bet Cru": "Mimosa",           "* eps Ori": "Alnilam",
    "* zet Ori": "Alnitak",          "* del Ori": "Mintaka",
    "* alf Ori": "Betelgeuse",        "* gam Ori": "Bellatrix",
    "* kap Ori": "Saiph",            "* alf Per": "Mirfak",
    "* bet Per": "Algol",            "* alf UMa": "Dubhe",
    "* bet UMa": "Merak",            "* eta UMa": "Alkaid",
    "* eps UMa": "Alioth",           "* zet UMa": "Mizar",
    "* gam UMa": "Phecda",           "* alf UMi": "Polaris",
    "* alf Cyg": "Deneb",            "* alf Pav": "Peacock",
    "* alf Sco A": "Antares A",      "* del CMa": "Wezen",
    "* bet CMa": "Mirzam",           "* gam Vel": "Regor",
    "M  1": "Crab Nebula",           "M  8": "Lagoon Nebula",
    "M 16": "Eagle Nebula",          "M 17": "Omega Nebula",
    "M 20": "Trifid Nebula",         "M 27": "Dumbbell Nebula",
    "M 31": "Andromeda Galaxy",      "M 33": "Triangulum Galaxy",
    "M 42": "Orion Nebula",          "M 43": "De Mairan's Nebula",
    "M 44": "Beehive Cluster",       "M 45": "Pleiades",
    "M 51": "Whirlpool Galaxy",       "M 57": "Ring Nebula",
    "M 64": "Black Eye Galaxy",      "M 78": "Messier 78",
    "M 97": "Owl Nebula",            "M101": "Pinwheel Galaxy",
    "M104": "Sombrero Galaxy",
    "NGC 7293": "Helix Nebula",       "NGC 6720": "Ring Nebula",
    "NGC 2392": "Eskimo Nebula",      "NGC 3372": "Eta Carinae Nebula",
    "NGC 6618": "Omega Nebula",       "NGC 1976": "Orion Nebula",
    "NGC 1499": "California Nebula",  "NGC 7000": "North America Nebula",
    "NGC 2237": "Rosette Nebula",     "NGC 6523": "Lagoon Nebula",
    "NGC 6611": "Eagle Nebula",
}

# ── Constellation name / abbreviation lookup ─────────────────────────────────
from .astroquery_universal import _CONSTELLATION_ABBREVS, _constellation_center

_ABBREV_TO_FULL = {
    'AND': 'Andromeda', 'ANT': 'Antlia', 'APS': 'Apus', 'AQR': 'Aquarius',
    'AQL': 'Aquila', 'ARA': 'Ara', 'ARI': 'Aries', 'AUR': 'Auriga',
    'BOO': 'Boötes', 'CAE': 'Caelum', 'CAM': 'Camelopardalis', 'CNC': 'Cancer',
    'CVN': 'Canes Venatici', 'CMA': 'Canis Major', 'CMI': 'Canis Minor',
    'CAP': 'Capricornus', 'CAR': 'Carina', 'CAS': 'Cassiopeia',
    'CEN': 'Centaurus', 'CEP': 'Cepheus', 'CET': 'Cetus', 'CHA': 'Chamaeleon',
    'CIR': 'Circinus', 'COL': 'Columba', 'COM': 'Coma Berenices',
    'CRA': 'Corona Australis', 'CRB': 'Corona Borealis', 'CRV': 'Corvus',
    'CRT': 'Crater', 'CRU': 'Crux', 'CYG': 'Cygnus', 'DEL': 'Delphinus',
    'DOR': 'Dorado', 'DRA': 'Draco', 'EQU': 'Equuleus', 'ERI': 'Eridanus',
    'FOR': 'Fornax', 'GEM': 'Gemini', 'GRU': 'Grus', 'HER': 'Hercules',
    'HOR': 'Horologium', 'HYA': 'Hydra', 'HYI': 'Hydrus', 'IND': 'Indus',
    'LAC': 'Lacerta', 'LEO': 'Leo', 'LMI': 'Leo Minor', 'LEP': 'Lepus',
    'LIB': 'Libra', 'LUP': 'Lupus', 'LYN': 'Lynx', 'LYR': 'Lyra',
    'MEN': 'Mensa', 'MIC': 'Microscopium', 'MON': 'Monoceros', 'MUS': 'Musca',
    'NOR': 'Norma', 'OCT': 'Octans', 'OPH': 'Ophiuchus', 'ORI': 'Orion',
    'PAV': 'Pavo', 'PEG': 'Pegasus', 'PER': 'Perseus', 'PHE': 'Phoenix',
    'PIC': 'Pictor', 'PSC': 'Pisces', 'PSA': 'Piscis Austrinus', 'PUP': 'Puppis',
    'PYX': 'Pyxis', 'RET': 'Reticulum', 'SGE': 'Sagitta', 'SGR': 'Sagittarius',
    'SCO': 'Scorpius', 'SCL': 'Sculptor', 'SCT': 'Scutum', 'SER': 'Serpens',
    'SEX': 'Sextans', 'TAU': 'Taurus', 'TEL': 'Telescopium', 'TRI': 'Triangulum',
    'TRA': 'Triangulum Australe', 'TUC': 'Tucana', 'UMA': 'Ursa Major',
    'UMI': 'Ursa Minor', 'VEL': 'Vela', 'VIR': 'Virgo', 'VOL': 'Volans',
    'VUL': 'Vulpecula',
}

# ── NL query → object type mapping ───────────────────────────────────────────
_NL_OTYPE_MAP = {
    "emission nebula": "HII",  "emission nebulae": "HII",
    "hii region": "HII",       "hii regions": "HII",
    "planetary nebula": "PN",  "planetary nebulae": "PN",
    "supernova remnant": "SNR","supernova remnants": "SNR",
    "globular cluster": "GlC", "globular clusters": "GlC",
    "open cluster": "OC",      "open clusters": "OC",
    "galaxy": "G",             "galaxies": "G",
    "quasar": "QSO",           "quasars": "QSO",
    "reflection nebula": "RNe","reflection nebulae": "RNe",
    "dark nebula": "DNe",      "dark nebulae": "DNe",
    "nebula": "GNe",           "nebulae": "GNe",
    "variable star": "V*",     "variable stars": "V*",
    "double star": "**",       "double stars": "**",
    "white dwarf": "WD*",      "white dwarfs": "WD*",
    "neutron star": "NS",      "neutron stars": "NS",
    "pulsar": "Pulsar",        "pulsars": "Pulsar",
    "star": "*",               "stars": "*",
}


# ── Greek letter expansion ───────────────────────────────────────────────────
_GREEK_FULL = {
    "alf": "Alpha", "bet": "Beta", "gam": "Gamma", "del": "Delta",
    "eps": "Epsilon", "zet": "Zeta", "eta": "Eta", "tet": "Theta",
    "iot": "Iota", "kap": "Kappa", "lam": "Lambda", "mu.": "Mu",
    "nu.": "Nu", "ksi": "Xi", "omi": "Omicron", "pi.": "Pi",
    "rho": "Rho", "sig": "Sigma", "tau": "Tau", "ups": "Upsilon",
    "phi": "Phi", "chi": "Chi", "psi": "Psi", "ome": "Omega",
}


def _friendly_otype(otype: str, otype_txt: str = "") -> str:
    otype = (otype or "").strip()
    # Always prefer the human-readable labels dict over otype_txt
    # (SIMBAD's otype_txt field often echoes the same raw code)
    if otype in _OTYPE_LABELS:
        return _OTYPE_LABELS[otype]
    # Fall back to otype_txt if it looks like a real description
    if otype_txt and otype_txt.strip() and otype_txt.strip().lower() not in (
        "nan", "--", "none", "", otype.lower()
    ):
        return otype_txt.strip()
    return otype or "Unknown"


def _pick_common_name(main_id: str, ids_str: str = "") -> str:
    """
    Return the most user-friendly name.
    Priority: hard-coded names → Messier → NAME proper name → NGC/IC → main_id.
    NAME technical sub-identifiers (e.g. 'UMa A', 'OrionBar D2') are skipped
    in favour of Messier/NGC designations.
    """
    clean = main_id.strip()
    # Normalize internal whitespace so SIMBAD's fixed-width format
    # (e.g. 'M  42', 'NGC  1976') matches the dict keys ('M 42', 'NGC 1976').
    clean_norm = re.sub(r'\s+', ' ', clean)

    # 1. Hard-coded common names (highest priority)
    if clean in _COMMON_NAMES:
        return _COMMON_NAMES[clean]
    if clean_norm in _COMMON_NAMES:
        return _COMMON_NAMES[clean_norm]

    ids = [s.strip() for s in (ids_str or "").split("|") if s.strip()]

    # Check hard-coded names against all identifiers too
    for iid in ids:
        if iid in _COMMON_NAMES:
            return _COMMON_NAMES[iid]
        iid_norm = re.sub(r'\s+', ' ', iid)
        if iid_norm in _COMMON_NAMES:
            return _COMMON_NAMES[iid_norm]

    # 2. Messier number — very recognisable, prefer over generic NAME labels
    for iid in [clean] + ids:
        m = re.match(r"^M\s+(\d+)$", iid, re.IGNORECASE)
        if m:
            return f"M{m.group(1)}"

    # 3. NAME prefix — but only for genuine proper names, not technical labels
    #    like 'UMa A', 'OrionBar D2', 'Cl* NGC 1039 ...', 'Cl NGC 2244 II' etc.
    _SKIP_NAME = re.compile(
        r'^(Cl[\s*]|\[|[A-Z]{1,4}\s+[A-Z]\b|[A-Z]{2,5}\s+[IV]+\b)',
        re.IGNORECASE
    )
    for iid in ids:
        if iid.upper().startswith("NAME "):
            name_val = iid[5:].strip()
            if not _SKIP_NAME.match(name_val):
                return name_val

    if clean.upper().startswith("NAME "):
        name_val = clean[5:].strip()
        if not _SKIP_NAME.match(name_val):
            return name_val

    # 4. NGC / IC
    for iid in [clean] + ids:
        if re.match(r"^NGC\s*\d+", iid, re.IGNORECASE):
            return iid
        if re.match(r"^IC\s*\d+", iid, re.IGNORECASE):
            return iid

    # 5. Prettify Bayer star designation  "* alf Ori" → "Alpha Orionis"
    m = re.match(r"^\*\s+([a-z]{2,3})\s+([A-Za-z]+)$", clean)
    if m:
        greek = _GREEK_FULL.get(m.group(1).lower())
        if greek:
            return f"{greek} {m.group(2)}"

    # 6. Strip leading "* " marker
    if clean.startswith("* "):
        return clean[2:]
    return clean


def _mag_description(vmag) -> str:
    try:
        v = float(vmag)
    except (TypeError, ValueError):
        return ""
    if v <= 0:
        return f"magnitude {v:.1f} — one of the very brightest objects in the sky"
    elif v <= 1.5:
        return f"magnitude {v:.1f} — extremely bright, easily visible"
    elif v <= 3.0:
        return f"magnitude {v:.1f} — bright, visible to the naked eye"
    elif v <= 5.0:
        return f"magnitude {v:.1f} — visible to the naked eye under dark skies"
    elif v <= 7.0:
        return f"magnitude {v:.1f} — faintly visible; easy in binoculars"
    elif v <= 10.0:
        return f"magnitude {v:.1f} — requires binoculars"
    else:
        return f"magnitude {v:.1f} — requires a telescope"


def _safe_str(val) -> Optional[str]:
    """Return string value of a table cell, or None if masked/empty."""
    try:
        if hasattr(val, 'mask') and val.mask:
            return None
    except (TypeError, ValueError):
        pass
    s = str(val).strip()
    return s if s and s.lower() not in ("nan", "--", "none", "") else None


# ── Natural-language query parser ─────────────────────────────────────────────
def _parse_natural_language(query: str, limit: int):
    """
    Parse a natural-language query string into structured parameters.
    Returns (mode, otype_filter, constellation_abbrev, limit)
    where mode is: 'brightest_stars' | 'constellation_objects' | 'general'
    """
    q = query.lower().strip()

    # Extract explicit number from the query
    m = re.search(r'\b(\d+)\b', q)
    if m:
        limit = min(int(m.group(1)), 100)

    # Detect object type — regex first for variable stars (handles typos /
    # plural variations like "variables stars", "variable", "variables"),
    # then fall back to longest-phrase-first dict scan.
    detected_otype = None
    if re.search(r'\bvariables?\b', q):
        detected_otype = "V*"
    if detected_otype is None:
        for phrase, otype in sorted(_NL_OTYPE_MAP.items(), key=lambda x: -len(x[0])):
            if phrase in q:
                detected_otype = otype
                break

    # Detect constellation — sort by name length descending so that longer
    # names (e.g. 'sagittarius') match before shorter substrings ('sagitta').
    # Constellation detection must happen BEFORE the brightest-stars shortcut
    # so that "10 brightest stars in Ursa Major" routes to constellation_objects
    # rather than the global brightest-stars query.
    detected_constellation = None
    for name, abbrev in sorted(_CONSTELLATION_ABBREVS.items(), key=lambda x: -len(x[0])):
        if len(name) >= 4 and name in q:
            detected_constellation = abbrev
            break

    # Detect hemisphere
    hemisphere = None
    if re.search(r'north(?:ern)?\s+(?:sky|hemisphere)', q):
        hemisphere = "north"
    elif re.search(r'south(?:ern)?\s+(?:sky|hemisphere)', q):
        hemisphere = "south"

    if detected_constellation:
        # If query says "bright* star*" but no otype was caught, default to stars
        if detected_otype is None and re.search(r'bright\w*\s+star', q):
            detected_otype = "*"
        return ("constellation_objects", detected_otype, detected_constellation, limit, hemisphere)

    # No constellation — global brightest stars shortcut
    if re.search(r'bright\w*\s+star', q):
        return ("brightest_stars", detected_otype, None, limit, hemisphere)

    return ("general", detected_otype, None, limit, hemisphere)


# ── SIMBAD queries ────────────────────────────────────────────────────────────

def _query_brightest_stars(limit: int, otype_filter=None, hemisphere=None):
    """TAP: brightest stellar objects by V magnitude. Returns astropy Table."""
    from astroquery.simbad import Simbad

    # Over-fetch to allow filtering out non-stellar objects (galaxies etc.)
    fetch = min(limit * 5, 200)

    # Relax magnitude ceiling for typed queries (e.g. variable stars span
    # a wide range; the default 4.0 would exclude most of them).
    mag_ceiling = 4.0 if not otype_filter else 8.0

    # Build otype WHERE clause
    if otype_filter and otype_filter in _OTYPE_GROUPS:
        group_types = _OTYPE_GROUPS[otype_filter]
        quoted = ", ".join(f"'{t}'" for t in group_types)
        otype_clause = f"AND otype IN ({quoted})"
    elif otype_filter and otype_filter != "*":
        otype_clause = f"AND otype = '{otype_filter}'"
    else:
        otype_clause = ""

    # Hemisphere filter
    hem_clause = ""
    if hemisphere == "north":
        hem_clause = "AND dec > 0"
    elif hemisphere == "south":
        hem_clause = "AND dec < 0"

    adql = f"""SELECT TOP {fetch}
    main_id, otype, otype_txt, ra, dec, V AS vmag, ids
FROM basic
JOIN allfluxes ON allfluxes.oidref = basic.oid
JOIN ids ON ids.oidref = basic.oid
WHERE V IS NOT NULL AND V < {mag_ceiling}
{otype_clause}
{hem_clause}
ORDER BY vmag ASC"""

    table = Simbad.query_tap(adql)
    if table is None:
        return None

    # Post-filter non-stellar objects only when no specific otype was requested
    rows = []
    for row in table:
        otype = _safe_str(row['otype']) or ""
        if otype_filter or otype not in _NON_STELLAR_OTYPES:
            rows.append(row)
        if len(rows) >= limit:
            break
    return rows


@functools.lru_cache(maxsize=1)
def _skyfield_constellation_map():
    """Load and cache the skyfield constellation boundary map."""
    from skyfield.api import load_constellation_map
    return load_constellation_map()


def _in_constellation(ra_deg: float, dec_deg: float, iau_upper: str) -> bool:
    """Return True if (ra_deg, dec_deg) lies inside the IAU boundary of iau_upper.

    skyfield returns mixed-case abbreviations (e.g. 'UMa') so we normalise
    both sides to uppercase before comparing.
    """
    from skyfield.api import position_of_radec
    constellation_at = _skyfield_constellation_map()
    pos = position_of_radec(ra_deg / 15.0, dec_deg)  # RA must be in hours
    return constellation_at(pos).upper() == iau_upper.upper()


def _query_constellation_objects(constellation: str, otype_filter: Optional[str], limit: int):
    """
    Query objects in a constellation.

    Strategy:
      1. Use a wide bounding circle around the constellation centre to pull
         candidates from SIMBAD (fast, server-side).
      2. Post-filter every candidate with skyfield IAU boundary polygons so
         that only objects genuinely inside the constellation are kept.
    """
    from astroquery.simbad import Simbad

    center_coords = _constellation_center(constellation)
    if center_coords is None:
        raise ValueError(f"Cannot compute centre for constellation '{constellation}'")

    ra_c, dec_c = center_coords

    # Build the otype WHERE clause.
    # Stars: SIMBAD has dozens of stellar subtypes (a2*, PM*, **, Em*, BY*, RS*,
    # Pe*, etc.) — an inclusion list can never be complete.  Use NOT IN with the
    # known non-stellar types instead, which is far more robust.
    if otype_filter == "*":
        non_stellar_quoted = ", ".join(f"'{t}'" for t in sorted(_NON_STELLAR_OTYPES))
        otype_clause = f"AND otype NOT IN ({non_stellar_quoted})"
    elif otype_filter and otype_filter in _OTYPE_GROUPS:
        group_types = _OTYPE_GROUPS[otype_filter]
        if len(group_types) == 1:
            otype_clause = f"AND otype = '{group_types[0]}'"
        else:
            quoted = ", ".join(f"'{t}'" for t in group_types)
            otype_clause = f"AND otype IN ({quoted})"
    elif otype_filter:
        otype_clause = f"AND otype = '{otype_filter}'"
    else:
        otype_clause = ""

    # Fetch generously — we'll trim to the true boundary after.
    # 35° covers even the largest constellations (Hydra spans ~100° but is long
    # and thin; its centre is close enough that 35° catches most objects).
    fetch_limit = min(limit * 30, 1000)
    adql = f"""SELECT TOP {fetch_limit}
    main_id, otype, otype_txt, ra, dec, V AS vmag, ids
FROM basic
LEFT JOIN allfluxes ON allfluxes.oidref = basic.oid
JOIN ids ON ids.oidref = basic.oid
WHERE CONTAINS(POINT('ICRS', ra, dec), CIRCLE('ICRS', {ra_c:.4f}, {dec_c:.4f}, 35)) = 1
{otype_clause}
ORDER BY vmag ASC"""

    logger.debug("Constellation ADQL:\n%s", adql)
    table = Simbad.query_tap(adql)
    all_rows = list(table) if table is not None else []
    logger.debug("Candidate rows before boundary filter: %d", len(all_rows))

    # Post-filter: keep only rows whose coordinates fall inside the true
    # IAU constellation boundary (skyfield).
    filtered: list = []
    for row in all_rows:
        try:
            ra_val  = float(row['ra'])
            dec_val = float(row['dec'])
        except (TypeError, ValueError):
            continue
        if _in_constellation(ra_val, dec_val, constellation):
            filtered.append(row)

    logger.debug("Rows after boundary filter: %d", len(filtered))
    known_rows  = [r for r in filtered if re.match(r'^(NAME |M |NGC |IC )', _safe_str(r['main_id']) or "", re.IGNORECASE)]
    other_rows  = [r for r in filtered if r not in known_rows]

    logger.debug("Known rows: %d, other rows: %d", len(known_rows), len(other_rows))

    # Rank known rows: bright objects first; within same brightness band, prefer
    # proper names > Messier > NGC > NAME sub-structures > IC.
    def _rank_known(row):
        main = _safe_str(row['main_id']) or ""
        name_part = main[5:].strip() if main.upper().startswith("NAME ") else main
        try:
            vmag_f = float(_safe_str(row['vmag']) or "")
        except (TypeError, ValueError):
            vmag_f = 50.0  # no vmag → treat as magnitude 50 (dim)

        # Tier: lower = more well-known naming convention
        if main.upper().startswith("NAME "):
            has_sub_number = bool(re.search(
                r'\b[A-Z]\d+\b|IRS \d|MIR \d|OrionBar [A-Z]|OMC-\d|LDN \d', name_part
            ))
            tier = 2 if has_sub_number else 0
        elif re.match(r'^M\s+', main):
            m_num_match = re.search(r'\d+', main)
            m_num = int(m_num_match.group()) if m_num_match else 999
            tier = 1
            # Use Messier number as tiebreaker within tier 1
            return (vmag_f, tier, float(m_num))
        elif re.match(r'^NGC\s+', main, re.IGNORECASE):
            tier = 3
        else:  # IC and others
            tier = 4

        return (vmag_f, tier, 0.0)

    sorted_known = sorted(known_rows, key=_rank_known)

    # Merge: rank known rows first, then rank others by familiarity
    def _rank_other(row):
        ids_s = _safe_str(row['ids']) or ""
        main  = _safe_str(row['main_id']) or ""
        name  = _pick_common_name(main, ids_s)
        try:
            vmag_f = float(_safe_str(row['vmag']) or "")
        except (TypeError, ValueError):
            vmag_f = 999.0
        if re.match(r'^(SH|LBN|LDN|RCW|Ced|vdB|Cr|Stock)\s*', name, re.IGNORECASE):
            return (0, vmag_f)
        if re.match(r'^\[', main):
            return (2, vmag_f)
        return (1, vmag_f)

    sorted_others = sorted(other_rows, key=_rank_other)

    combined = sorted_known + sorted_others
    # Remove exact duplicates by main_id
    seen_ids: set = set()
    unique: list = []
    for row in combined:
        mid = _safe_str(row['main_id']) or ""
        if mid not in seen_ids:
            seen_ids.add(mid)
            unique.append(row)

    return unique[:limit]


def _query_general(otype_filter: Optional[str], limit: int):
    """General TAP query filtered by otype only."""
    from astroquery.simbad import Simbad

    if otype_filter and otype_filter in _OTYPE_GROUPS:
        group_types = _OTYPE_GROUPS[otype_filter]
        quoted = ", ".join(f"'{t}'" for t in group_types)
        otype_clause = f"AND otype IN ({quoted})"
    elif otype_filter:
        otype_clause = f"AND otype = '{otype_filter}'"
    else:
        otype_clause = ""
    adql = f"""SELECT TOP {limit}
    main_id, otype, otype_txt, ra, dec, V AS vmag, ids
FROM basic
JOIN allfluxes ON allfluxes.oidref = basic.oid
JOIN ids ON ids.oidref = basic.oid
WHERE V IS NOT NULL
{otype_clause}
ORDER BY vmag ASC"""
    return Simbad.query_tap(adql)


# ── Result formatting ─────────────────────────────────────────────────────────

def _format_rows(rows, title: str) -> str:
    """
    Format a list of table rows (from TAP or query_region) into a
    user-friendly string.  Handles both column-name conventions.
    """
    lines = [title, "=" * len(title), ""]

    if not rows:
        lines.append("No objects found.")
        return "\n".join(lines)

    for i, row in enumerate(rows, 1):
        # Column name variants: TAP uses 'main_id', query_region uses 'main_id'
        # (astroquery 0.4.11 lowercases everything)
        main_id  = _safe_str(row['main_id'])  or f"Object {i}"
        otype    = _safe_str(row['otype'])    or ""
        try:
            otype_txt = _safe_str(row['otype_txt'])
        except (KeyError, TypeError):
            otype_txt = None
        # V magnitude: TAP alias is 'vmag', query_region field is 'V'
        vmag = None
        for vkey in ('vmag', 'V'):
            try:
                vmag = _safe_str(row[vkey])
                if vmag:
                    break
            except (KeyError, TypeError):
                pass
        ids_str  = _safe_str(row['ids'])       or ""
        ra_s     = _safe_str(row['ra'])         or ""
        dec_s    = _safe_str(row['dec'])        or ""

        name       = _pick_common_name(main_id, ids_str)
        type_label = _friendly_otype(otype, otype_txt or "")
        mag_desc   = _mag_description(vmag) if vmag else ""

        location_hint = ""
        if ra_s and dec_s:
            try:
                ra_f  = float(ra_s)
                dec_f = float(dec_s)
                hem   = "north" if dec_f >= 0 else "south"
                location_hint = f"  Position: RA {ra_f:.1f}°, Dec {dec_f:+.1f}° ({hem}ern sky)"
            except ValueError:
                pass

        lines.append(f"{i}. {name}")
        lines.append(f"   Type: {type_label}")
        if mag_desc:
            lines.append(f"   Brightness: {mag_desc}")
        if location_hint:
            lines.append(location_hint)
        lines.append("")

    lines.append("Source: SIMBAD Astronomical Database (simbad.u-strasbg.fr)")
    return "\n".join(lines)


# ── Public async entry point ──────────────────────────────────────────────────

async def simbad_search(query: str, limit: int = 10) -> str:
    """
    Search SIMBAD for astronomical objects from a natural-language query.
    The query is parsed locally into structured ADQL / astroquery calls;
    only structured requests are sent to SIMBAD.
    """
    import asyncio

    mode, otype_filter, constellation, limit, hemisphere = _parse_natural_language(query, limit)
    limit = max(1, min(int(limit), 100))

    logger.info(
        "SIMBAD search: mode=%s  otype=%s  constellation=%s  limit=%d  hemisphere=%s",
        mode, otype_filter, constellation, limit, hemisphere,
    )

    loop = asyncio.get_event_loop()

    try:
        if mode == "brightest_stars":
            type_label = (_friendly_otype(otype_filter) + "s") if otype_filter and otype_filter != "*" else "Stars"
            hem_label = (" in the Northern Sky" if hemisphere == "north"
                         else " in the Southern Sky" if hemisphere == "south"
                         else " in the Sky")
            title = f"The {limit} Brightest {type_label}{hem_label}"
            rows = await loop.run_in_executor(
                None, lambda: _query_brightest_stars(limit, otype_filter, hemisphere)
            )

        elif mode == "constellation_objects":
            const_name = _ABBREV_TO_FULL.get(constellation, constellation)
            type_label = _friendly_otype(otype_filter) if otype_filter else "Object"
            title = f"{type_label} in the Constellation {const_name}"
            table = await loop.run_in_executor(
                None, _query_constellation_objects, constellation, otype_filter, limit
            )
            rows = list(table) if table is not None else []

        else:  # general
            if otype_filter:
                title = f"{_friendly_otype(otype_filter)}"
            else:
                title = "Astronomical Objects"
            table = await loop.run_in_executor(None, _query_general, otype_filter, limit)
            rows = list(table) if table is not None else []

    except Exception as exc:
        logger.error("SIMBAD query failed: %s", exc, exc_info=True)
        return f"Sorry, the SIMBAD search could not be completed: {exc}"

    if not rows:
        return f'No results found for: "{query}"'

    return _format_rows(rows, title)


