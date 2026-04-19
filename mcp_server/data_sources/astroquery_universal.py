"""
Universal Astroquery Wrapper for Astro MCP

Provides automatic discovery and access to all astroquery services
without manual integration of each service.
"""

import importlib
import pkgutil
import inspect
import logging
from typing import Any, Dict, List, Optional, Tuple
from datetime import datetime
import uuid
from pathlib import Path
import numpy as np

import astroquery
from astropy.table import Table
from astropy.coordinates import SkyCoord
import astropy.units as u

import re
import functools

from .base import BaseDataSource
from .astroquery_metadata import get_service_info, ASTROQUERY_SERVICE_INFO

logger = logging.getLogger(__name__)

# Maps lowercase constellation names / IAU abbreviations → uppercase IAU 3-letter code.
# Used to look up the constellation in astropy's boundary data. Coordinates are derived
# from that data at runtime (see _constellation_center), so no coordinate table is needed.
_CONSTELLATION_ABBREVS = {
    # 3-letter IAU abbreviations (lowercase → uppercase)
    'and': 'AND', 'ant': 'ANT', 'aps': 'APS', 'aqr': 'AQR', 'aql': 'AQL',
    'ara': 'ARA', 'ari': 'ARI', 'aur': 'AUR', 'boo': 'BOO', 'cae': 'CAE',
    'cam': 'CAM', 'cnc': 'CNC', 'cvn': 'CVN', 'cma': 'CMA', 'cmi': 'CMI',
    'cap': 'CAP', 'car': 'CAR', 'cas': 'CAS', 'cen': 'CEN', 'cep': 'CEP',
    'cet': 'CET', 'cha': 'CHA', 'cir': 'CIR', 'col': 'COL', 'com': 'COM',
    'cra': 'CRA', 'crb': 'CRB', 'crv': 'CRV', 'crt': 'CRT', 'cru': 'CRU',
    'cyg': 'CYG', 'del': 'DEL', 'dor': 'DOR', 'dra': 'DRA', 'equ': 'EQU',
    'eri': 'ERI', 'for': 'FOR', 'gem': 'GEM', 'gru': 'GRU', 'her': 'HER',
    'hor': 'HOR', 'hya': 'HYA', 'hyi': 'HYI', 'ind': 'IND', 'lac': 'LAC',
    'leo': 'LEO', 'lmi': 'LMI', 'lep': 'LEP', 'lib': 'LIB', 'lup': 'LUP',
    'lyn': 'LYN', 'lyr': 'LYR', 'men': 'MEN', 'mic': 'MIC', 'mon': 'MON',
    'mus': 'MUS', 'nor': 'NOR', 'oct': 'OCT', 'oph': 'OPH', 'ori': 'ORI',
    'pav': 'PAV', 'peg': 'PEG', 'per': 'PER', 'phe': 'PHE', 'pic': 'PIC',
    'psc': 'PSC', 'psa': 'PSA', 'pup': 'PUP', 'pyx': 'PYX', 'ret': 'RET',
    'sge': 'SGE', 'sgr': 'SGR', 'sco': 'SCO', 'scl': 'SCL', 'sct': 'SCT',
    'ser': 'SER', 'sex': 'SEX', 'tau': 'TAU', 'tel': 'TEL', 'tri': 'TRI',
    'tra': 'TRA', 'tuc': 'TUC', 'uma': 'UMA', 'umi': 'UMI', 'vel': 'VEL',
    'vir': 'VIR', 'vol': 'VOL', 'vul': 'VUL',
    # Full IAU names (lowercase)
    'andromeda': 'AND',          'antlia': 'ANT',           'apus': 'APS',
    'aquarius': 'AQR',           'aquila': 'AQL',           'ara': 'ARA',
    'aries': 'ARI',              'auriga': 'AUR',           'boötes': 'BOO',
    'bootes': 'BOO',             'caelum': 'CAE',           'camelopardalis': 'CAM',
    'cancer': 'CNC',             'canes venatici': 'CVN',   'canis major': 'CMA',
    'canis minor': 'CMI',        'capricornus': 'CAP',      'carina': 'CAR',
    'cassiopeia': 'CAS',         'centaurus': 'CEN',        'cepheus': 'CEP',
    'cetus': 'CET',              'chamaeleon': 'CHA',       'circinus': 'CIR',
    'columba': 'COL',            'coma berenices': 'COM',   'corona australis': 'CRA',
    'corona borealis': 'CRB',    'corvus': 'CRV',           'crater': 'CRT',
    'crux': 'CRU',               'cygnus': 'CYG',           'delphinus': 'DEL',
    'dorado': 'DOR',             'draco': 'DRA',            'equuleus': 'EQU',
    'eridanus': 'ERI',           'fornax': 'FOR',           'gemini': 'GEM',
    'grus': 'GRU',               'hercules': 'HER',         'horologium': 'HOR',
    'hydra': 'HYA',              'hydrus': 'HYI',           'indus': 'IND',
    'lacerta': 'LAC',            'leo minor': 'LMI',        'lepus': 'LEP',
    'libra': 'LIB',              'lupus': 'LUP',            'lynx': 'LYN',
    'lyra': 'LYR',               'mensa': 'MEN',            'microscopium': 'MIC',
    'monoceros': 'MON',          'musca': 'MUS',            'norma': 'NOR',
    'octans': 'OCT',             'ophiuchus': 'OPH',        'orion': 'ORI',
    'pavo': 'PAV',               'pegasus': 'PEG',          'perseus': 'PER',
    'phoenix': 'PHE',            'pictor': 'PIC',           'pisces': 'PSC',
    'piscis austrinus': 'PSA',   'puppis': 'PUP',           'pyxis': 'PYX',
    'reticulum': 'RET',          'sagitta': 'SGE',          'sagittarius': 'SGR',
    'scorpius': 'SCO',           'scorpio': 'SCO',          'sculptor': 'SCL',
    'scutum': 'SCT',             'serpens': 'SER',          'sextans': 'SEX',
    'taurus': 'TAU',             'telescopium': 'TEL',      'triangulum': 'TRI',
    'triangulum australe': 'TRA','tucana': 'TUC',           'ursa major': 'UMA',
    'ursa minor': 'UMI',         'vela': 'VEL',             'virgo': 'VIR',
    'volans': 'VOL',             'vulpecula': 'VUL',
}


@functools.lru_cache(maxsize=None)
def _constellation_center(iau_abbrev: str):
    """Compute (ra_deg, dec_deg) centroid for a constellation.

    Samples a 3°×3° sky grid, identifies all points belonging to the target
    constellation via astropy.coordinates.get_constellation(), then returns
    their circular-mean RA and arithmetic-mean Dec.
    """
    try:
        import numpy as np
        from astropy.coordinates import SkyCoord, get_constellation
        import astropy.units as u

        step = 3.0  # degrees — coarse enough to be fast, fine enough for all 88 constellations
        ras = np.arange(0.0, 360.0, step)
        decs = np.arange(-89.0, 90.0, step)
        RA, DEC = np.meshgrid(ras, decs)
        ra_flat = RA.ravel()
        dec_flat = DEC.ravel()

        coords = SkyCoord(ra=ra_flat * u.deg, dec=dec_flat * u.deg, frame='icrs')
        abbrevs = get_constellation(coords, short_name=True)

        target_lower = iau_abbrev.lower()
        mask = np.array([a.lower() == target_lower for a in abbrevs])
        if not mask.any():
            logger.warning(f"No grid points found for constellation '{iau_abbrev}'")
            return None

        hit_ras = ra_flat[mask]
        hit_decs = dec_flat[mask]

        # Circular mean for RA to handle the 0h/24h wrap (e.g. Pisces)
        ra_rad = np.deg2rad(hit_ras)
        mean_ra_rad = np.arctan2(np.mean(np.sin(ra_rad)), np.mean(np.cos(ra_rad)))
        if mean_ra_rad < 0:
            mean_ra_rad += 2 * np.pi

        ra_deg = float(np.rad2deg(mean_ra_rad))
        dec_deg = float(np.mean(hit_decs))
        logger.info(f"Constellation center for {iau_abbrev}: RA={ra_deg:.2f} Dec={dec_deg:.2f} ({mask.sum()} grid points)")
        return (ra_deg, dec_deg)

    except Exception as exc:
        logger.warning(f"Could not compute constellation center for '{iau_abbrev}': {exc}")
        return None


def _resolve_constellation(name: str):
    """Return (ra_deg, dec_deg) if *name* is a constellation abbreviation or full name."""
    abbrev = _CONSTELLATION_ABBREVS.get(name.lower().strip())
    if abbrev:
        return _constellation_center(abbrev)
    return None


_REGION_CIRCLE_RE = re.compile(
    r'region\s*\(\s*Circle\s*,\s*([^,]+?)\s*,\s*([^)]+?)\s*\)',
    re.IGNORECASE,
)


def _expand_constellation_in_criteria(criteria: str) -> str:
    """Replace region(Circle, CONSTELLATION, RADIUSd) with explicit RA/Dec coordinates.

    SIMBAD's query_criteria passes the criteria string to a service that calls SESAME
    to resolve the centre name.  Constellation abbreviations (e.g. 'Ori') are not
    recognised by SESAME, so we swap them out for numeric coordinates.
    """
    def replacer(m):
        name = m.group(1).strip()
        radius = m.group(2).strip()
        coords = _resolve_constellation(name)
        if coords:
            ra, dec = coords
            sign = '+' if dec >= 0 else ''
            logger.info(f"Replaced constellation '{name}' with coordinates ({ra:.4f}, {sign}{dec:.4f}) in criteria")
            return f"region(Circle, {ra:.4f} {sign}{dec:.4f}, {radius})"
        return m.group(0)
    return _REGION_CIRCLE_RE.sub(replacer, criteria)


class AstroqueryUniversal(BaseDataSource):
    """Universal wrapper for all astroquery services."""
    
    def __init__(self, base_dir: str = None):
        super().__init__(base_dir=base_dir, source_name="astroquery")
        self.source_dir = self.base_dir / self.source_name
        self._services = {}
        self._service_metadata = {}
        self._discover_services()
    
    def _load_dace_query_service(self):
        """Register the dace-query package as a DACE data service."""
        try:
            import dace_query
            from dace_query import spectroscopy, photometry, opacity, exoplanet, catalog, target
            self._services['dace'] = {
                'module': dace_query,
                'class': None,
                'instance': None,
                'capabilities': {
                    'spectroscopy': True,
                    'photometry': True,
                    'radial_velocity': True,
                    'exoplanet': True,
                    'opacity': True,
                    'catalog': True,
                    'target': True,
                },
                'description': 'DACE (Data & Analysis Center for Exoplanets) — radial velocities, photometry, spectra, and exoplanet data via dace-query.',
                'requires_auth': False,
                'full_name': 'DACE (dace-query)',
                'data_types': ['spectroscopy', 'photometry', 'radial_velocity', 'exoplanet'],
                'wavelength_coverage': 'optical, near-IR',
                'object_types': 'exoplanets, stars',
                'example_queries': [
                    'from dace_query import spectroscopy; spectroscopy.Spectroscopy.query_database(limit=10)',
                    'from dace_query import photometry; photometry.Photometry.query_database(limit=10)',
                ]
            }
            logger.info("Registered dace-query as DACE service")
        except ImportError:
            logger.warning("dace-query not installed; DACE service unavailable. Install with: pip install dace-query")

    def _discover_services(self):
        """Automatically discover all available astroquery services."""

        logger.info("Discovering astroquery services...")
        
        # dace has been removed from astroquery; handled separately via dace-query package
        EXCLUDED_MODULES = {'utils', 'extern', 'solarsystem', 'dace'}
        self._load_dace_query_service()

        for importer, modname, ispkg in pkgutil.iter_modules(astroquery.__path__):
            if ispkg and modname not in EXCLUDED_MODULES:
                try:
                    # Import the module
                    module = importlib.import_module(f'astroquery.{modname}')
                    
                    # Find the main query class
                    service_class = self._find_service_class(module, modname)
                    
                    if service_class:
                        # Get enhanced metadata from our metadata system
                        enhanced_metadata = get_service_info(modname)
                        
                        self._services[modname] = {
                            'module': module,
                            'class': service_class,
                            'instance': None,  # Lazy instantiation
                            'capabilities': self._detect_capabilities(service_class),
                            'description': enhanced_metadata.get('description', self._extract_description(module, service_class)),
                            'requires_auth': enhanced_metadata.get('requires_auth', self._check_authentication(service_class)),
                            'full_name': enhanced_metadata.get('full_name', f'{modname.upper()} Service'),
                            'data_types': enhanced_metadata.get('data_types', ['unknown']),
                            'wavelength_coverage': enhanced_metadata.get('wavelength_coverage', 'unknown'),
                            'object_types': enhanced_metadata.get('object_types', 'unknown'),
                            'example_queries': enhanced_metadata.get('example_queries', [])
                        }
                        logger.info(f"Discovered service: {modname}")
                except Exception as e:
                    logger.warning(f"Could not load service {modname}: {e}")
        
        logger.info(f"Discovered {len(self._services)} astroquery services")
    
    def _find_service_class(self, module, modname):
        """Find the main query class in a module."""
        # Common patterns for main class names
        potential_names = [
            modname.capitalize(),
            modname.upper(),
            f"{modname.capitalize()}Class",
            modname.replace('_', '').capitalize()
        ]
        
        for name in potential_names:
            if hasattr(module, name):
                cls = getattr(module, name)
                if isinstance(cls, type):
                    return cls
        
        # Fallback: look for a class with query methods
        for name in dir(module):
            obj = getattr(module, name)
            if isinstance(obj, type) and hasattr(obj, 'query_region'):
                return obj
        
        return None
    
    def _detect_capabilities(self, service_class):
        """Detect what query methods a service supports."""
        capabilities = {}
        
        # Check for standard query methods
        standard_methods = [
            'query_object', 'query_region', 'query_criteria',
            'get_images', 'get_image_list', 'query', 'query_async'
        ]
        
        for method in standard_methods:
            if hasattr(service_class, method):
                capabilities[method] = True
        
        # Find all query_* methods
        for attr in dir(service_class):
            if attr.startswith('query_') and callable(getattr(service_class, attr, None)):
                capabilities[attr] = True
        
        return capabilities
    
    def _extract_description(self, module, service_class):
        """Extract a description from the module or class docstring."""
        if service_class.__doc__:
            return inspect.cleandoc(service_class.__doc__).split('\\n')[0]
        if module.__doc__:
            return inspect.cleandoc(module.__doc__).split('\\n')[0]
        return "No description available."

    def _check_authentication(self, service_class):
        """Check if the service likely requires authentication."""
        # Heuristic: check for methods like 'login' or '_login'
        for attr in dir(service_class):
            if attr.lower() in ['login', '_login']:
                return True
        return False

    def list_services(self) -> List[Dict[str, Any]]:
        """Return a list of discovered services with their enhanced metadata."""
        service_list = []
        for name, meta in self._services.items():
            service_list.append({
                "service": name,
                "full_name": meta['full_name'],
                "description": meta['description'],
                "data_types": meta['data_types'],
                "wavelength_coverage": meta['wavelength_coverage'],
                "object_types": meta['object_types'],
                "capabilities": list(meta['capabilities'].keys()),
                "requires_auth": meta['requires_auth'],
                "example_queries": meta['example_queries']
            })
        return sorted(service_list, key=lambda x: x['service'])

    def get_service_details(self, service_name: str) -> Dict[str, Any]:
        """Get detailed information about a specific service, including method signatures."""
        if service_name not in self._services:
            raise ValueError(f"Unknown service: {service_name}")
        
        meta = self._services[service_name]
        service_class = meta.get('class')

        # Base details
        details = {
            "service": service_name,
            "full_name": meta['full_name'],
            "description": meta['description'],
            "data_types": meta['data_types'],
            "wavelength_coverage": meta['wavelength_coverage'],
            "object_types": meta['object_types'],
            "capabilities": list(meta['capabilities'].keys()),
            "requires_auth": meta['requires_auth'],
            "example_queries": meta['example_queries'],
            "module_path": f"astroquery.{service_name}",
            "class_name": service_class.__name__ if service_class else "Unknown",
            "methods": {}
        }

        if not service_class:
            return details

        # Introspect methods to get parameters and docstrings
        for method_name in details['capabilities']:
            if hasattr(service_class, method_name):
                method = getattr(service_class, method_name)
                
                try:
                    sig = inspect.signature(method)
                    method_info = {
                        'docstring': inspect.cleandoc(method.__doc__ or "No docstring available.").split('\\n')[0],
                        'parameters': {}
                    }
                    
                    for param in sig.parameters.values():
                        # Skip self, args, kwargs
                        if param.name in ['self', 'args', 'kwargs']:
                            continue
                        
                        param_info = {}
                        if param.default is not inspect.Parameter.empty:
                            param_info['default'] = str(param.default)
                        else:
                            param_info['required'] = True
                            
                        if param.annotation is not inspect.Parameter.empty:
                            # Clean up the type annotation string
                            param_info['type'] = str(param.annotation).replace("<class '", "").replace("'>", "")
                        else:
                            param_info['type'] = 'Any'

                        method_info['parameters'][param.name] = param_info
                    
                    details['methods'][method_name] = method_info

                except (ValueError, TypeError): # Some methods may not be introspectable
                    details['methods'][method_name] = {
                        'docstring': 'Could not inspect method signature.',
                        'parameters': {}
                    }

        return details

    def search_services(self,
                        data_type: str = None,
                        wavelength: str = None,
                        object_type: str = None,
                        capability: str = None,
                        requires_auth: bool = None) -> List[Dict]:
        """Find and rank services that match specified criteria."""
        matches = []
        
        for service_name, service_info in self._services.items():
            score = 0
            match_reasons = []

            # Filter by data type
            if data_type:
                service_data_types = [dt.lower() for dt in service_info.get('data_types', [])]
                if data_type.lower() in service_data_types:
                    score += 3
                    match_reasons.append(f"provides {data_type} data")

            # Filter by wavelength coverage
            if wavelength:
                coverage = service_info.get('wavelength_coverage', '').lower()
                if wavelength.lower() in coverage or coverage == 'all':
                    score += 2
                    match_reasons.append(f"covers {wavelength} wavelengths")

            # Filter by object type
            if object_type:
                obj_types = service_info.get('object_types', 'all')
                object_type_lower = object_type.lower()
                
                match_found = False
                if isinstance(obj_types, list):
                    service_object_types = [ot.lower() for ot in obj_types]
                    if 'all' in service_object_types or object_type_lower in service_object_types:
                        match_found = True
                elif obj_types == 'all' or object_type_lower in obj_types.lower():
                    match_found = True
                
                if match_found:
                    score += 2
                    match_reasons.append(f"includes {object_type}")

            # Filter by capability
            if capability and capability in service_info['capabilities']:
                score += 3
                match_reasons.append(f"supports '{capability}'")
            
            # Filter by authentication requirement
            if requires_auth is not None and service_info['requires_auth'] == requires_auth:
                score += 1
                reason = "does not require authentication" if not requires_auth else "matches auth requirement"
                match_reasons.append(reason)
            
            if score > 0:
                matches.append({
                    'service': service_name,
                    'full_name': service_info['full_name'],
                    'score': score,
                    'reasons': match_reasons,
                    'description': service_info['description'].split('\\n')[0]
                })

        # Sort by score
        matches.sort(key=lambda x: x['score'], reverse=True)
        return matches

    def universal_query(self, service_name: str, query_type: str = 'auto', auto_save: bool = True, **kwargs) -> Dict[str, Any]:
        """
        Universal query interface for any astroquery service.
        
        Parameters
        ----------
        service_name : str
            Name of the astroquery service
        query_type : str
            Type of query to perform (auto-detected if 'auto')
        auto_save : bool
            Whether to automatically save results to a file (default: True)
        **kwargs : dict
            Query parameters passed to the service
        
        Returns
        -------
        dict
            Query results with status and data
        """
        try:
            # Ensure the service class is available
            if service_name not in self._services:
                raise ValueError(f"Unknown service: {service_name}")
            
            service_info = self._services[service_name]
            
            # --- AUTHENTICATION CHECK ---
            if service_info.get('requires_auth'):
                return self._generate_auth_required_help(service_name, query_type, kwargs)

            service = self.get_service(service_name)
            
            # Auto-detect query type
            if query_type == 'auto':
                query_type = self._detect_query_type(service_name, kwargs)
            
            if not hasattr(service, query_type):
                raise AttributeError(f"Service '{service_name}' does not have method '{query_type}'")

            # Parameter preprocessing
            processed_kwargs = self._preprocess_parameters(service_name, query_type, kwargs)

            # ----------------------------------------------------------------
            # SIMBAD-specific handling
            # ----------------------------------------------------------------
            positional_args = []
            vmag_max = None

            if service_name == 'simbad':
                from astroquery.simbad import Simbad as SimbadClass
                simbad_instance = SimbadClass()

                # Always include V-magnitude and object type in results.
                # Suppress the "column already added" warning that fires on repeated calls.
                import warnings
                with warnings.catch_warnings():
                    warnings.simplefilter("ignore")
                    simbad_instance.add_votable_fields('flux(V)', 'otype', 'sptype')

                # Allow the caller to add extra votable fields.
                extra_fields = processed_kwargs.pop('votable_fields', None)
                if extra_fields:
                    if isinstance(extra_fields, str):
                        extra_fields = [f.strip() for f in extra_fields.split(',')]
                    with warnings.catch_warnings():
                        warnings.simplefilter("ignore")
                        simbad_instance.add_votable_fields(*extra_fields)

                # query_region: if coordinates is a plain string constellation name,
                # resolve it to a SkyCoord now so SIMBAD doesn't forward it to SESAME.
                if query_type == 'query_region':
                    coord_val = processed_kwargs.get('coordinates')
                    if isinstance(coord_val, str):
                        resolved = _resolve_constellation(coord_val)
                        if resolved:
                            ra_c, dec_c = resolved
                            logger.info(f"Resolved constellation '{coord_val}' to RA={ra_c:.4f}, Dec={dec_c:.4f} for query_region")
                            processed_kwargs['coordinates'] = SkyCoord(
                                ra=ra_c * u.deg, dec=dec_c * u.deg, frame='icrs'
                            )

                # query_criteria takes *criteria positional strings, NOT a keyword arg.
                # Extract the 'criteria' key regardless of query_type — it is only valid for
                # query_criteria and must never be forwarded to query_region or query_object.
                criteria_str = processed_kwargs.pop('criteria', None)
                if query_type == 'query_criteria' and criteria_str:
                    criteria_str = _expand_constellation_in_criteria(criteria_str)
                    positional_args = [criteria_str]

                # vmag_max: apply V-magnitude filter.
                # For query_criteria it goes into the ADQL string (server-side).
                # For query_region we redirect to query_criteria so the filter is
                # applied at SIMBAD before any rows are transmitted — fetching an
                # unfiltered region (e.g. 12°) could return 100k+ rows and crash.
                vmag_max = processed_kwargs.pop('vmag_max', None)
                if vmag_max is not None:
                    vmag_max = float(vmag_max)

                if query_type == 'query_region' and vmag_max is not None:
                    coord_obj = processed_kwargs.get('coordinates')
                    radius_qty = processed_kwargs.get('radius')
                    if coord_obj is not None and radius_qty is not None:
                        ra_deg = coord_obj.icrs.ra.deg
                        dec_deg = coord_obj.icrs.dec.deg
                        radius_deg = float(radius_qty.to(u.deg).value)
                        sign = '+' if dec_deg >= 0 else ''
                        criteria_built = (
                            f"region(Circle, {ra_deg:.4f} {sign}{dec_deg:.4f}, {radius_deg:.4f}d)"
                            f" & Vmag <= {vmag_max}"
                        )
                        logger.info(
                            f"Redirecting query_region+vmag_max to query_criteria: {criteria_built}"
                        )
                        query_type = 'query_criteria'
                        positional_args = [criteria_built]
                        processed_kwargs = {}  # query_criteria takes no keyword args
                        vmag_max = None        # filter applied server-side; no post-filter needed

                service = simbad_instance
            # ----------------------------------------------------------------

            # Execute query
            method = getattr(service, query_type)
            result = method(*positional_args, **processed_kwargs)

            # SIMBAD post-filter: keep only rows where FLUX_V <= vmag_max
            if vmag_max is not None and result is not None:
                from astropy.table import Table as AstroTable
                if isinstance(result, AstroTable):
                    vmag_col = next((c for c in result.colnames if c.upper() == 'FLUX_V'), None)
                    if vmag_col:
                        import numpy as np
                        filled = np.ma.filled(result[vmag_col], fill_value=999.0)
                        result = result[filled <= vmag_max]
                        logger.info(f"SIMBAD post-filter (FLUX_V <= {vmag_max}): {len(result)} rows remaining")
            
            # Process and save results
            return self._process_results(result, service_name, query_type, kwargs, auto_save)
            
        except Exception as e:
            logger.error(f"Query failed for {service_name}: {str(e)}")
            return {
                'status': 'error',
                'service': service_name,
                'query_type': query_type,
                'error': str(e),
                'help': self._generate_error_help(service_name, query_type, e)
            }

    def _detect_query_type(self, service_name: str, kwargs) -> str:
        """Auto-detect the appropriate query method based on parameters."""
        capabilities = self._services[service_name]['capabilities']
        
        # If the caller explicitly provided a radius alongside an object name,
        # they want a cone/region search, not a single-object lookup.
        has_object_name = any(key in kwargs for key in ['object_name', 'objectname', 'target', 'source'])
        has_radius = 'radius' in kwargs
        has_coords = 'coordinates' in kwargs or all(k in kwargs for k in ['ra', 'dec'])

        if (has_coords or (has_object_name and has_radius)):
            if 'query_region' in capabilities:
                return 'query_region'

        if has_object_name:
            if 'query_object' in capabilities:
                return 'query_object'
        
        # Check for catalog query (Vizier specific)
        if 'catalog' in kwargs and 'query_catalog' in capabilities:
            return 'query_catalog'
        
        # Criteria string → query_criteria
        if 'criteria' in kwargs and 'query_criteria' in capabilities:
            return 'query_criteria'
        
        # Default to generic query if available
        if 'query' in capabilities:
            return 'query'
        
        # Fallback to first available high-priority query method
        for method in ['query_object', 'query_region', 'query_criteria']:
            if method in capabilities:
                return method
        
        if capabilities:
            return list(capabilities.keys())[0]

        raise ValueError(f"Could not determine appropriate query method for service {service_name}")

    def _preprocess_parameters(self, service_name: str, query_type: str, kwargs: Dict) -> Dict:
        """Preprocess parameters for compatibility."""
        processed = kwargs.copy()

        # Strip internal routing/meta keys that must not be forwarded to the service method.
        # Also strip common LLM hallucinations that have no meaning to astroquery.
        # Note: 'criteria' and 'vmag_max' are intentionally NOT stripped here — they are
        # consumed by the SIMBAD-specific block in universal_query (popped before the method call).
        STRIP_KEYS = (
            'query_type', 'service_name', 'auto_save', 'votable_fields',
            'data_type', 'wavelength', 'magnitude', 'filter', 'band', 'survey',
        )
        for key in STRIP_KEYS:
            processed.pop(key, None)

        # 'criteria' is only valid for query_criteria; remove it for all other query types
        # so it never leaks into query_region, query_object, etc.
        if query_type != 'query_criteria':
            processed.pop('criteria', None)
        
        logger.info(f"Preprocessing parameters for {service_name} ({query_type}): {processed}")
        
        # Hybrid approach: LLM provides simple dicts for complex objects,
        # and this function constructs the actual Python objects.

        # For query_region, 'coordinates' can be a name string (SIMBAD resolves it),
        # a SkyCoord dict, or raw ra/dec.
        if query_type == 'query_region':
            has_name = any(k in processed for k in ['object_name', 'objectname', 'target', 'source'])
            has_coords = 'coordinates' in processed or all(k in processed for k in ['ra', 'dec'])

            if has_name and has_coords:
                # LLM sent both — prefer the object name for SIMBAD constellation searches;
                # drop raw coordinates so SIMBAD can resolve the name to a centre.
                for alias in ['object_name', 'objectname', 'target', 'source']:
                    if alias in processed:
                        processed['coordinates'] = processed.pop(alias)
                        break
                processed.pop('ra', None)
                processed.pop('dec', None)
            elif has_name and 'coordinates' not in processed:
                # Promote object_name → coordinates string
                for alias in ['object_name', 'objectname', 'target', 'source']:
                    if alias in processed:
                        processed['coordinates'] = processed.pop(alias)
                        break

        # Construct SkyCoord object if 'coordinates' is a dict with ra/dec keys
        if 'coordinates' in processed and isinstance(processed['coordinates'], dict):
            coord_info = processed.pop('coordinates')
            from astropy.coordinates import SkyCoord
            unit_str = coord_info.get('unit', 'deg')
            unit = (u.deg, u.deg) if unit_str == 'deg' else u.Unit(unit_str)
            processed['coordinates'] = SkyCoord(
                ra=coord_info['ra'],
                dec=coord_info['dec'],
                unit=unit
            )
        elif 'ra' in processed and 'dec' in processed and 'coordinates' not in processed:
            from astropy.coordinates import SkyCoord
            processed['coordinates'] = SkyCoord(
                ra=processed.pop('ra'),
                dec=processed.pop('dec'),
                unit=u.deg
            )

        # Convert radius/size to astropy quantity if it's not already
        # Accept both numeric degrees and string notation like '5d0m0s' or '20arcmin'
        if 'radius' in processed:
            r = processed['radius']
            if not hasattr(r, 'unit'):
                if isinstance(r, str):
                    processed['radius'] = u.Quantity(r)
                else:
                    processed['radius'] = float(r) * u.deg
        
        if 'size' in processed and not hasattr(processed['size'], 'unit'):
            processed['size'] = processed.pop('size') * u.deg
        
        # Handle object name aliases - this is a safe, generic improvement.
        if query_type == 'query_object':
            target_param = 'object_name' 
            for alias in ['objectname', 'target', 'source']:
                if alias in processed:
                    processed[target_param] = processed.pop(alias)
                    break
        
        # Handle SQL query parameter mapping
        if query_type == 'query_sql' or 'sql' in query_type:
            # SDSS and some other services use 'sql_query' instead of 'sql'
            if 'sql' in processed and 'sql_query' not in processed:
                processed['sql_query'] = processed.pop('sql')
        
        return processed

    def _process_results(self, result, service_name, query_type, kwargs, auto_save):
        """Standardize query results and handle auto-saving."""
        data = None
        num_rows = 0
        save_result = None

        logger.info(f"Processing result of type: {type(result)} for service {service_name}")

        def clean_value(value):
            """Converts numpy/special types to standard python types for JSON."""
            # Masked / missing values → None
            if value is np.ma.masked or isinstance(value, np.ma.core.MaskedConstant):
                return None
            if isinstance(value, (np.integer, np.int64)):
                return int(value)
            if isinstance(value, (np.floating, np.float32, np.float64)):
                v = float(value)
                # np.nan / np.inf are not valid JSON
                if v != v or v == float('inf') or v == float('-inf'):
                    return None
                return v
            if isinstance(value, np.bool_):
                return bool(value)
            if isinstance(value, bytes):
                return value.decode('utf-8', 'ignore')
            if isinstance(value, np.ndarray):
                return value.tolist()
            return value
        
        def process_row(row):
            """Safely processes a dict or an astropy.table.Row into a clean dict."""
            if isinstance(row, dict):
                return {k: clean_value(v) for k, v in row.items()}
            # astropy.table.Row can be accessed by column name
            elif hasattr(row, 'colnames'):
                return {col: clean_value(row[col]) for col in row.colnames}
            # Handle other potential non-row items in a list
            return clean_value(row)

        if isinstance(result, Table):
            data = [process_row(row) for row in result]
            num_rows = len(data)

            if auto_save and num_rows > 0:
                # Generate filename
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                filename = f"astroquery_{service_name}_{query_type}_{timestamp}.csv"
                self.source_dir.mkdir(parents=True, exist_ok=True)
                full_path = self.source_dir / filename
                
                # Save to CSV
                result.write(full_path, format='csv', overwrite=True)
                
                # Register file
                description = f"Results from astroquery service '{service_name}' using '{query_type}'"
                serializable_kwargs = {k: str(v) for k, v in kwargs.items()}
                save_result = self._register_file(
                    filename=str(full_path),
                    description=description,
                    file_type='csv',
                    metadata={'service': service_name, 'query_type': query_type, 'query_params': serializable_kwargs}
                )
        
        # Handle a list of FITS images
        elif isinstance(result, list) and len(result) > 0 and hasattr(result[0], 'writeto'):
            logger.info(f"Detected a list of {len(result)} FITS-like objects.")
            num_rows = len(result)
            data = f"Returned {len(result)} FITS image(s)."

            if auto_save:
                saved_files_info = []
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                
                for i, hdu in enumerate(result):
                    filename = f"astroquery_{service_name}_{query_type}_{timestamp}_{i}.fits"
                    full_path = self.source_dir / filename
                    hdu.writeto(full_path, overwrite=True)
                    
                    description = f"FITS image {i+1}/{len(result)} from service '{service_name}'"
                    serializable_kwargs = {k: str(v) for k, v in kwargs.items()}
                    file_info = self._register_file(
                        filename=str(full_path),
                        description=description,
                        file_type='fits',
                        metadata={'service': service_name, 'query_type': query_type, 'query_params': serializable_kwargs}
                    )
                    saved_files_info.append(file_info)

                if saved_files_info:
                    first_file = saved_files_info[0]
                    save_result = {
                        'status': 'success',
                        'file_id': first_file['id'],
                        'filename': f"{len(saved_files_info)} files saved in {self.source_dir}",
                        'size_bytes': sum(f['size_bytes'] for f in saved_files_info),
                        'location': str(self.source_dir)
                    }

        # Handle a single FITS image
        elif hasattr(result, 'writeto'): # Heuristic for FITS HDU objects
            logger.info("Detected a single FITS-like object.")
            num_rows = 1
            data = "FITS image data" # Placeholder text
            
            if auto_save:
                # Generate filename
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                filename = f"astroquery_{service_name}_{query_type}_{timestamp}.fits"
                full_path = self.source_dir / filename
                
                # Save to FITS
                result.writeto(full_path, overwrite=True)
                
                # Register file
                description = f"FITS image from astroquery service '{service_name}' using '{query_type}'"
                serializable_kwargs = {k: str(v) for k, v in kwargs.items()}
                save_result = self._register_file(
                    filename=str(full_path),
                    description=description,
                    file_type='fits',
                    metadata={'service': service_name, 'query_type': query_type, 'query_params': serializable_kwargs}
                )

        elif result is None:
            data = []
            num_rows = 0
        elif isinstance(result, list):
            data = [process_row(row) for row in result]
            num_rows = len(data)
        else:
            # For other types, just represent them as a string
            data = str(result)
            num_rows = 1 if data else 0

        # Make kwargs serializable for the response
        serializable_kwargs = {}
        for k, v in kwargs.items():
            if isinstance(v, (u.Quantity, SkyCoord)):
                serializable_kwargs[k] = str(v)
            else:
                serializable_kwargs[k] = v

        return {
            'status': 'success',
            'service': service_name,
            'query_type': query_type,
            'query_params': serializable_kwargs,
            'num_results': num_rows,
            'results': data,
            'save_result': save_result
        }

    def _register_file(self, filename: str, description: str, file_type: str, metadata: dict) -> dict:
        """
        Registers a new file with the central registry and updates statistics.
        This is a core piece of functionality that should be in the base class.
        """
        try:
            file_path = Path(filename)
            file_id = str(uuid.uuid4())
            file_stat = file_path.stat()
            
            record = {
                'id': file_id,
                'source': self.source_name,
                'filename': filename,
                'description': description,
                'file_type': file_type,
                'size_bytes': file_stat.st_size,
                'created': datetime.fromtimestamp(file_stat.st_ctime).isoformat(),
                'metadata': metadata
            }
            
            # Use the shared registry
            self.registry['files'][file_id] = record
            
            # Update statistics
            self.registry['statistics']['total_files'] += 1
            self.registry['statistics']['total_size_bytes'] += file_stat.st_size
            self.registry['statistics']['by_source'][self.source_name] = self.registry['statistics']['by_source'].get(self.source_name, 0) + 1
            self.registry['statistics']['by_type'][file_type] = self.registry['statistics']['by_type'].get(file_type, 0) + 1
            
            return {'status': 'success', 'file_id': file_id, **record}
        
        except Exception as e:
            logger.error(f"Failed to register file {filename}: {e}")
            return {'status': 'error', 'error': str(e)}

    def _generate_auth_required_help(self, service_name: str, query_type: str, kwargs: dict) -> dict:
        """Generate a standardized response for services that require authentication."""
        service_class_name = self._services[service_name]['class'].__name__
        
        # Build the example script
        script_lines = [
            f"from astroquery.{service_name} import {service_class_name}",
            "import astropy.units as u",
            "from astropy.coordinates import SkyCoord",
            "",
            "# --- Step 1: Login ---",
            f"service = {service_class_name}()",
            "# Replace with your actual credentials",
            "service.login('your_username')",
            "",
            "# --- Step 2: Prepare Query Parameters ---"
        ]

        # Reconstruct kwargs for the example
        param_lines = []
        for key, value in kwargs.items():
            if key == 'auto_save': continue # Not part of the astroquery call
            if isinstance(value, str):
                param_lines.append(f"    {key}='{value}'")
            else:
                param_lines.append(f"    {key}={value}")
        
        param_str = ",\n".join(param_lines)
        
        # Use auto-detected query type if needed
        final_query_type = query_type
        if final_query_type == 'auto':
            try:
                final_query_type = self._detect_query_type(service_name, kwargs)
            except ValueError:
                final_query_type = "[could not auto-detect, please specify]"

        script_lines.append(f"# --- Step 3: Run Query ---")
        script_lines.append(f"results = service.{final_query_type}(")
        script_lines.append(param_str)
        script_lines.append(")")
        script_lines.append("")
        script_lines.append("print(results)")

        script_text = "\\n".join(script_lines)
        help_text = (
            f"AUTHENTICATION REQUIRED for service '{service_name}'.\\n\\n"
            "This service requires a login, and automatic authentication is not yet implemented in this tool.\\n"
            "To proceed, please run the following Python code in your own environment with your credentials:\\n\\n"
            "-------------------- PYTHON SCRIPT --------------------\\n"
            f"{script_text}\\n"
            "-------------------------------------------------------"
        )
        
        return {
            'status': 'auth_required',
            'help': help_text
        }

    def _generate_error_help(self, service_name: str, query_type: str, exception: Exception) -> str:
        """Generate helpful error messages."""
        try:
            service_details = self.get_service_details(service_name)
            capabilities = service_details.get('capabilities', [])
            examples = service_details.get('example_queries', [])
            
            help_text = f"The query failed for service '{service_name}' while attempting method '{query_type}'.\n"
            help_text += f"Error: {exception}\n\n"
            help_text += f"Available query methods for this service are: {', '.join(capabilities)}\n"
            
            if examples:
                help_text += "Here are some example queries for this service:\n"
                for ex in examples:
                    help_text += f"- {ex['description']}: `{ex['query']}`\n"
            
            help_text += "\nTip: Ensure your parameters match the requirements of the query method. You can specify a `query_type` directly to bypass auto-detection."
            return help_text
        except Exception as e:
            return f"An error occurred while generating help: {e}"

    def get_service(self, service_name: str):
        """Get or create a service instance."""
        if service_name not in self._services:
            raise ValueError(f"Unknown service: {service_name}")
        
        service_info = self._services[service_name]
        
        # Lazy instantiation
        if service_info['instance'] is None:
            try:
                service_info['instance'] = service_info['class']()
            except:
                # Some services might need different instantiation
                service_info['instance'] = service_info['class']
        
        return service_info['instance'] 