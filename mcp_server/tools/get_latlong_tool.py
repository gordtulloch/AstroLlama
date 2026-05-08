from __future__ import annotations


class Tools:
    """OpenWebUI-compatible geocoding tool."""

    async def get_latlong(self, location: str, count: int = 1) -> str:
        """Geocode a place name to latitude/longitude and timezone.

        :param location: City or place name to resolve (for example, 'Winnipeg, Manitoba').
        :param count: Maximum number of candidates to return (1-5).
        :returns: A formatted geocoding report with coordinates and timezone.
        """
        try:
            from ..data_sources.open_meteo import fetch_open_meteo_geocode
        except ImportError:
            from data_sources.open_meteo import fetch_open_meteo_geocode
        return await fetch_open_meteo_geocode(location=location, count=count)
