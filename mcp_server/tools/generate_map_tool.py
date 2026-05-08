from __future__ import annotations


class Tools:
    """OpenWebUI-compatible all-sky map generation tool."""

    async def generate_map(
        self,
        lat: float,
        lon: float,
        location_name: str = "Unknown location",
        datetime_str: str = "now",
        timezone: str = "UTC",
    ) -> str:
        """Generate an all-sky zenith chart for a location and time.

        :param lat: Observer latitude in decimal degrees.
        :param lon: Observer longitude in decimal degrees.
        :param location_name: Human-readable location name shown on the map.
        :param datetime_str: ISO timestamp or 'now'.
        :param timezone: IANA timezone identifier used with local times.
        :returns: API file URL and metadata for the rendered PNG sky map.
        """
        try:
            from ..data_sources.generate_map import generate_map
        except ImportError:
            from data_sources.generate_map import generate_map
        return await generate_map(
            lat=lat,
            lon=lon,
            location_name=location_name,
            datetime_str=datetime_str,
            timezone=timezone,
        )
