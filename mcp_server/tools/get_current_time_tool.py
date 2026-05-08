from __future__ import annotations


class Tools:
    """OpenWebUI-compatible current-time tool."""

    async def get_current_time(self, location: str) -> str:
        """Get the current local time and date for a location.

        :param location: City or place name to resolve to a timezone.
        :returns: A formatted local-time report including timezone and coordinates.
        """
        try:
            from ..data_sources.open_meteo import get_current_time
        except ImportError:
            from data_sources.open_meteo import get_current_time
        return await get_current_time(location=location)
