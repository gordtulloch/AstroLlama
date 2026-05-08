from __future__ import annotations


class Tools:
    """OpenWebUI-compatible constellation map generation tool."""

    async def generate_constellation_map(self, constellation: str) -> str:
        """Generate a detailed map for a specific constellation.

        :param constellation: Full name or IAU abbreviation of the constellation.
        :returns: API file URL and metadata for the rendered PNG constellation map.
        """
        try:
            from ..data_sources.generate_constellation_map import generate_constellation_map
        except ImportError:
            from data_sources.generate_constellation_map import generate_constellation_map
        return await generate_constellation_map(constellation=constellation)
