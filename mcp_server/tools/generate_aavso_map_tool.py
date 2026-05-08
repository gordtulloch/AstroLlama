from __future__ import annotations


class Tools:
    """OpenWebUI-compatible AAVSO finder chart tool."""

    async def generate_aavso_map(
        self,
        star: str | None = None,
        ra: float | None = None,
        dec: float | None = None,
        fov: float = 60,
        maglimit: float = 14.5,
    ) -> str:
        """Generate an AAVSO finder chart for a variable star.

        :param star: Variable star name. Provide this or both ra and dec.
        :param ra: Right ascension in decimal degrees.
        :param dec: Declination in decimal degrees.
        :param fov: Field of view in arcminutes.
        :param maglimit: Faint magnitude limit for comparison stars.
        :returns: API file URL and metadata for the rendered finder chart.
        """
        try:
            from ..data_sources.generate_aavso_map import generate_aavso_map
        except ImportError:
            from data_sources.generate_aavso_map import generate_aavso_map
        return await generate_aavso_map(
            star=star,
            ra=ra,
            dec=dec,
            fov=fov,
            maglimit=maglimit,
        )
