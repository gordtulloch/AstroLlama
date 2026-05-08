from __future__ import annotations


class Tools:
    """OpenWebUI-compatible variable-star comparison table tool."""

    async def variable_comparison_stars(
        self,
        star: str | None = None,
        ra: float | None = None,
        dec: float | None = None,
        fov: float = 60,
        maglimit: float = 14.5,
    ) -> str:
        """Fetch AAVSO comparison-star photometry for a variable star field.

        :param star: Variable star name. Provide this or both ra and dec.
        :param ra: Right ascension in decimal degrees.
        :param dec: Declination in decimal degrees.
        :param fov: Field of view in arcminutes.
        :param maglimit: Faintest comparison star magnitude to include.
        :returns: A formatted comparison-star table with photometric bands.
        """
        try:
            from ..data_sources.variable_comparison_stars import variable_comparison_stars
        except ImportError:
            from data_sources.variable_comparison_stars import variable_comparison_stars
        return await variable_comparison_stars(
            star=star,
            ra=ra,
            dec=dec,
            fov=fov,
            maglimit=maglimit,
        )
