from __future__ import annotations

from pydantic import BaseModel, Field


class Tools:
    """OpenWebUI-compatible SIMBAD multi-result search tool."""

    class Valves(BaseModel):
        """Global configuration fields for SIMBAD search behavior."""

        simbad_default_limit: int = Field(
            default=10,
            ge=1,
            le=100,
            description="Default row limit for SIMBAD multi-result searches.",
        )

    def __init__(self, valves: "Tools.Valves | None" = None) -> None:
        self.valves = valves or self.Valves()

    async def simbad_search(self, query: str, limit: int | None = None) -> str:
        """Search SIMBAD for multiple objects matching a natural-language query.

        Use this for list/browse queries such as brightest stars or objects in a constellation.

        :param query: Natural-language multi-result query.
        :param limit: Optional maximum number of rows; defaults to configured valve value.
        :returns: A formatted list of matching astronomical objects.
        """
        try:
            from ..data_sources.simbad_search import simbad_search
        except ImportError:
            from data_sources.simbad_search import simbad_search

        effective_limit = limit if limit is not None else self.valves.simbad_default_limit
        return await simbad_search(query=query, limit=effective_limit)
