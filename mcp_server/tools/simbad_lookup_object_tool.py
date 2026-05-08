from __future__ import annotations


class Tools:
    """OpenWebUI-compatible single-object SIMBAD lookup tool."""

    async def simbad_lookup_object(self, object_name: str) -> str:
        """Lookup a single SIMBAD object by identifier/name.

        Use this for one-object requests such as coordinates/properties of NGC 1015.

        :param object_name: Single object identifier or name (for example 'NGC 1015' or 'Betelgeuse').
        :returns: Coordinates and metadata for exactly one matched object.
        """
        try:
            from ..data_sources.simbad_search import simbad_lookup_object
        except ImportError:
            from data_sources.simbad_search import simbad_lookup_object
        return await simbad_lookup_object(object_name=object_name)
