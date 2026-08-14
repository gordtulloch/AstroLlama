"""
title: Telescopius Astronomy Planner
description: >
  Tools for observation planning via the Telescopius RESTful API v2.2.
  Covers:
    - Solar system rise/transit/set times (Sun, Moon, planets)
    - Advanced deep-sky target search by type, magnitude and altitude
    - Tonight's curated "best targets" highlights
    - User observation lists (list all / fetch by ID)
    - User telescope/equipment inventory
    - Astronomy quote of the day
    - Latest global astronomy news

  Setup: Set TELESCOPIUS_API_KEY in the Valves (or via the tool_valves store).
  Observer location is read automatically from OBSERVER_* environment variables.
  Get a free API key at https://api.telescopius.com/

author: AstroLlama
version: 1.2.0
"""
from __future__ import annotations

import os
from typing import Optional

from pydantic import BaseModel, Field


class Tools:
    """OpenWebUI-compatible Telescopius observation planning tools."""

    class Valves(BaseModel):
        TELESCOPIUS_API_KEY: str = Field(
            default="",
            description=(
                "Your Telescopius API key.  "
                "Get one for free at https://api.telescopius.com/."
            ),
        )
        OBSERVER_LAT: float = Field(
            default=49.8951,
            description="Observer latitude in decimal degrees (default: Winnipeg, MB).",
        )
        OBSERVER_LON: float = Field(
            default=-97.1384,
            description="Observer longitude in decimal degrees (default: Winnipeg, MB).",
        )
        OBSERVER_TIMEZONE: str = Field(
            default="America/Winnipeg",
            description="Observer IANA timezone name (default: America/Winnipeg).",
        )

    def __init__(self) -> None:
        self.valves = self.Valves(
            TELESCOPIUS_API_KEY=os.environ.get("TELESCOPIUS_API_KEY", ""),
            OBSERVER_LAT=float(os.environ.get("OBSERVER_LAT", "49.8951")),
            OBSERVER_LON=float(os.environ.get("OBSERVER_LON", "-97.1384")),
            OBSERVER_TIMEZONE=os.environ.get("OBSERVER_TIMEZONE", "America/Winnipeg"),
        )

    # ── private helpers ───────────────────────────────────────────────────────

    def _api_key(self) -> str | None:
        key = (self.valves.TELESCOPIUS_API_KEY or "").strip()
        return key if key else None

    def _no_key_msg(self) -> str:
        return (
            "Telescopius API key not configured.  "
            "Set TELESCOPIUS_API_KEY in the tool Valves or as an environment variable."
        )

    def _ds(self) -> dict:
        try:
            from ..data_sources.telescopius import (
                get_solar_system_times,
                search_targets,
                get_target_highlights,
                get_target_lists,
                get_target_list_by_id,
                get_equipment,
                get_quote_of_day,
                get_global_news,
            )
        except ImportError:
            from data_sources.telescopius import (
                get_solar_system_times,
                search_targets,
                get_target_highlights,
                get_target_lists,
                get_target_list_by_id,
                get_equipment,
                get_quote_of_day,
                get_global_news,
            )
        return {
            "solar_system_times": get_solar_system_times,
            "search_targets": search_targets,
            "highlights": get_target_highlights,
            "lists": get_target_lists,
            "list_by_id": get_target_list_by_id,
            "equipment": get_equipment,
            "quote": get_quote_of_day,
            "news": get_global_news,
        }

    # ── public tools ──────────────────────────────────────────────────────────

    async def telescopius_solar_system_times(
        self,
        date: Optional[str] = None,
    ) -> str:
        """Get rise, transit and set times for the Sun, Moon and all planets
        for the observer's configured location.

        ALWAYS call this tool when the user asks which planets are visible,
        are up tonight, or asks about rise/transit/set times for the Sun,
        Moon or any planet.  DO NOT answer from memory.
        The observer location is set automatically — no coordinates needed.

        :param date: Optional date as YYYY-MM-DD.  Omit for today/tonight.
        :returns: Formatted table of rise/transit/set times for all solar system bodies.
        """
        key = self._api_key()
        if not key:
            return self._no_key_msg()
        return await self._ds()["solar_system_times"](
            lat=self.valves.OBSERVER_LAT,
            lon=self.valves.OBSERVER_LON,
            timezone=self.valves.OBSERVER_TIMEZONE,
            api_key=key,
            date=date,
        )

    async def telescopius_tonight_highlights(
        self,
        min_altitude: Optional[int] = None,
        date: Optional[str] = None,
    ) -> str:
        """Get tonight's curated highlights — the most popular, best-placed
        deep-sky targets for the observer's location.

        ALWAYS call this tool immediately when the user asks any of:
        'what should I look at tonight?', 'what's visible tonight?',
        'what are the best objects to observe?', 'what can I see tonight?',
        'what planets or objects are up tonight?', 'what is viewable today/tonight?'.
        DO NOT answer from memory — call this tool and present its results.
        The observer location is set automatically — no coordinates needed.

        :param min_altitude: Minimum altitude above horizon in degrees (optional).
        :param date: Optional date as YYYY-MM-DD.  Omit for tonight.
        :returns: Curated list of recommended targets with rise/set times.
        """
        key = self._api_key()
        if not key:
            return self._no_key_msg()
        return await self._ds()["highlights"](
            lat=self.valves.OBSERVER_LAT,
            lon=self.valves.OBSERVER_LON,
            timezone=self.valves.OBSERVER_TIMEZONE,
            api_key=key,
            min_altitude=min_altitude,
            date=date,
        )

    async def telescopius_search_targets(
        self,
        object_types: Optional[str] = None,
        min_altitude: Optional[int] = None,
        max_magnitude: Optional[float] = None,
        min_magnitude: Optional[float] = None,
        date: Optional[str] = None,
        limit: int = 15,
    ) -> str:
        """Search for astronomical targets (DSOs, stars, etc.) visible tonight,
        with optional filters by object type, altitude and magnitude.

        ALWAYS call this tool when the user wants a filtered list of targets —
        e.g. 'show me galaxies brighter than mag 10' or 'planetary nebulae
        above 30 degrees'.  DO NOT answer with guesses.
        The observer location is set automatically — no coordinates needed.

        :param object_types: Comma-separated type codes (omit for all types).
               Codes: galaxy, eneb (emission nebula), rneb (reflection nebula),
               pneb (planetary nebula), snr, glob (globular cluster),
               opnc (open cluster), dbl* (double star).
        :param min_altitude: Minimum altitude above horizon in degrees (e.g. 20).
        :param max_magnitude: Faintest visual magnitude to include (e.g. 10.0).
        :param min_magnitude: Brightest visual magnitude to include.
        :param date: Optional date as YYYY-MM-DD.  Omit for tonight.
        :param limit: Maximum results to return, 1-50 (default 15).
        :returns: Formatted list of matching targets with observability data.
        """
        key = self._api_key()
        if not key:
            return self._no_key_msg()
        return await self._ds()["search_targets"](
            lat=self.valves.OBSERVER_LAT,
            lon=self.valves.OBSERVER_LON,
            timezone=self.valves.OBSERVER_TIMEZONE,
            api_key=key,
            object_types=object_types,
            min_altitude=min_altitude,
            max_magnitude=max_magnitude,
            min_magnitude=min_magnitude,
            date=date,
            limit=limit,
        )

    async def telescopius_observation_lists(self) -> str:
        """Fetch all the user's saved observation lists from Telescopius.

        Call this when the user asks to see their Telescopius lists or saved
        target collections.

        :returns: Formatted list of the user's observation lists with IDs and target counts.
        """
        key = self._api_key()
        if not key:
            return self._no_key_msg()
        return await self._ds()["lists"](api_key=key)

    async def telescopius_observation_list_targets(
        self,
        list_id: str,
    ) -> str:
        """Fetch the targets inside a specific Telescopius observation list.

        Call telescopius_observation_lists first to discover valid list IDs.

        :param list_id: Numeric or string ID of the Telescopius list.
        :returns: Formatted list of targets in the specified observation list.
        """
        key = self._api_key()
        if not key:
            return self._no_key_msg()
        return await self._ds()["list_by_id"](
            list_id=list_id,
            api_key=key,
            lat=self.valves.OBSERVER_LAT,
            lon=self.valves.OBSERVER_LON,
            timezone=self.valves.OBSERVER_TIMEZONE,
        )

    async def telescopius_my_equipment(self) -> str:
        """Fetch the user's telescopes and equipment registered on Telescopius.

        Call this when the user asks about their telescope setup, equipment, or
        what gear is registered on their Telescopius account.

        :returns: Formatted list of telescopes, cameras and accessories.
        """
        key = self._api_key()
        if not key:
            return self._no_key_msg()
        return await self._ds()["equipment"](api_key=key)

    async def telescopius_quote_of_the_day(self) -> str:
        """Get today's astronomy quote of the day from Telescopius.

        Call this when the user asks for an astronomy quote or something
        inspiring about the cosmos.

        :returns: The astronomy quote and its author.
        """
        key = self._api_key()
        if not key:
            return self._no_key_msg()
        return await self._ds()["quote"](api_key=key)

    async def telescopius_astronomy_news(self, limit: int = 10) -> str:
        """Get the latest astronomy news from global feeds via Telescopius.

        Call this when the user asks for astronomy news, recent space news,
        or what's happening in the astronomy world.

        :param limit: Maximum number of news articles to return (1-30, default 10).
        :returns: Formatted list of recent astronomy headlines with sources and links.
        """
        key = self._api_key()
        if not key:
            return self._no_key_msg()
        return await self._ds()["news"](api_key=key, limit=min(int(limit), 30))
