from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field


class Tools:
    """OpenWebUI-compatible weather tool."""

    class UserValves(BaseModel):
        """Per-user configuration fields for weather output preferences."""

        temperature_unit: Literal["celsius", "fahrenheit"] = Field(
            default="celsius",
            description="Preferred unit for weather temperature values.",
        )
        wind_speed_unit: Literal["kmh", "mph", "ms", "kn"] = Field(
            default="kmh",
            description="Preferred unit for weather wind speed values.",
        )

    def __init__(self, user_valves: "Tools.UserValves | None" = None) -> None:
        self.user_valves = user_valves or self.UserValves()

    async def get_weather(
        self,
        latitude: float | None = None,
        longitude: float | None = None,
        location: str | None = None,
        location_name: str | None = None,
        temperature_unit: Literal["celsius", "fahrenheit"] | None = None,
        wind_speed_unit: Literal["kmh", "mph", "ms", "kn"] | None = None,
    ) -> str:
        """Get current weather from Open-Meteo.

        Provide either latitude+longitude or a location string.

        :param latitude: Latitude in decimal degrees.
        :param longitude: Longitude in decimal degrees.
        :param location: City/place name to geocode when coordinates are omitted.
        :param location_name: Optional display label for the response.
        :param temperature_unit: Optional temperature unit override.
        :param wind_speed_unit: Optional wind speed unit override.
        :returns: A formatted current-conditions weather report.
        """
        try:
            from ..data_sources.open_meteo import (
                fetch_open_meteo_geocode_results,
                fetch_open_meteo_weather,
            )
        except ImportError:
            from data_sources.open_meteo import (
                fetch_open_meteo_geocode_results,
                fetch_open_meteo_weather,
            )

        if latitude is None or longitude is None:
            if not location:
                return "Error: Provide either latitude/longitude or a location string."

            results, _ = await fetch_open_meteo_geocode_results(location=location, count=1)
            if not results:
                return f"No results found for '{location}'."

            first = results[0]
            latitude = first["latitude"]
            longitude = first["longitude"]

            if not location_name:
                name_parts = [first.get("name", "")]
                for field in ("admin1", "country"):
                    value = first.get(field)
                    if value and value != name_parts[0]:
                        name_parts.append(value)
                location_name = ", ".join([p for p in name_parts if p])

        temp_unit = temperature_unit or self.user_valves.temperature_unit
        wind_unit = wind_speed_unit or self.user_valves.wind_speed_unit
        return await fetch_open_meteo_weather(
            latitude=latitude,
            longitude=longitude,
            location_name=location_name,
            temperature_unit=temp_unit,
            wind_speed_unit=wind_unit,
        )
