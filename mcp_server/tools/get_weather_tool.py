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
        latitude: float,
        longitude: float,
        location_name: str | None = None,
        temperature_unit: Literal["celsius", "fahrenheit"] | None = None,
        wind_speed_unit: Literal["kmh", "mph", "ms", "kn"] | None = None,
    ) -> str:
        """Get current weather from Open-Meteo for a coordinate.

        :param latitude: Latitude in decimal degrees.
        :param longitude: Longitude in decimal degrees.
        :param location_name: Optional display label for the response.
        :param temperature_unit: Optional temperature unit override.
        :param wind_speed_unit: Optional wind speed unit override.
        :returns: A formatted current-conditions weather report.
        """
        try:
            from ..data_sources.open_meteo import fetch_open_meteo_weather
        except ImportError:
            from data_sources.open_meteo import fetch_open_meteo_weather

        temp_unit = temperature_unit or self.user_valves.temperature_unit
        wind_unit = wind_speed_unit or self.user_valves.wind_speed_unit
        return await fetch_open_meteo_weather(
            latitude=latitude,
            longitude=longitude,
            location_name=location_name,
            temperature_unit=temp_unit,
            wind_speed_unit=wind_unit,
        )
