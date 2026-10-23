"""WMO 4677 Weather Interpretation Code definitions for Open-Meteo (including Code 97)."""

from __future__ import annotations

from typing import Dict, Set

# Complete Open-Meteo WMO 4677 weather interpretation codes.
# Note: Global models covering Australia (BOM ACCESS-G / ECMWF) emit 95 and 97
# for thunderstorms (where 97 = Heavy thunderstorm without hail).
WMO_CODE_MAP: Dict[int, str] = {
    0: "Clear sky",
    1: "Mainly clear",
    2: "Partly cloudy",
    3: "Overcast",
    45: "Fog",
    48: "Depositing rime fog",
    51: "Drizzle: Light intensity",
    53: "Drizzle: Moderate intensity",
    55: "Drizzle: Dense intensity",
    56: "Freezing Drizzle: Light intensity",
    57: "Freezing Drizzle: Dense intensity",
    61: "Rain: Slight intensity",
    63: "Rain: Moderate intensity",
    65: "Rain: Heavy intensity",
    66: "Freezing Rain: Light intensity",
    67: "Freezing Rain: Heavy intensity",
    71: "Snow fall: Slight intensity",
    73: "Snow fall: Moderate intensity",
    75: "Snow fall: Heavy intensity",
    77: "Snow grains",
    80: "Rain showers: Slight",
    81: "Rain showers: Moderate",
    82: "Rain showers: Violent",
    85: "Snow showers: Slight",
    86: "Snow showers: Heavy",
    95: "Thunderstorm: Slight or moderate",
    96: "Thunderstorm with slight hail",
    97: "Thunderstorm: Heavy without hail",
    99: "Thunderstorm with heavy hail",
}

VALID_WMO_CODES: Set[int] = set(WMO_CODE_MAP.keys())

SEVERE_STORM_WMO_CODES: Set[int] = {82, 95, 96, 97, 99}


def get_wmo_description(code: int | None) -> str:
    """Return human-readable weather description for a WMO code."""
    if code is None:
        return "Unknown"
    return WMO_CODE_MAP.get(int(code), "Unknown")
