"""Shared pytest fixtures for the Loop IQ Open-Meteo Lakehouse test suite."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List

import pytest

from src.config.settings import PipelineSettings
from src.transformation.spark_session import LocalLakehouseSession, build_spark_session


@pytest.fixture(scope="session")
def spark() -> LocalLakehouseSession:
    """Session-scoped Lakehouse session."""
    session = build_spark_session(app_name="loopiq-lakehouse-pytest")
    yield session
    session.stop()


@pytest.fixture()
def isolated_settings(tmp_path: Path) -> PipelineSettings:
    """Create isolated PipelineSettings pointing all storage and logs to tmp_path."""
    loc_file = tmp_path / "config" / "locations.json"
    loc_file.parent.mkdir(parents=True, exist_ok=True)

    locations_data = {
        "schema_version": "1.0",
        "locations": [
            {
                "location_id": "AU-NSW-SYD-01",
                "location_name": "Sydney Metro Care Hub",
                "state_code": "NSW",
                "latitude": -33.8688,
                "longitude": 151.2093,
                "timezone_name": "Australia/Sydney",
                "climate_risk_tier": "STANDARD",
            },
            {
                "location_id": "AU-NT-DRW-01",
                "location_name": "Darwin Top End Care Hub",
                "state_code": "NT",
                "latitude": -12.4634,
                "longitude": 130.8456,
                "timezone_name": "Australia/Darwin",
                "climate_risk_tier": "TROPICAL",
            },
        ],
    }
    loc_file.write_text(json.dumps(locations_data, indent=2), encoding="utf-8")

    settings = PipelineSettings(
        APP_ENV="test",
        LOG_DIR=tmp_path / "logs",
        PIPELINE_STEP_AUDIT_FILE=tmp_path / "logs" / "pipeline_steps_audit.jsonl",
        LOCATIONS_CONFIG_PATH=loc_file,
        DATA_ROOT_DIR=tmp_path / "data",
        LANDING_RAW_JSON_DIR=tmp_path / "data" / "landing" / "raw_json",
        OPS_PATH=tmp_path / "data" / "lakehouse" / "ops",
        BRONZE_PATH=tmp_path / "data" / "lakehouse" / "bronze",
        SILVER_PATH=tmp_path / "data" / "lakehouse" / "silver",
        GOLD_PATH=tmp_path / "data" / "lakehouse" / "gold",
        WEATHER_PAST_DAYS=1,
        WEATHER_FORECAST_DAYS=1,
    )
    settings.ensure_directories()
    return settings


@pytest.fixture()
def sample_open_meteo_batch_payload() -> List[Dict[str, Any]]:
    """Return a realistic 2-location Open-Meteo response batch (Sydney +11h & Darwin +9.5h)."""
    return [
        # Location 0: Sydney (Australia/Sydney, UTC+11 = 39600s)
        {
            "latitude": -33.85,
            "longitude": 151.20,
            "elevation": 42.0,
            "timezone": "Australia/Sydney",
            "timezone_abbreviation": "AEDT",
            "utc_offset_seconds": 39600,
            "minutely_15_units": {
                "time": "iso8601",
                "temperature_2m": "°C",
                "precipitation": "mm",
                "wind_speed_10m": "km/h",
                "weather_code": "wmo code",
            },
            "minutely_15": {
                "time": ["2026-10-23T01:00", "2026-10-23T01:15"],
                "temperature_2m": [19.5, 19.4],
                "precipitation": [0.0, 0.2],
                "wind_speed_10m": [14.0, 15.2],
                "weather_code": [1, 2],
            },
            "hourly_units": {
                "time": "iso8601",
                "temperature_2m": "°C",
                "relative_humidity_2m": "%",
                "apparent_temperature": "°C",
                "precipitation_probability": "%",
                "precipitation": "mm",
                "weather_code": "wmo code",
                "surface_pressure": "hPa",
                "wind_speed_10m": "km/h",
                "uv_index": "",
            },
            "hourly": {
                "time": ["2026-10-22T14:00", "2026-10-23T01:00", "2026-10-24T14:00"],
                "temperature_2m": [22.0, 19.5, 25.0],
                "relative_humidity_2m": [60.0, 68.0, 55.0],
                "apparent_temperature": [21.5, 19.0, 25.5],
                "precipitation_probability": [10.0, 20.0, 5.0],
                "precipitation": [0.0, 0.2, 0.0],
                "weather_code": [1, 2, 0],
                "surface_pressure": [1015.0, 1014.2, 1016.5],
                "wind_speed_10m": [18.0, 14.0, 20.0],
                "uv_index": [6.5, 0.0, 7.5],
            },
            "daily_units": {
                "time": "iso8601",
                "weather_code": "wmo code",
                "temperature_2m_max": "°C",
                "temperature_2m_min": "°C",
                "apparent_temperature_max": "°C",
                "apparent_temperature_min": "°C",
                "precipitation_sum": "mm",
                "uv_index_max": "",
                "wind_speed_10m_max": "km/h",
            },
            "daily": {
                "time": ["2026-10-22", "2026-10-23", "2026-10-24"],
                "weather_code": [1, 2, 0],
                "temperature_2m_max": [24.0, 23.5, 26.0],
                "temperature_2m_min": [15.0, 16.0, 16.5],
                "apparent_temperature_max": [24.2, 23.0, 26.5],
                "apparent_temperature_min": [14.5, 15.2, 16.0],
                "precipitation_sum": [0.0, 0.8, 0.0],
                "uv_index_max": [6.8, 7.1, 7.9],
                "wind_speed_10m_max": [25.0, 22.0, 28.0],
            },
        },
        # Location 1: Darwin (Australia/Darwin, UTC+9:30 = 34200s) with Heatwave & WMO 97 Storm!
        {
            "latitude": -12.45,
            "longitude": 130.85,
            "elevation": 31.0,
            "timezone": "Australia/Darwin",
            "timezone_abbreviation": "ACST",
            "utc_offset_seconds": 34200,
            "minutely_15_units": {"time": "iso8601"},
            "minutely_15": {
                "time": ["2026-10-23T14:00", "2026-10-23T14:15"],
                "temperature_2m": [36.2, 36.5],
                "precipitation": [4.5, 6.0],
                "wind_speed_10m": [62.0, 68.0],
                "weather_code": [95, 97],  # WMO 97 = Heavy thunderstorm without hail (Fix 2.2)
            },
            "hourly_units": {"time": "iso8601"},
            "hourly": {
                "time": ["2026-10-22T14:00", "2026-10-23T14:00", "2026-10-24T14:00"],
                "temperature_2m": [34.0, 36.5, 37.2],
                "relative_humidity_2m": [70.0, 75.0, 72.0],
                "apparent_temperature": [38.0, 41.2, 42.0],
                "precipitation_probability": [40.0, 90.0, 80.0],
                "precipitation": [1.2, 18.5, 12.0],
                "weather_code": [80, 97, 95],
                "surface_pressure": [1008.0, 1005.5, 1006.0],
                "wind_speed_10m": [32.0, 68.0, 55.0],
                "uv_index": [9.5, 11.2, 10.8],
            },
            "daily_units": {"time": "iso8601"},
            "daily": {
                "time": ["2026-10-22", "2026-10-23", "2026-10-24"],
                "weather_code": [80, 97, 95],
                "temperature_2m_max": [34.5, 36.8, 37.5],
                "temperature_2m_min": [26.0, 27.2, 27.5],
                "apparent_temperature_max": [38.5, 41.5, 42.3],
                "apparent_temperature_min": [28.0, 29.5, 30.0],
                "precipitation_sum": [5.0, 35.0, 20.0],
                "uv_index_max": [9.8, 11.5, 11.0],
                "wind_speed_10m_max": [38.0, 72.0, 58.0],
            },
        },
    ]
