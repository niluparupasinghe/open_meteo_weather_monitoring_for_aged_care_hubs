"""Pydantic .env-first configuration management for the Open-Meteo Lakehouse pipeline."""

from __future__ import annotations

import json
from functools import lru_cache
from pathlib import Path
from typing import List, Optional

from pydantic import BaseModel, Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class LocationConfig(BaseModel):
    """Represents a monitored Australian Aged Care hub from config/locations.json."""

    location_id: str = Field(..., min_length=2)
    location_name: str = Field(..., min_length=2)
    state_code: str = Field(..., min_length=2, max_length=5)
    latitude: float = Field(..., ge=-90.0, le=90.0)
    longitude: float = Field(..., ge=-180.0, le=180.0)
    timezone_name: str = Field(..., min_length=2)
    climate_risk_tier: str = Field(default="STANDARD")


class PipelineSettings(BaseSettings):
    """Centralized, validated .env configuration for the entire Medallion pipeline."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # 1. Environment, Logging & Repair Configuration
    app_env: str = Field(default="local", alias="APP_ENV")
    log_level: str = Field(default="INFO", alias="LOG_LEVEL")
    log_dir: Path = Field(default=Path("./logs"), alias="LOG_DIR")
    pipeline_step_audit_file: Path = Field(
        default=Path("./logs/pipeline_steps_audit.jsonl"),
        alias="PIPELINE_STEP_AUDIT_FILE",
    )
    pipeline_name: str = Field(default="loopiq_open_meteo_lakehouse", alias="PIPELINE_NAME")

    # 2. Open-Meteo API & Rate Limit Governance
    open_meteo_base_url: str = Field(
        default="https://api.open-meteo.com/v1/forecast",
        alias="OPEN_METEO_BASE_URL",
    )
    open_meteo_api_key: Optional[str] = Field(default=None, alias="OPEN_METEO_API_KEY")
    api_timeout_seconds: int = Field(default=30, ge=5, le=120, alias="API_TIMEOUT_SECONDS")
    api_max_retries: int = Field(default=5, ge=0, le=10, alias="API_MAX_RETRIES")
    api_retry_backoff_factor: float = Field(
        default=1.5, ge=0.1, le=10.0, alias="API_RETRY_BACKOFF_FACTOR"
    )
    api_max_locations_per_batch: int = Field(
        default=50, ge=1, le=100, alias="API_MAX_LOCATIONS_PER_BATCH"
    )
    api_rate_limit_per_minute: float = Field(
        default=600.0, gt=0.0, alias="API_RATE_LIMIT_PER_MINUTE"
    )
    api_rate_limit_per_hour: float = Field(default=5000.0, gt=0.0, alias="API_RATE_LIMIT_PER_HOUR")
    api_rate_limit_per_day: float = Field(default=10000.0, gt=0.0, alias="API_RATE_LIMIT_PER_DAY")
    api_safety_buffer_ratio: float = Field(
        default=0.85, gt=0.1, le=1.0, alias="API_SAFETY_BUFFER_RATIO"
    )
    api_use_offline_cache: bool = Field(default=False, alias="API_USE_OFFLINE_CACHE")

    # 3. Weather Domain Parameters (15-Minute, Hourly & Daily)
    weather_past_days: int = Field(default=7, ge=0, le=92, alias="WEATHER_PAST_DAYS")
    weather_forecast_days: int = Field(default=7, ge=1, le=16, alias="WEATHER_FORECAST_DAYS")
    weather_temperature_unit: str = Field(default="celsius", alias="WEATHER_TEMPERATURE_UNIT")
    weather_wind_speed_unit: str = Field(default="kmh", alias="WEATHER_WIND_SPEED_UNIT")
    weather_precipitation_unit: str = Field(default="mm", alias="WEATHER_PRECIPITATION_UNIT")
    weather_cell_selection: str = Field(default="land", alias="WEATHER_CELL_SELECTION")
    weather_minutely_15_vars: str = Field(
        default="temperature_2m,precipitation,wind_speed_10m,weather_code",
        alias="WEATHER_MINUTELY_15_VARS",
    )
    weather_hourly_vars: str = Field(
        default=(
            "temperature_2m,relative_humidity_2m,apparent_temperature,"
            "precipitation_probability,precipitation,weather_code,"
            "surface_pressure,wind_speed_10m,uv_index"
        ),
        alias="WEATHER_HOURLY_VARS",
    )
    weather_daily_vars: str = Field(
        default=(
            "weather_code,temperature_2m_max,temperature_2m_min,"
            "apparent_temperature_max,apparent_temperature_min,"
            "precipitation_sum,uv_index_max,wind_speed_10m_max"
        ),
        alias="WEATHER_DAILY_VARS",
    )
    locations_config_path: Path = Field(
        default=Path("config/locations.json"), alias="LOCATIONS_CONFIG_PATH"
    )

    # 4. Landing Zone, Lakehouse Storage & Spark / Delta Configuration
    data_root_dir: Path = Field(default=Path("./data"), alias="DATA_ROOT_DIR")
    landing_raw_json_dir: Path = Field(
        default=Path("./data/landing/raw_json"), alias="LANDING_RAW_JSON_DIR"
    )
    ops_path: Path = Field(default=Path("./data/lakehouse/ops"), alias="OPS_PATH")
    bronze_path: Path = Field(default=Path("./data/lakehouse/bronze"), alias="BRONZE_PATH")
    silver_path: Path = Field(default=Path("./data/lakehouse/silver"), alias="SILVER_PATH")
    gold_path: Path = Field(default=Path("./data/lakehouse/gold"), alias="GOLD_PATH")
    spark_master: str = Field(default="local[*]", alias="SPARK_MASTER")
    spark_driver_memory: str = Field(default="2g", alias="SPARK_DRIVER_MEMORY")
    delta_enable_cdf: bool = Field(default=True, alias="DELTA_ENABLE_CDF")

    # 5. Data Quality, Archiving & Gold Alerting Thresholds
    dq_min_valid_temp_c: float = Field(default=-50.0, alias="DQ_MIN_VALID_TEMP_C")
    dq_max_valid_temp_c: float = Field(default=60.0, alias="DQ_MAX_VALID_TEMP_C")
    dq_max_valid_precip_mm: float = Field(default=1000.0, alias="DQ_MAX_VALID_PRECIP_MM")
    dq_max_valid_wind_kmh: float = Field(default=400.0, alias="DQ_MAX_VALID_WIND_KMH")
    dq_min_surface_pressure_hpa: float = Field(default=850.0, alias="DQ_MIN_SURFACE_PRESSURE_HPA")
    dq_max_surface_pressure_hpa: float = Field(default=1100.0, alias="DQ_MAX_SURFACE_PRESSURE_HPA")
    dq_max_valid_uv_index: float = Field(default=25.0, alias="DQ_MAX_VALID_UV_INDEX")

    alert_heatwave_temp_c: float = Field(default=35.0, alias="ALERT_HEATWAVE_TEMP_C")
    alert_cold_snap_temp_c: float = Field(default=5.0, alias="ALERT_COLD_SNAP_TEMP_C")
    alert_high_wind_kmh: float = Field(default=60.0, alias="ALERT_HIGH_WIND_KMH")
    alert_high_uv_index: float = Field(default=8.0, alias="ALERT_HIGH_UV_INDEX")

    archive_hot_retention_days: int = Field(default=30, alias="ARCHIVE_HOT_RETENTION_DAYS")
    archive_warm_retention_days: int = Field(default=90, alias="ARCHIVE_WARM_RETENTION_DAYS")
    archive_cold_expiration_days: int = Field(default=2555, alias="ARCHIVE_COLD_EXPIRATION_DAYS")

    @field_validator("weather_cell_selection")
    @classmethod
    def validate_cell_selection(cls, v: str) -> str:
        allowed = {"land", "sea", "nearest"}
        if v not in allowed:
            raise ValueError(f"WEATHER_CELL_SELECTION must be one of {allowed}, got '{v}'")
        return v

    @property
    def minutely_15_var_list(self) -> List[str]:
        return [x.strip() for x in self.weather_minutely_15_vars.split(",") if x.strip()]

    @property
    def hourly_var_list(self) -> List[str]:
        return [x.strip() for x in self.weather_hourly_vars.split(",") if x.strip()]

    @property
    def daily_var_list(self) -> List[str]:
        return [x.strip() for x in self.weather_daily_vars.split(",") if x.strip()]

    @property
    def total_requested_variables_count(self) -> int:
        return (
            len(self.minutely_15_var_list)
            + len(self.hourly_var_list)
            + len(self.daily_var_list)
        )

    @property
    def total_requested_days(self) -> int:
        return self.weather_past_days + self.weather_forecast_days

    def load_locations(self) -> List[LocationConfig]:
        """Load and validate monitored care hub locations from JSON config."""
        if not self.locations_config_path.exists():
            raise FileNotFoundError(
                f"Locations configuration file not found at {self.locations_config_path.resolve()}"
            )
        raw = json.loads(self.locations_config_path.read_text(encoding="utf-8"))
        locations_raw = raw.get("locations", [])
        if not locations_raw:
            raise ValueError(f"No locations defined in {self.locations_config_path}")
        return [LocationConfig(**loc) for loc in locations_raw]

    def ensure_directories(self) -> None:
        """Create required runtime directories for logs, landing zone, and lakehouse layers."""
        for directory in (
            self.log_dir,
            self.pipeline_step_audit_file.parent,
            self.landing_raw_json_dir,
            self.ops_path,
            self.bronze_path,
            self.silver_path,
            self.gold_path,
        ):
            directory.mkdir(parents=True, exist_ok=True)


@lru_cache(maxsize=1)
def get_settings() -> PipelineSettings:
    """Return cached singleton PipelineSettings instance."""
    settings = PipelineSettings()
    settings.ensure_directories()
    return settings
