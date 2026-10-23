"""Batched Open-Meteo HTTP client with per-location timezones, retry backoff, and Landing Zone write."""

from __future__ import annotations

import json
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from src.config.settings import LocationConfig, PipelineSettings, get_settings
from src.ingestion.call_weight_ledger import CallWeightLedger
from src.utils.logger import get_logger

logger = get_logger(__name__)


class OpenMeteoClient:
    """Resilient, rate-limit-aware HTTP client for the Open-Meteo /v1/forecast API."""

    def __init__(
        self,
        settings: Optional[PipelineSettings] = None,
        ledger: Optional[CallWeightLedger] = None,
        session: Optional[requests.Session] = None,
    ) -> None:
        self.settings = settings or get_settings()
        self.ledger = ledger or CallWeightLedger(self.settings)
        self.session = session or self._build_retry_session()

    def _build_retry_session(self) -> requests.Session:
        """Configure requests Session with exponential backoff on 429 and 5xx errors."""
        sess = requests.Session()
        retry_strategy = Retry(
            total=self.settings.api_max_retries,
            connect=self.settings.api_max_retries,
            read=self.settings.api_max_retries,
            status=self.settings.api_max_retries,
            backoff_factor=self.settings.api_retry_backoff_factor,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=frozenset(["GET"]),
            respect_retry_after_header=True,
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        sess.mount("https://", adapter)
        sess.mount("http://", adapter)
        return sess

    def build_query_params(self, locations: List[LocationConfig]) -> Dict[str, Any]:
        """Build Open-Meteo query parameters with per-location local timezones (Fix 2.1)."""
        params: Dict[str, Any] = {
            "latitude": ",".join(str(loc.latitude) for loc in locations),
            "longitude": ",".join(str(loc.longitude) for loc in locations),
            "timezone": ",".join(loc.timezone_name for loc in locations),
            "past_days": self.settings.weather_past_days,
            "forecast_days": self.settings.weather_forecast_days,
            "temperature_unit": self.settings.weather_temperature_unit,
            "wind_speed_unit": self.settings.weather_wind_speed_unit,
            "precipitation_unit": self.settings.weather_precipitation_unit,
            "cell_selection": self.settings.weather_cell_selection,
            "timeformat": "iso8601",
            "minutely_15": self.settings.weather_minutely_15_vars,
            "hourly": self.settings.weather_hourly_vars,
            "daily": self.settings.weather_daily_vars,
        }
        if self.settings.open_meteo_api_key:
            params["apikey"] = self.settings.open_meteo_api_key
        return params

    @staticmethod
    def normalize_response_payload(raw_json: Any, expected_count: int) -> List[Dict[str, Any]]:
        """Normalize Open-Meteo response root (dict for 1 location vs list for N locations)."""
        if isinstance(raw_json, dict):
            if raw_json.get("error") is True:
                reason = raw_json.get("reason", "Unknown Open-Meteo API validation error")
                raise ValueError(f"Open-Meteo API returned error payload: {reason}")
            normalized = [raw_json]
        elif isinstance(raw_json, list):
            normalized = raw_json
        else:
            raise ValueError(f"Unexpected Open-Meteo JSON root type: {type(raw_json).__name__}")

        if len(normalized) != expected_count:
            raise ValueError(
                f"Expected {expected_count} location payloads from Open-Meteo, got {len(normalized)}"
            )
        return normalized

    def fetch_and_land_batch(
        self,
        locations: List[LocationConfig],
        pipeline_run_id: str,
    ) -> Dict[str, Any]:
        """Fetch weather data for a batch of locations, persist to Landing Zone, and log weight."""
        if not locations:
            raise ValueError("locations list cannot be empty")

        # Offline cache fallback check (does NOT write to ops.api_call_weight_ledger!)
        if self.settings.api_use_offline_cache:
            cached_files = sorted(self.settings.landing_raw_json_dir.rglob("batch_*.json"))
            if cached_files:
                latest_cache = cached_files[-1]
                logger.info("Using offline cached Landing JSON file: %s", latest_cache)
                cached_doc = json.loads(latest_cache.read_text(encoding="utf-8"))
                return {
                    "call_id": cached_doc["call_id"],
                    "landing_file_path": str(latest_cache),
                    "ingested_at_utc": cached_doc["ingested_at_utc"],
                    "ingestion_date": cached_doc["ingestion_date"],
                    "payloads": cached_doc["payloads"],
                    "from_offline_cache": True,
                }

        num_locs = len(locations)
        num_days = self.settings.total_requested_days
        num_vars = self.settings.total_requested_variables_count

        # 1. Pre-flight rate-limit check
        planned_weight = self.ledger.verify_pre_flight_budget(num_locs, num_days, num_vars)
        params = self.build_query_params(locations)
        call_id = str(uuid.uuid4())

        logger.info(
            "Dispatching Open-Meteo GET request: call_id=%s locations=%d days=%d vars=%d "
            "planned_weight=%.2f",
            call_id,
            num_locs,
            num_days,
            num_vars,
            planned_weight,
        )

        # 2. Execute outbound HTTP GET
        start_t = time.perf_counter()
        response = self.session.get(
            self.settings.open_meteo_base_url,
            params=params,
            timeout=self.settings.api_timeout_seconds,
        )
        latency_ms = (time.perf_counter() - start_t) * 1000.0
        now_utc = datetime.now(timezone.utc)
        ingestion_date = now_utc.strftime("%Y-%m-%d")

        # 3. If non-200 HTTP response, record 0.0 consumed weight in ledger for auditability, then raise
        if response.status_code != 200:
            self.ledger.record_post_response(
                call_id=call_id,
                pipeline_run_id=pipeline_run_id,
                endpoint_url=self.settings.open_meteo_base_url,
                http_status_code=response.status_code,
                num_locations=num_locs,
                num_days=num_days,
                num_variables=num_vars,
                response_latency_ms=latency_ms,
                landing_file_path=None,
                called_at_utc=now_utc,
            )
            response.raise_for_status()

        raw_json = response.json()
        payloads = self.normalize_response_payload(raw_json, expected_count=num_locs)

        # 4. Write raw envelope to Landing Zone: data/landing/raw_json/ingestion_date=YYYY-MM-DD/
        partition_dir = self.settings.landing_raw_json_dir / f"ingestion_date={ingestion_date}"
        partition_dir.mkdir(parents=True, exist_ok=True)
        landing_file = partition_dir / f"batch_{call_id}.json"

        landing_envelope = {
            "call_id": call_id,
            "pipeline_run_id": pipeline_run_id,
            "ingested_at_utc": now_utc.isoformat(),
            "ingestion_date": ingestion_date,
            "endpoint_url": self.settings.open_meteo_base_url,
            "locations_metadata": [loc.model_dump() for loc in locations],
            "payloads": payloads,
        }
        landing_file.write_text(json.dumps(landing_envelope, indent=2), encoding="utf-8")
        rel_landing_path = str(landing_file)

        # 5. Strictly post-consumption: record HTTP 200 consumed call weight in ledger
        self.ledger.record_post_response(
            call_id=call_id,
            pipeline_run_id=pipeline_run_id,
            endpoint_url=self.settings.open_meteo_base_url,
            http_status_code=response.status_code,
            num_locations=num_locs,
            num_days=num_days,
            num_variables=num_vars,
            response_latency_ms=latency_ms,
            landing_file_path=rel_landing_path,
            called_at_utc=now_utc,
        )

        return {
            "call_id": call_id,
            "landing_file_path": rel_landing_path,
            "ingested_at_utc": now_utc.isoformat(),
            "ingestion_date": ingestion_date,
            "payloads": payloads,
            "from_offline_cache": False,
        }
