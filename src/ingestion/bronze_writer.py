"""Bronze Writer: splits Landing Zone JSON into 3 granular Bronze Delta tables."""

from __future__ import annotations

import hashlib
import inspect
import json
import uuid
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd
import pyarrow as pa
from deltalake import write_deltalake

from src.config.settings import PipelineSettings, get_settings
from src.utils.logger import get_logger

logger = get_logger(__name__)

_SUPPORTS_ENGINE_ARG = "engine" in inspect.signature(write_deltalake).parameters


class BronzeWriter:
    """Reads a landed raw JSON batch and writes to the 3 grain-specific Bronze Delta tables:
    - bronze.weather_minutely_15
    - bronze.weather_hourly
    - bronze.weather_daily
    """

    def __init__(self, spark: Optional[Any] = None, settings: Optional[PipelineSettings] = None) -> None:
        self.spark = spark
        self.settings = settings or get_settings()

    @staticmethod
    def _load_landing_envelope(landing_file_path: str | Path) -> Dict[str, Any]:
        path = Path(landing_file_path)
        if not path.exists():
            raise FileNotFoundError(f"Landing Zone JSON file not found: {path}")
        return json.loads(path.read_text(encoding="utf-8"))

    @staticmethod
    def _sha256_str(content: str) -> str:
        return hashlib.sha256(content.encode("utf-8")).hexdigest()

    def _build_bronze_rows(
        self,
        landing_file_path: str | Path,
        grain_key: str,
        units_key: str,
        id_prefix: str,
        pipeline_run_id_override: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        envelope = self._load_landing_envelope(landing_file_path)
        call_id = envelope["call_id"]
        run_id = pipeline_run_id_override or envelope["pipeline_run_id"]
        ingested_at_utc = envelope["ingested_at_utc"]
        ingestion_date = envelope["ingestion_date"]
        locations_meta = envelope["locations_metadata"]
        payloads = envelope["payloads"]

        rows: List[Dict[str, Any]] = []
        for loc_meta, api_obj in zip(locations_meta, payloads):
            grain_data = api_obj.get(grain_key, {})
            units_data = api_obj.get(units_key, {})

            payload_json_str = json.dumps(grain_data, sort_keys=True)
            units_json_str = json.dumps(units_data, sort_keys=True)

            rows.append(
                {
                    "ingestion_id": f"{id_prefix}-{uuid.uuid4()}",
                    "call_id": call_id,
                    "pipeline_run_id": run_id,
                    "location_id": str(loc_meta["location_id"]),
                    "location_name": str(loc_meta["location_name"]),
                    "state_code": str(loc_meta["state_code"]),
                    "climate_risk_tier": str(loc_meta.get("climate_risk_tier", "STANDARD")),
                    "timezone_name": str(loc_meta["timezone_name"]),
                    "utc_offset_seconds": (
                        int(api_obj["utc_offset_seconds"])
                        if api_obj.get("utc_offset_seconds") is not None
                        else 0
                    ),
                    "requested_latitude": float(loc_meta["latitude"]),
                    "requested_longitude": float(loc_meta["longitude"]),
                    "model_grid_latitude": float(api_obj.get("latitude", 0.0)),
                    "model_grid_longitude": float(api_obj.get("longitude", 0.0)),
                    "elevation_m": (
                        float(api_obj["elevation"])
                        if api_obj.get("elevation") is not None
                        else None
                    ),
                    f"{grain_key}_units_json": units_json_str,
                    f"{grain_key}_payload_json": payload_json_str,
                    "landing_file_path": str(landing_file_path),
                    "payload_sha256": self._sha256_str(payload_json_str),
                    "ingested_at_utc": ingested_at_utc,
                    "ingestion_date": ingestion_date,
                }
            )
        return rows

    def _write_bronze_table(
        self,
        rows: List[Dict[str, Any]],
        grain_key: str,
        table_folder: str,
    ) -> int:
        schema = pa.schema(
            [
                pa.field("ingestion_id", pa.string(), nullable=False),
                pa.field("call_id", pa.string(), nullable=False),
                pa.field("pipeline_run_id", pa.string(), nullable=False),
                pa.field("location_id", pa.string(), nullable=False),
                pa.field("location_name", pa.string(), nullable=False),
                pa.field("state_code", pa.string(), nullable=False),
                pa.field("climate_risk_tier", pa.string(), nullable=False),
                pa.field("timezone_name", pa.string(), nullable=False),
                pa.field("utc_offset_seconds", pa.int32(), nullable=False),
                pa.field("requested_latitude", pa.float64(), nullable=False),
                pa.field("requested_longitude", pa.float64(), nullable=False),
                pa.field("model_grid_latitude", pa.float64(), nullable=False),
                pa.field("model_grid_longitude", pa.float64(), nullable=False),
                pa.field("elevation_m", pa.float64(), nullable=True),
                pa.field(f"{grain_key}_units_json", pa.string(), nullable=False),
                pa.field(f"{grain_key}_payload_json", pa.string(), nullable=False),
                pa.field("landing_file_path", pa.string(), nullable=False),
                pa.field("payload_sha256", pa.string(), nullable=False),
                pa.field("ingested_at_utc", pa.timestamp("us", tz="UTC"), nullable=False),
                pa.field("ingestion_date", pa.date32(), nullable=False),
            ]
        )

        df = pd.DataFrame(rows)
        df["ingested_at_utc"] = pd.to_datetime(df["ingested_at_utc"], utc=True)
        df["ingestion_date"] = pd.to_datetime(df["ingestion_date"]).dt.date

        target_path = self.settings.bronze_path / table_folder
        target_path.mkdir(parents=True, exist_ok=True)
        table = pa.Table.from_pandas(df, schema=schema, preserve_index=False)

        write_kwargs: Dict[str, Any] = {
            "table_or_uri": str(target_path),
            "data": table,
            "mode": "append",
            "partition_by": ["ingestion_date"],
        }
        if (target_path / "_delta_log").exists():
            write_kwargs["schema_mode"] = "merge"
            if _SUPPORTS_ENGINE_ARG:
                write_kwargs["engine"] = "rust"

        write_deltalake(**write_kwargs)
        logger.info("Appended %d rows to bronze.%s at %s", len(rows), table_folder, target_path)
        return len(rows)

    def write_minutely_15(
        self,
        landing_file_path: str | Path,
        pipeline_run_id: Optional[str] = None,
    ) -> int:
        """Populate bronze.weather_minutely_15 from Landing Zone JSON."""
        rows = self._build_bronze_rows(
            landing_file_path=landing_file_path,
            grain_key="minutely_15",
            units_key="minutely_15_units",
            id_prefix="m15",
            pipeline_run_id_override=pipeline_run_id,
        )
        return self._write_bronze_table(rows, "minutely_15", "weather_minutely_15")

    def write_hourly(
        self,
        landing_file_path: str | Path,
        pipeline_run_id: Optional[str] = None,
    ) -> int:
        """Populate bronze.weather_hourly from Landing Zone JSON."""
        rows = self._build_bronze_rows(
            landing_file_path=landing_file_path,
            grain_key="hourly",
            units_key="hourly_units",
            id_prefix="hr",
            pipeline_run_id_override=pipeline_run_id,
        )
        return self._write_bronze_table(rows, "hourly", "weather_hourly")

    def write_daily(
        self,
        landing_file_path: str | Path,
        pipeline_run_id: Optional[str] = None,
    ) -> int:
        """Populate bronze.weather_daily from Landing Zone JSON."""
        rows = self._build_bronze_rows(
            landing_file_path=landing_file_path,
            grain_key="daily",
            units_key="daily_units",
            id_prefix="dy",
            pipeline_run_id_override=pipeline_run_id,
        )
        return self._write_bronze_table(rows, "daily", "weather_daily")
