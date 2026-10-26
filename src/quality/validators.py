"""Two-stage Data Quality (DQ) validator implementing all 13 rules from data_quality_checks.md."""

from __future__ import annotations

import hashlib
import inspect
import json
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import pandas as pd
import pyarrow as pa
from deltalake import DeltaTable, write_deltalake

from src.config.settings import PipelineSettings, get_settings
from src.utils.logger import get_logger
from src.utils.wmo_codes import VALID_WMO_CODES

logger = get_logger(__name__)

_SUPPORTS_ENGINE_ARG = "engine" in inspect.signature(write_deltalake).parameters

QUARANTINE_ARROW_SCHEMA = pa.schema(
    [
        pa.field("quarantine_id", pa.string(), nullable=False),
        pa.field("pipeline_run_id", pa.string(), nullable=False),
        pa.field("source_bronze_table", pa.string(), nullable=False),
        pa.field("source_ingestion_id", pa.string(), nullable=False),
        pa.field("target_silver_table", pa.string(), nullable=False),
        pa.field("location_id", pa.string(), nullable=True),
        pa.field("observation_timestamp_raw", pa.string(), nullable=True),
        pa.field("failed_record_json", pa.string(), nullable=False),
        pa.field("dq_rule_violated", pa.string(), nullable=False),
        pa.field("dq_failure_details", pa.string(), nullable=False),
        pa.field("quarantined_at_utc", pa.timestamp("us", tz="UTC"), nullable=False),
    ]
)


class DataQualityValidator:
    """Enforces Stage 1 (Structural/Array Parity) and Stage 2 (Row-Level Physical/Domain) DQ rules,
    routing any failed records to silver.quarantine_weather_records.
    """

    def __init__(self, spark: Optional[Any] = None, settings: Optional[PipelineSettings] = None) -> None:
        self.spark = spark
        self.settings = settings or get_settings()
        self.quarantine_path = self.settings.silver_path / "quarantine_weather_records"

    @staticmethod
    def _build_quarantine_id(
        pipeline_run_id: str,
        source_ingestion_id: str,
        raw_time_str: Optional[str],
        rules: List[str],
    ) -> str:
        """Compute deterministic SHA-256 quarantine_id so checkpoint repair runs are idempotent."""
        raw_key = (
            f"{pipeline_run_id}||{source_ingestion_id}||"
            f"{raw_time_str if raw_time_str is not None else 'STAGE1'}||{','.join(rules)}"
        )
        return hashlib.sha256(raw_key.encode("utf-8")).hexdigest()

    def append_to_quarantine(self, quarantine_rows: List[Dict[str, Any]]) -> int:
        """Append formatted failure records to silver.quarantine_weather_records Delta table (idempotently)."""
        if not quarantine_rows:
            return 0

        # Deduplicate within the incoming batch by deterministic quarantine_id
        deduped_by_id: Dict[str, Dict[str, Any]] = {}
        for row in quarantine_rows:
            deduped_by_id[str(row["quarantine_id"])] = row

        has_existing_table = (self.quarantine_path / "_delta_log").exists()
        if has_existing_table:
            existing_ids = set(
                DeltaTable(str(self.quarantine_path))
                .to_pyarrow_table(columns=["quarantine_id"])
                .column("quarantine_id")
                .to_pylist()
            )
            new_rows = [
                row for qid, row in deduped_by_id.items() if qid not in existing_ids
            ]
        else:
            new_rows = list(deduped_by_id.values())

        if not new_rows:
            logger.info(
                "All %d quarantined record(s) already present in silver.quarantine_weather_records; skipping duplicate append",
                len(quarantine_rows),
            )
            return 0

        df = pd.DataFrame(new_rows)
        df["quarantined_at_utc"] = pd.to_datetime(df["quarantined_at_utc"], utc=True)

        self.quarantine_path.mkdir(parents=True, exist_ok=True)
        table = pa.Table.from_pandas(df, schema=QUARANTINE_ARROW_SCHEMA, preserve_index=False)

        write_kwargs: Dict[str, Any] = {
            "table_or_uri": str(self.quarantine_path),
            "data": table,
            "mode": "append",
        }
        if has_existing_table:
            write_kwargs["schema_mode"] = "merge"
            if _SUPPORTS_ENGINE_ARG:
                write_kwargs["engine"] = "rust"

        write_deltalake(**write_kwargs)
        logger.warning(
            "Routed %d failed record(s) to silver.quarantine_weather_records",
            len(new_rows),
        )
        return len(new_rows)

    def validate_stage1_structural_row(
        self,
        bronze_row: Dict[str, Any],
        payload_dict: Dict[str, Any],
        metric_array_cols: List[str],
        source_bronze_table: str,
        target_silver_table: str,
        payload_col_name: str,
    ) -> Optional[Dict[str, Any]]:
        """Evaluate Stage 1 Structural & Array Parity rules on a Bronze row BEFORE unnesting:
        - DQ-STRUCT-001: NON_EMPTY_TIME_ARRAY
        - DQ-STRUCT-002: ARRAY_LENGTH_PARITY
        - DQ-STRUCT-003: VALID_COORDINATE_AND_OFFSET
        Returns a quarantine record dict if failed, or None if valid.
        """
        rules: List[str] = []
        details: List[str] = []

        time_arr = payload_dict.get("time")
        if not isinstance(time_arr, list) or len(time_arr) == 0:
            rules.append("DQ-STRUCT-001")
            details.append("Time array is null or empty in payload")
        else:
            expected_len = len(time_arr)
            parity_ok = True
            for m in metric_array_cols:
                m_arr = payload_dict.get(m)
                if not isinstance(m_arr, list) or len(m_arr) != expected_len:
                    parity_ok = False
                    break
            if not parity_ok:
                rules.append("DQ-STRUCT-002")
                details.append("Parallel metric array lengths do not match time array length")

        lat = bronze_row.get("model_grid_latitude")
        lon = bronze_row.get("model_grid_longitude")
        offset = bronze_row.get("utc_offset_seconds")
        coords_ok = (
            lat is not None
            and not pd.isna(lat)
            and -90.0 <= float(lat) <= 90.0
            and lon is not None
            and not pd.isna(lon)
            and -180.0 <= float(lon) <= 180.0
            and offset is not None
            and not pd.isna(offset)
            and -43200 <= int(offset) <= 50400
        )
        if not coords_ok:
            rules.append("DQ-STRUCT-003")
            details.append("Invalid model_grid coordinates or utc_offset_seconds")

        if not rules:
            return None

        run_id_str = str(bronze_row["pipeline_run_id"])
        ing_id_str = str(bronze_row["ingestion_id"])
        return {
            "quarantine_id": self._build_quarantine_id(run_id_str, ing_id_str, None, rules),
            "pipeline_run_id": run_id_str,
            "source_bronze_table": source_bronze_table,
            "source_ingestion_id": ing_id_str,
            "target_silver_table": target_silver_table,
            "location_id": str(bronze_row.get("location_id", "")),
            "observation_timestamp_raw": None,
            "failed_record_json": str(bronze_row.get(payload_col_name, "{}")),
            "dq_rule_violated": ",".join(rules),
            "dq_failure_details": "; ".join(details),
            "quarantined_at_utc": datetime.now(timezone.utc).isoformat(),
        }

    def validate_stage2_unnested_row(
        self,
        row: Dict[str, Any],
        grain: str,
        pipeline_run_id: str,
        source_bronze_table: str,
        target_silver_table: str,
        raw_time_str: Optional[str],
    ) -> Optional[Dict[str, Any]]:
        """Evaluate Stage 2 Row-Level rules on an unnested time-series row:
        - DQ-KEY-001: NOT_NULL_PRIMARY_GRAIN
        - DQ-PHY-001: TEMP_PHYSICAL_BOUNDS
        - DQ-PHY-002: HUMIDITY_PERCENT_BOUNDS
        - DQ-PHY-003: NON_NEGATIVE_PRECIPITATION
        - DQ-PHY-004: PRECIP_PROBABILITY_BOUNDS
        - DQ-PHY-005: WIND_SPEED_BOUNDS
        - DQ-PHY-006: SURFACE_PRESSURE_BOUNDS
        - DQ-PHY-007: UV_INDEX_BOUNDS
        - DQ-LOGIC-001: DAILY_MIN_LEQ_MAX_TEMP
        - DQ-DOM-001: VALID_WMO_WEATHER_CODE (includes 97!)
        Returns a quarantine record dict if failed, or None if valid.
        """
        cfg = self.settings
        rules: List[str] = []
        details: List[str] = []

        # DQ-KEY-001
        loc_id = row.get("location_id")
        local_date = row.get("observation_local_date")
        time_utc = row.get("observation_time_utc")
        if grain in ("minutely_15", "hourly"):
            key_ok = bool(loc_id and str(loc_id).strip() and local_date and time_utc)
        else:
            key_ok = bool(loc_id and str(loc_id).strip() and local_date)
        if not key_ok:
            rules.append("DQ-KEY-001")
            details.append("Primary key (location_id / observation timestamp or date) is null or empty")

        # DQ-PHY-001: Temperature bounds
        if grain in ("minutely_15", "hourly"):
            t = row.get("temperature_2m_c")
            if t is None or not (cfg.dq_min_valid_temp_c <= float(t) <= cfg.dq_max_valid_temp_c):
                rules.append("DQ-PHY-001")
                details.append(
                    f"temperature_2m_c ({t}) outside [{cfg.dq_min_valid_temp_c}, {cfg.dq_max_valid_temp_c}]"
                )
        else:
            tmax = row.get("temp_max_c")
            tmin = row.get("temp_min_c")
            if (
                tmax is None
                or tmin is None
                or not (cfg.dq_min_valid_temp_c <= float(tmax) <= cfg.dq_max_valid_temp_c)
                or not (cfg.dq_min_valid_temp_c <= float(tmin) <= cfg.dq_max_valid_temp_c)
            ):
                rules.append("DQ-PHY-001")
                details.append(
                    f"temp_min_c/temp_max_c ({tmin}/{tmax}) outside [{cfg.dq_min_valid_temp_c}, {cfg.dq_max_valid_temp_c}]"
                )

        # DQ-PHY-002: Relative humidity [0, 100] (hourly)
        if grain == "hourly":
            hum = row.get("relative_humidity_pct")
            if hum is not None and not (0.0 <= float(hum) <= 100.0):
                rules.append("DQ-PHY-002")
                details.append(f"relative_humidity_pct ({hum}) outside [0.0, 100.0]")

        # DQ-PHY-003: Non-negative precipitation
        p_col = "precipitation_sum_mm" if grain == "daily" else "precipitation_mm"
        p_val = row.get(p_col)
        if p_val is not None and not (0.0 <= float(p_val) <= cfg.dq_max_valid_precip_mm):
            rules.append("DQ-PHY-003")
            details.append(f"{p_col} ({p_val}) outside [0.0, {cfg.dq_max_valid_precip_mm}]")

        # DQ-PHY-004: Precipitation probability [0, 100] (hourly)
        if grain == "hourly":
            prob = row.get("precipitation_prob_pct")
            if prob is not None and not (0.0 <= float(prob) <= 100.0):
                rules.append("DQ-PHY-004")
                details.append(f"precipitation_prob_pct ({prob}) outside [0.0, 100.0]")

        # DQ-PHY-005: Wind speed bounds
        w_col = "wind_speed_max_kmh" if grain == "daily" else "wind_speed_10m_kmh"
        w_val = row.get(w_col)
        if w_val is not None and not (0.0 <= float(w_val) <= cfg.dq_max_valid_wind_kmh):
            rules.append("DQ-PHY-005")
            details.append(f"{w_col} ({w_val}) outside [0.0, {cfg.dq_max_valid_wind_kmh}]")

        # DQ-PHY-006: Surface pressure bounds (hourly)
        if grain == "hourly":
            pres = row.get("surface_pressure_hpa")
            if pres is not None and not (
                cfg.dq_min_surface_pressure_hpa <= float(pres) <= cfg.dq_max_surface_pressure_hpa
            ):
                rules.append("DQ-PHY-006")
                details.append(
                    f"surface_pressure_hpa ({pres}) outside [{cfg.dq_min_surface_pressure_hpa}, {cfg.dq_max_surface_pressure_hpa}]"
                )

        # DQ-PHY-007: UV index bounds (hourly & daily)
        if grain in ("hourly", "daily"):
            uv_col = "uv_index_max" if grain == "daily" else "uv_index"
            uv_val = row.get(uv_col)
            if uv_val is not None and not (0.0 <= float(uv_val) <= cfg.dq_max_valid_uv_index):
                rules.append("DQ-PHY-007")
                details.append(f"{uv_col} ({uv_val}) outside [0.0, {cfg.dq_max_valid_uv_index}]")

        # DQ-LOGIC-001: Daily temp_min_c <= temp_max_c
        if grain == "daily":
            tmax = row.get("temp_max_c")
            tmin = row.get("temp_min_c")
            if tmax is not None and tmin is not None and float(tmin) > float(tmax):
                rules.append("DQ-LOGIC-001")
                details.append(f"temp_min_c ({tmin}) > temp_max_c ({tmax})")

        # DQ-DOM-001: Valid WMO weather code (including 97!)
        wmo = row.get("weather_code")
        if wmo is None or int(wmo) not in VALID_WMO_CODES:
            rules.append("DQ-DOM-001")
            details.append(f"weather_code ({wmo}) is not a valid WMO 4677 code")

        if not rules:
            return None

        serializable_row = {
            k: (v.isoformat() if hasattr(v, "isoformat") else v) for k, v in row.items()
        }
        run_id_str = str(pipeline_run_id)
        ing_id_str = str(row.get("source_ingestion_id", ""))
        return {
            "quarantine_id": self._build_quarantine_id(
                run_id_str, ing_id_str, raw_time_str, rules
            ),
            "pipeline_run_id": run_id_str,
            "source_bronze_table": source_bronze_table,
            "source_ingestion_id": ing_id_str,
            "target_silver_table": target_silver_table,
            "location_id": str(row.get("location_id", "")),
            "observation_timestamp_raw": str(raw_time_str) if raw_time_str is not None else None,
            "failed_record_json": json.dumps(serializable_row, default=str),
            "dq_rule_violated": ",".join(rules),
            "dq_failure_details": "; ".join(details),
            "quarantined_at_utc": datetime.now(timezone.utc).isoformat(),
        }
