"""Silver Layer Transformer: SCD Type 2 Dimension, 13-Rule DQ Gate, and 3-Guard Idempotent MERGE."""

from __future__ import annotations

import hashlib
import json
import math
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd
import pyarrow as pa
from deltalake import DeltaTable, write_deltalake

from src.config.settings import PipelineSettings, get_settings
from src.quality.validators import DataQualityValidator
from src.utils.logger import get_logger
from src.utils.wmo_codes import get_wmo_description

logger = get_logger(__name__)

DIM_LOCATION_SCD2_SCHEMA = pa.schema(
    [
        pa.field("location_sk", pa.string(), nullable=False),
        pa.field("location_id", pa.string(), nullable=False),
        pa.field("location_name", pa.string(), nullable=False),
        pa.field("state_code", pa.string(), nullable=False),
        pa.field("requested_latitude", pa.float64(), nullable=False),
        pa.field("requested_longitude", pa.float64(), nullable=False),
        pa.field("model_grid_latitude", pa.float64(), nullable=False),
        pa.field("model_grid_longitude", pa.float64(), nullable=False),
        pa.field("grid_snap_distance_km", pa.float64(), nullable=False),
        pa.field("elevation_m", pa.float64(), nullable=True),
        pa.field("timezone_name", pa.string(), nullable=False),
        pa.field("utc_offset_seconds", pa.int32(), nullable=False),
        pa.field("climate_risk_tier", pa.string(), nullable=False),
        pa.field("attribute_hash", pa.string(), nullable=False),
        pa.field("effective_from_utc", pa.timestamp("us", tz="UTC"), nullable=False),
        pa.field("effective_to_utc", pa.timestamp("us", tz="UTC"), nullable=True),
        pa.field("is_current", pa.bool_(), nullable=False),
    ]
)

FACT_MINUTELY_15_SCHEMA = pa.schema(
    [
        pa.field("location_id", pa.string(), nullable=False),
        pa.field("observation_local_time", pa.string(), nullable=False),
        pa.field("observation_local_date", pa.date32(), nullable=False),
        pa.field("observation_time_utc", pa.timestamp("us", tz="UTC"), nullable=False),
        pa.field("temperature_2m_c", pa.float64(), nullable=False),
        pa.field("precipitation_mm", pa.float64(), nullable=True),
        pa.field("wind_speed_10m_kmh", pa.float64(), nullable=True),
        pa.field("weather_code", pa.int32(), nullable=True),
        pa.field("weather_description", pa.string(), nullable=False),
        pa.field("row_value_hash", pa.string(), nullable=False),
        pa.field("source_ingestion_id", pa.string(), nullable=False),
        pa.field("api_ingested_at_utc", pa.timestamp("us", tz="UTC"), nullable=False),
        pa.field("updated_at_utc", pa.timestamp("us", tz="UTC"), nullable=False),
    ]
)

FACT_HOURLY_SCHEMA = pa.schema(
    [
        pa.field("location_id", pa.string(), nullable=False),
        pa.field("observation_local_time", pa.string(), nullable=False),
        pa.field("observation_local_date", pa.date32(), nullable=False),
        pa.field("observation_time_utc", pa.timestamp("us", tz="UTC"), nullable=False),
        pa.field("temperature_2m_c", pa.float64(), nullable=False),
        pa.field("apparent_temperature_c", pa.float64(), nullable=True),
        pa.field("relative_humidity_pct", pa.float64(), nullable=True),
        pa.field("precipitation_mm", pa.float64(), nullable=True),
        pa.field("precipitation_prob_pct", pa.float64(), nullable=True),
        pa.field("wind_speed_10m_kmh", pa.float64(), nullable=True),
        pa.field("surface_pressure_hpa", pa.float64(), nullable=True),
        pa.field("uv_index", pa.float64(), nullable=True),
        pa.field("weather_code", pa.int32(), nullable=True),
        pa.field("weather_description", pa.string(), nullable=False),
        pa.field("row_value_hash", pa.string(), nullable=False),
        pa.field("source_ingestion_id", pa.string(), nullable=False),
        pa.field("api_ingested_at_utc", pa.timestamp("us", tz="UTC"), nullable=False),
        pa.field("updated_at_utc", pa.timestamp("us", tz="UTC"), nullable=False),
    ]
)

FACT_DAILY_SCHEMA = pa.schema(
    [
        pa.field("location_id", pa.string(), nullable=False),
        pa.field("observation_local_date", pa.date32(), nullable=False),
        pa.field("temp_max_c", pa.float64(), nullable=False),
        pa.field("temp_min_c", pa.float64(), nullable=False),
        pa.field("diurnal_temp_range_c", pa.float64(), nullable=False),
        pa.field("apparent_temp_max_c", pa.float64(), nullable=True),
        pa.field("apparent_temp_min_c", pa.float64(), nullable=True),
        pa.field("precipitation_sum_mm", pa.float64(), nullable=True),
        pa.field("wind_speed_max_kmh", pa.float64(), nullable=True),
        pa.field("uv_index_max", pa.float64(), nullable=True),
        pa.field("weather_code", pa.int32(), nullable=True),
        pa.field("weather_description", pa.string(), nullable=False),
        pa.field("row_value_hash", pa.string(), nullable=False),
        pa.field("source_ingestion_id", pa.string(), nullable=False),
        pa.field("api_ingested_at_utc", pa.timestamp("us", tz="UTC"), nullable=False),
        pa.field("updated_at_utc", pa.timestamp("us", tz="UTC"), nullable=False),
    ]
)


class SilverTransformer:
    """Transforms Bronze tables into conformed Silver Dimension and Fact Delta tables."""

    def __init__(
        self,
        spark: Optional[Any] = None,
        settings: Optional[PipelineSettings] = None,
        dq_validator: Optional[DataQualityValidator] = None,
    ) -> None:
        self.spark = spark
        self.settings = settings or get_settings()
        self.dq = dq_validator or DataQualityValidator(spark, self.settings)

    @staticmethod
    def _sha256_join(values: List[Any]) -> str:
        joined = "||".join("NULL" if v is None else str(v) for v in values)
        return hashlib.sha256(joined.encode("utf-8")).hexdigest()

    @staticmethod
    def _haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
        r_earth_km = 6371.0
        dlat = math.radians(lat2 - lat1)
        dlon = math.radians(lon2 - lon1)
        a = math.sin(dlat / 2.0) ** 2 + (
            math.cos(math.radians(lat1))
            * math.cos(math.radians(lat2))
            * (math.sin(dlon / 2.0) ** 2)
        )
        c = 2.0 * math.atan2(math.sqrt(a), math.sqrt(1.0 - a))
        return round(r_earth_km * c, 2)

    @staticmethod
    def _parse_local_iso_and_utc(
        local_time_str: Optional[str], utc_offset_seconds: int
    ) -> tuple[Optional[date], Optional[datetime]]:
        """Parse 'YYYY-MM-DDTHH:MM' in facility local time and compute:
        1. observation_local_date (date in facility local timezone)
        2. observation_time_utc   (true UTC instant = local_time - utc_offset_seconds)
        """
        if not local_time_str or not isinstance(local_time_str, str):
            return None, None
        try:
            naive_dt = datetime.strptime(local_time_str, "%Y-%m-%dT%H:%M")
            local_d = naive_dt.date()
            utc_dt = (naive_dt - timedelta(seconds=int(utc_offset_seconds))).replace(
                tzinfo=timezone.utc
            )
            return local_d, utc_dt
        except ValueError:
            return None, None

    @staticmethod
    def _parse_local_date(date_str: Optional[str]) -> Optional[date]:
        if not date_str or not isinstance(date_str, str):
            return None
        try:
            return datetime.strptime(date_str, "%Y-%m-%d").date()
        except ValueError:
            return None

    def _read_incremental_bronze(
        self,
        bronze_subfolder: str,
        pipeline_run_id: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """Guard 1: Read only the target pipeline_run_id batch from Bronze so failed rows are not re-quarantined."""
        path = self.settings.bronze_path / bronze_subfolder
        if not (path / "_delta_log").exists():
            return []
        df = DeltaTable(str(path)).to_pandas()
        if pipeline_run_id:
            df = df[df["pipeline_run_id"] == pipeline_run_id]
        return df.to_dict(orient="records")

    # ==========================================================================
    # 1. SCD Type 2 Dimension: silver.dim_location_scd2
    # ==========================================================================
    def transform_dim_location_scd2(
        self,
        pipeline_run_id: Optional[str] = None,
    ) -> Dict[str, int]:
        """Build and maintain silver.dim_location_scd2 using Slowly Changing Dimension Type 2 logic."""
        bronze_rows = self._read_incremental_bronze("weather_hourly", pipeline_run_id)
        if not bronze_rows:
            return {"rows_processed": 0, "rows_quarantined": 0}

        # Deduplicate incoming location metadata to latest per location_id
        latest_by_loc: Dict[str, Dict[str, Any]] = {}
        for r in sorted(bronze_rows, key=lambda x: pd.Timestamp(x["ingested_at_utc"])):
            latest_by_loc[str(r["location_id"])] = r

        far_future_utc = datetime(9999, 12, 31, 23, 59, 59, tzinfo=timezone.utc)
        staged_rows: List[Dict[str, Any]] = []
        for loc_id, r in latest_by_loc.items():
            eff_from = pd.Timestamp(r["ingested_at_utc"]).to_pydatetime()
            if eff_from.tzinfo is None:
                eff_from = eff_from.replace(tzinfo=timezone.utc)

            snap_km = self._haversine_km(
                float(r["requested_latitude"]),
                float(r["requested_longitude"]),
                float(r["model_grid_latitude"]),
                float(r["model_grid_longitude"]),
            )
            elev = float(r["elevation_m"]) if pd.notna(r.get("elevation_m")) else None
            attr_hash = self._sha256_join(
                [
                    r["location_name"],
                    r["state_code"],
                    r["model_grid_latitude"],
                    r["model_grid_longitude"],
                    elev,
                    r["climate_risk_tier"],
                    r["timezone_name"],
                ]
            )
            loc_sk = self._sha256_join([loc_id, eff_from.isoformat()])

            staged_rows.append(
                {
                    "location_sk": loc_sk,
                    "location_id": loc_id,
                    "location_name": str(r["location_name"]),
                    "state_code": str(r["state_code"]),
                    "requested_latitude": float(r["requested_latitude"]),
                    "requested_longitude": float(r["requested_longitude"]),
                    "model_grid_latitude": float(r["model_grid_latitude"]),
                    "model_grid_longitude": float(r["model_grid_longitude"]),
                    "grid_snap_distance_km": snap_km,
                    "elevation_m": elev,
                    "timezone_name": str(r["timezone_name"]),
                    "utc_offset_seconds": int(r["utc_offset_seconds"]),
                    "climate_risk_tier": str(r["climate_risk_tier"]),
                    "attribute_hash": attr_hash,
                    "effective_from_utc": eff_from,
                    "effective_to_utc": far_future_utc,
                    "is_current": True,
                }
            )

        target_path = self.settings.silver_path / "dim_location_scd2"
        target_path.mkdir(parents=True, exist_ok=True)

        if not (target_path / "_delta_log").exists():
            df_init = pd.DataFrame(staged_rows)
            tbl = pa.Table.from_pandas(df_init, schema=DIM_LOCATION_SCD2_SCHEMA, preserve_index=False)
            write_deltalake(
                str(target_path),
                tbl,
                mode="overwrite",
                configuration={"delta.enableChangeDataFeed": "true"},
            )
            logger.info("Initialized silver.dim_location_scd2 with %d rows", len(staged_rows))
            return {"rows_processed": len(staged_rows), "rows_quarantined": 0}

        dt = DeltaTable(str(target_path))
        existing_df = dt.to_pandas()
        active_df = existing_df[existing_df["is_current"] == True]  # noqa: E712
        active_hashes = dict(zip(active_df["location_id"], active_df["attribute_hash"]))

        changed_or_new = [
            row
            for row in staged_rows
            if active_hashes.get(row["location_id"]) != row["attribute_hash"]
        ]
        if not changed_or_new:
            logger.info("No SCD2 attribute changes detected for silver.dim_location_scd2")
            return {"rows_processed": len(staged_rows), "rows_quarantined": 0}

        changed_df = pd.DataFrame(changed_or_new)
        changed_tbl = pa.Table.from_pandas(
            changed_df, schema=DIM_LOCATION_SCD2_SCHEMA, preserve_index=False
        )

        # Step A: Expire existing active rows for changed locations via Delta MERGE
        (
            dt.merge(
                source=changed_tbl,
                predicate="tgt.location_id = src.location_id AND tgt.is_current = true",
                source_alias="src",
                target_alias="tgt",
            )
            .when_matched_update(
                predicate="tgt.attribute_hash != src.attribute_hash",
                updates={
                    "is_current": "false",
                    "effective_to_utc": "src.effective_from_utc",
                },
            )
            .execute()
        )

        # Step B: Append the new active versions
        write_deltalake(str(target_path), changed_tbl, mode="append")
        logger.info(
            "Applied SCD2 update to silver.dim_location_scd2 (%d new/updated versions)",
            len(changed_or_new),
        )
        return {"rows_processed": len(staged_rows), "rows_quarantined": 0}

    # ==========================================================================
    # Helper: 3-Guard Idempotent Fact Table MERGE INTO
    # ==========================================================================
    def _upsert_silver_fact_with_3_guards(
        self,
        valid_rows: List[Dict[str, Any]],
        target_subfolder: str,
        key_cols: List[str],
        partition_col: str,
        arrow_schema: pa.Schema,
    ) -> int:
        """Execute 3-guard idempotent Delta MERGE INTO on a Silver fact table:
        Guard 1: Incremental Bronze batch filtering (already applied upstream)
        Guard 2: Source deduplication on key_cols ordered by api_ingested_at_utc DESC
        Guard 3: WHEN MATCHED AND src.api_ingested_at_utc >= tgt.api_ingested_at_utc
                 AND src.row_value_hash != tgt.row_value_hash
        """
        if not valid_rows:
            return 0

        # Guard 2: Deduplicate source on primary grain keeping latest api_ingested_at_utc
        deduped_map: Dict[tuple, Dict[str, Any]] = {}
        for r in sorted(valid_rows, key=lambda x: x["api_ingested_at_utc"]):
            k = tuple(r[col] for col in key_cols)
            deduped_map[k] = r

        deduped_list = list(deduped_map.values())
        row_count = len(deduped_list)

        df = pd.DataFrame(deduped_list)
        tbl = pa.Table.from_pandas(df, schema=arrow_schema, preserve_index=False)

        target_path = self.settings.silver_path / target_subfolder
        target_path.mkdir(parents=True, exist_ok=True)

        if not (target_path / "_delta_log").exists():
            write_deltalake(
                str(target_path),
                tbl,
                mode="overwrite",
                partition_by=[partition_col],
                configuration={"delta.enableChangeDataFeed": "true"},
            )
            logger.info("Initialized silver.%s with %d rows", target_subfolder, row_count)
            return row_count

        dt = DeltaTable(str(target_path))
        merge_cond = " AND ".join([f"tgt.{k} = src.{k}" for k in key_cols])

        # Guard 3: Newer-only & value-changed update condition to protect CDF
        update_cond = (
            "src.api_ingested_at_utc >= tgt.api_ingested_at_utc "
            "AND src.row_value_hash != tgt.row_value_hash"
        )

        (
            dt.merge(
                source=tbl,
                predicate=merge_cond,
                source_alias="src",
                target_alias="tgt",
            )
            .when_matched_update_all(predicate=update_cond)
            .when_not_matched_insert_all()
            .execute()
        )
        logger.info("Merged %d deduplicated rows into silver.%s", row_count, target_subfolder)
        return row_count

    @staticmethod
    def _safe_float(val: Any) -> Optional[float]:
        if val is None or (isinstance(val, float) and math.isnan(val)):
            return None
        return float(val)

    @staticmethod
    def _safe_int(val: Any) -> Optional[int]:
        if val is None or (isinstance(val, float) and math.isnan(val)):
            return None
        return int(val)

    # ==========================================================================
    # 2. Silver Fact Table: silver.fact_weather_minutely_15
    # ==========================================================================
    def transform_fact_minutely_15(
        self,
        pipeline_run_id: Optional[str] = None,
    ) -> Dict[str, int]:
        """Unnest, validate, and merge bronze.weather_minutely_15 -> silver.fact_weather_minutely_15."""
        bronze_rows = self._read_incremental_bronze("weather_minutely_15", pipeline_run_id)
        quarantine_batch: List[Dict[str, Any]] = []
        valid_rows: List[Dict[str, Any]] = []
        now_utc = datetime.now(timezone.utc)

        metric_cols = ["temperature_2m", "precipitation", "wind_speed_10m", "weather_code"]

        for brow in bronze_rows:
            payload = json.loads(brow.get("minutely_15_payload_json") or "{}")
            q_struct = self.dq.validate_stage1_structural_row(
                bronze_row=brow,
                payload_dict=payload,
                metric_array_cols=metric_cols,
                source_bronze_table="bronze.weather_minutely_15",
                target_silver_table="silver.fact_weather_minutely_15",
                payload_col_name="minutely_15_payload_json",
            )
            if q_struct is not None:
                quarantine_batch.append(q_struct)
                continue

            times = payload["time"]
            temps = payload["temperature_2m"]
            precips = payload["precipitation"]
            winds = payload["wind_speed_10m"]
            codes = payload["weather_code"]
            offset_sec = int(brow["utc_offset_seconds"])
            ingested_dt = pd.Timestamp(brow["ingested_at_utc"]).to_pydatetime()
            if ingested_dt.tzinfo is None:
                ingested_dt = ingested_dt.replace(tzinfo=timezone.utc)

            for k in range(len(times)):
                t_str = times[k]
                local_d, utc_dt = self._parse_local_iso_and_utc(t_str, offset_sec)
                t_c = self._safe_float(temps[k])
                p_mm = self._safe_float(precips[k])
                w_kmh = self._safe_float(winds[k])
                w_code = self._safe_int(codes[k])

                candidate = {
                    "location_id": str(brow["location_id"]),
                    "observation_local_time": str(t_str) if t_str is not None else "",
                    "observation_local_date": local_d,
                    "observation_time_utc": utc_dt,
                    "temperature_2m_c": t_c,
                    "precipitation_mm": p_mm,
                    "wind_speed_10m_kmh": w_kmh,
                    "weather_code": w_code,
                    "source_ingestion_id": str(brow["ingestion_id"]),
                    "api_ingested_at_utc": ingested_dt,
                }

                q_row = self.dq.validate_stage2_unnested_row(
                    row=candidate,
                    grain="minutely_15",
                    pipeline_run_id=str(brow["pipeline_run_id"]),
                    source_bronze_table="bronze.weather_minutely_15",
                    target_silver_table="silver.fact_weather_minutely_15",
                    raw_time_str=t_str,
                )
                if q_row is not None:
                    quarantine_batch.append(q_row)
                    continue

                candidate["weather_description"] = get_wmo_description(w_code)
                candidate["row_value_hash"] = self._sha256_join([t_c, p_mm, w_kmh, w_code])
                candidate["updated_at_utc"] = now_utc
                valid_rows.append(candidate)

        q_count = self.dq.append_to_quarantine(quarantine_batch)
        merged_count = self._upsert_silver_fact_with_3_guards(
            valid_rows=valid_rows,
            target_subfolder="fact_weather_minutely_15",
            key_cols=["location_id", "observation_time_utc"],
            partition_col="observation_local_date",
            arrow_schema=FACT_MINUTELY_15_SCHEMA,
        )
        return {"rows_processed": merged_count, "rows_quarantined": q_count}

    # ==========================================================================
    # 3. Silver Fact Table: silver.fact_weather_hourly
    # ==========================================================================
    def transform_fact_hourly(
        self,
        pipeline_run_id: Optional[str] = None,
    ) -> Dict[str, int]:
        """Unnest, validate, and merge bronze.weather_hourly -> silver.fact_weather_hourly."""
        bronze_rows = self._read_incremental_bronze("weather_hourly", pipeline_run_id)
        quarantine_batch: List[Dict[str, Any]] = []
        valid_rows: List[Dict[str, Any]] = []
        now_utc = datetime.now(timezone.utc)

        metric_cols = [
            "temperature_2m",
            "relative_humidity_2m",
            "apparent_temperature",
            "precipitation_probability",
            "precipitation",
            "weather_code",
            "surface_pressure",
            "wind_speed_10m",
            "uv_index",
        ]

        for brow in bronze_rows:
            payload = json.loads(brow.get("hourly_payload_json") or "{}")
            q_struct = self.dq.validate_stage1_structural_row(
                bronze_row=brow,
                payload_dict=payload,
                metric_array_cols=metric_cols,
                source_bronze_table="bronze.weather_hourly",
                target_silver_table="silver.fact_weather_hourly",
                payload_col_name="hourly_payload_json",
            )
            if q_struct is not None:
                quarantine_batch.append(q_struct)
                continue

            times = payload["time"]
            temps = payload["temperature_2m"]
            app_temps = payload["apparent_temperature"]
            hums = payload["relative_humidity_2m"]
            precips = payload["precipitation"]
            probs = payload["precipitation_probability"]
            winds = payload["wind_speed_10m"]
            pressures = payload["surface_pressure"]
            uvs = payload["uv_index"]
            codes = payload["weather_code"]

            offset_sec = int(brow["utc_offset_seconds"])
            ingested_dt = pd.Timestamp(brow["ingested_at_utc"]).to_pydatetime()
            if ingested_dt.tzinfo is None:
                ingested_dt = ingested_dt.replace(tzinfo=timezone.utc)

            for k in range(len(times)):
                t_str = times[k]
                local_d, utc_dt = self._parse_local_iso_and_utc(t_str, offset_sec)
                t_c = self._safe_float(temps[k])
                app_c = self._safe_float(app_temps[k])
                hum_pct = self._safe_float(hums[k])
                p_mm = self._safe_float(precips[k])
                prob_pct = self._safe_float(probs[k])
                w_kmh = self._safe_float(winds[k])
                pres_hpa = self._safe_float(pressures[k])
                uv_val = self._safe_float(uvs[k])
                w_code = self._safe_int(codes[k])

                candidate = {
                    "location_id": str(brow["location_id"]),
                    "observation_local_time": str(t_str) if t_str is not None else "",
                    "observation_local_date": local_d,
                    "observation_time_utc": utc_dt,
                    "temperature_2m_c": t_c,
                    "apparent_temperature_c": app_c,
                    "relative_humidity_pct": hum_pct,
                    "precipitation_mm": p_mm,
                    "precipitation_prob_pct": prob_pct,
                    "wind_speed_10m_kmh": w_kmh,
                    "surface_pressure_hpa": pres_hpa,
                    "uv_index": uv_val,
                    "weather_code": w_code,
                    "source_ingestion_id": str(brow["ingestion_id"]),
                    "api_ingested_at_utc": ingested_dt,
                }

                q_row = self.dq.validate_stage2_unnested_row(
                    row=candidate,
                    grain="hourly",
                    pipeline_run_id=str(brow["pipeline_run_id"]),
                    source_bronze_table="bronze.weather_hourly",
                    target_silver_table="silver.fact_weather_hourly",
                    raw_time_str=t_str,
                )
                if q_row is not None:
                    quarantine_batch.append(q_row)
                    continue

                candidate["weather_description"] = get_wmo_description(w_code)
                candidate["row_value_hash"] = self._sha256_join(
                    [t_c, app_c, hum_pct, p_mm, prob_pct, w_kmh, pres_hpa, uv_val, w_code]
                )
                candidate["updated_at_utc"] = now_utc
                valid_rows.append(candidate)

        q_count = self.dq.append_to_quarantine(quarantine_batch)
        merged_count = self._upsert_silver_fact_with_3_guards(
            valid_rows=valid_rows,
            target_subfolder="fact_weather_hourly",
            key_cols=["location_id", "observation_time_utc"],
            partition_col="observation_local_date",
            arrow_schema=FACT_HOURLY_SCHEMA,
        )
        return {"rows_processed": merged_count, "rows_quarantined": q_count}

    # ==========================================================================
    # 4. Silver Fact Table: silver.fact_weather_daily
    # ==========================================================================
    def transform_fact_daily(
        self,
        pipeline_run_id: Optional[str] = None,
    ) -> Dict[str, int]:
        """Unnest, validate, and merge bronze.weather_daily -> silver.fact_weather_daily."""
        bronze_rows = self._read_incremental_bronze("weather_daily", pipeline_run_id)
        quarantine_batch: List[Dict[str, Any]] = []
        valid_rows: List[Dict[str, Any]] = []
        now_utc = datetime.now(timezone.utc)

        metric_cols = [
            "weather_code",
            "temperature_2m_max",
            "temperature_2m_min",
            "apparent_temperature_max",
            "apparent_temperature_min",
            "precipitation_sum",
            "uv_index_max",
            "wind_speed_10m_max",
        ]

        for brow in bronze_rows:
            payload = json.loads(brow.get("daily_payload_json") or "{}")
            q_struct = self.dq.validate_stage1_structural_row(
                bronze_row=brow,
                payload_dict=payload,
                metric_array_cols=metric_cols,
                source_bronze_table="bronze.weather_daily",
                target_silver_table="silver.fact_weather_daily",
                payload_col_name="daily_payload_json",
            )
            if q_struct is not None:
                quarantine_batch.append(q_struct)
                continue

            times = payload["time"]
            tmaxs = payload["temperature_2m_max"]
            tmins = payload["temperature_2m_min"]
            app_maxs = payload["apparent_temperature_max"]
            app_mins = payload["apparent_temperature_min"]
            precips = payload["precipitation_sum"]
            winds = payload["wind_speed_10m_max"]
            uvs = payload["uv_index_max"]
            codes = payload["weather_code"]

            ingested_dt = pd.Timestamp(brow["ingested_at_utc"]).to_pydatetime()
            if ingested_dt.tzinfo is None:
                ingested_dt = ingested_dt.replace(tzinfo=timezone.utc)

            for k in range(len(times)):
                d_str = times[k]
                local_d = self._parse_local_date(d_str)
                tmax = self._safe_float(tmaxs[k])
                tmin = self._safe_float(tmins[k])
                app_max = self._safe_float(app_maxs[k])
                app_min = self._safe_float(app_mins[k])
                p_sum = self._safe_float(precips[k])
                w_max = self._safe_float(winds[k])
                uv_max = self._safe_float(uvs[k])
                w_code = self._safe_int(codes[k])

                candidate = {
                    "location_id": str(brow["location_id"]),
                    "observation_local_date": local_d,
                    "temp_max_c": tmax,
                    "temp_min_c": tmin,
                    "apparent_temp_max_c": app_max,
                    "apparent_temp_min_c": app_min,
                    "precipitation_sum_mm": p_sum,
                    "wind_speed_max_kmh": w_max,
                    "uv_index_max": uv_max,
                    "weather_code": w_code,
                    "source_ingestion_id": str(brow["ingestion_id"]),
                    "api_ingested_at_utc": ingested_dt,
                }

                q_row = self.dq.validate_stage2_unnested_row(
                    row=candidate,
                    grain="daily",
                    pipeline_run_id=str(brow["pipeline_run_id"]),
                    source_bronze_table="bronze.weather_daily",
                    target_silver_table="silver.fact_weather_daily",
                    raw_time_str=d_str,
                )
                if q_row is not None:
                    quarantine_batch.append(q_row)
                    continue

                candidate["diurnal_temp_range_c"] = round(float(tmax) - float(tmin), 2)
                candidate["weather_description"] = get_wmo_description(w_code)
                candidate["row_value_hash"] = self._sha256_join(
                    [tmax, tmin, app_max, app_min, p_sum, w_max, uv_max, w_code]
                )
                candidate["updated_at_utc"] = now_utc
                valid_rows.append(candidate)

        q_count = self.dq.append_to_quarantine(quarantine_batch)
        merged_count = self._upsert_silver_fact_with_3_guards(
            valid_rows=valid_rows,
            target_subfolder="fact_weather_daily",
            key_cols=["location_id", "observation_local_date"],
            partition_col="observation_local_date",
            arrow_schema=FACT_DAILY_SCHEMA,
        )
        return {"rows_processed": merged_count, "rows_quarantined": q_count}

    def transform_all_facts(
        self,
        pipeline_run_id: Optional[str] = None,
    ) -> Dict[str, int]:
        """Run all three Silver fact transformations and aggregate metrics."""
        res_m15 = self.transform_fact_minutely_15(pipeline_run_id)
        res_hr = self.transform_fact_hourly(pipeline_run_id)
        res_dy = self.transform_fact_daily(pipeline_run_id)

        return {
            "rows_processed": (
                res_m15["rows_processed"] + res_hr["rows_processed"] + res_dy["rows_processed"]
            ),
            "rows_quarantined": (
                res_m15["rows_quarantined"]
                + res_hr["rows_quarantined"]
                + res_dy["rows_quarantined"]
            ),
        }
