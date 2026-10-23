"""Gold Layer Transformer: Daily Weather Summary & Aged Care Facility Risk Alert Marts."""

from __future__ import annotations

import hashlib
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from zoneinfo import ZoneInfo

import duckdb
import pandas as pd
import pyarrow as pa
from deltalake import DeltaTable, write_deltalake

from src.config.settings import PipelineSettings, get_settings
from src.utils.logger import get_logger
from src.utils.wmo_codes import SEVERE_STORM_WMO_CODES

logger = get_logger(__name__)

MART_DAILY_SUMMARY_SCHEMA = pa.schema(
    [
        pa.field("location_id", pa.string(), nullable=False),
        pa.field("location_sk", pa.string(), nullable=False),
        pa.field("location_name", pa.string(), nullable=False),
        pa.field("state_code", pa.string(), nullable=False),
        pa.field("timezone_name", pa.string(), nullable=False),
        pa.field("observation_local_date", pa.date32(), nullable=False),
        pa.field("is_forecast", pa.bool_(), nullable=False),
        pa.field("data_horizon_type", pa.string(), nullable=False),
        pa.field("temp_max_c", pa.float64(), nullable=False),
        pa.field("temp_min_c", pa.float64(), nullable=False),
        pa.field("diurnal_temp_range_c", pa.float64(), nullable=False),
        pa.field("hourly_avg_temp_c", pa.float64(), nullable=False),
        pa.field("hourly_avg_humidity_pct", pa.float64(), nullable=True),
        pa.field("rolling_7d_avg_temp_c", pa.float64(), nullable=False),
        pa.field("temp_anomaly_vs_7d_c", pa.float64(), nullable=False),
        pa.field("rolling_7d_precip_mm", pa.float64(), nullable=False),
        pa.field("extreme_heat_hours_count", pa.int32(), nullable=False),
        pa.field("extreme_cold_hours_count", pa.int32(), nullable=False),
        pa.field("dominant_weather_condition", pa.string(), nullable=False),
        pa.field("mart_updated_at_utc", pa.timestamp("us", tz="UTC"), nullable=False),
    ]
)

MART_RISK_ALERTS_SCHEMA = pa.schema(
    [
        pa.field("alert_id", pa.string(), nullable=False),
        pa.field("location_id", pa.string(), nullable=False),
        pa.field("location_name", pa.string(), nullable=False),
        pa.field("state_code", pa.string(), nullable=False),
        pa.field("climate_risk_tier", pa.string(), nullable=False),
        pa.field("observation_local_date", pa.date32(), nullable=False),
        pa.field("is_forecast", pa.bool_(), nullable=False),
        pa.field("data_horizon_type", pa.string(), nullable=False),
        pa.field("overall_risk_severity", pa.string(), nullable=False),
        pa.field("is_heatwave_risk", pa.bool_(), nullable=False),
        pa.field("is_cold_snap_risk", pa.bool_(), nullable=False),
        pa.field("is_severe_storm_risk", pa.bool_(), nullable=False),
        pa.field("is_high_uv_risk", pa.bool_(), nullable=False),
        pa.field("primary_hazard_reason", pa.string(), nullable=False),
        pa.field("recommended_care_action", pa.string(), nullable=False),
        pa.field("source_ingestion_id", pa.string(), nullable=False),
        pa.field("evaluated_at_utc", pa.timestamp("us", tz="UTC"), nullable=False),
    ]
)


class GoldTransformer:
    """Builds business-ready Gold analytical and alerting marts from conformed Silver tables."""

    def __init__(
        self, spark: Optional[Any] = None, settings: Optional[PipelineSettings] = None
    ) -> None:
        self.spark = spark
        self.settings = settings or get_settings()

    def _upsert_gold_mart(
        self,
        df: pd.DataFrame,
        mart_subfolder: str,
        key_cols: List[str],
        arrow_schema: pa.Schema,
    ) -> int:
        if df.empty:
            return 0
        count = len(df)
        tbl = pa.Table.from_pandas(df, schema=arrow_schema, preserve_index=False)
        target_path = self.settings.gold_path / mart_subfolder
        target_path.mkdir(parents=True, exist_ok=True)

        if not (target_path / "_delta_log").exists():
            write_deltalake(
                str(target_path),
                tbl,
                mode="overwrite",
                configuration={"delta.enableChangeDataFeed": "true"},
            )
            logger.info("Initialized gold.%s with %d rows", mart_subfolder, count)
            return count

        dt = DeltaTable(str(target_path))
        merge_cond = " AND ".join([f"tgt.{k} = src.{k}" for k in key_cols])
        (
            dt.merge(
                source=tbl,
                predicate=merge_cond,
                source_alias="src",
                target_alias="tgt",
            )
            .when_matched_update_all()
            .when_not_matched_insert_all()
            .execute()
        )
        logger.info("Merged %d rows into gold.%s", count, mart_subfolder)
        return count

    @staticmethod
    def _facility_local_today(tz_name: str, now_utc: datetime) -> datetime.date:
        return now_utc.astimezone(ZoneInfo(tz_name)).date()

    # ==========================================================================
    # 1. Gold Mart: gold.mart_daily_weather_summary
    # ==========================================================================
    def build_mart_daily_weather_summary(self) -> int:
        """Build gold.mart_daily_weather_summary aligned on facility local date (Fix 2.1 & Section 3)."""
        cfg = self.settings
        dim_loc_tbl = DeltaTable(str(cfg.silver_path / "dim_location_scd2")).to_pyarrow_table()
        fact_daily_tbl = DeltaTable(str(cfg.silver_path / "fact_weather_daily")).to_pyarrow_table()
        fact_hourly_tbl = DeltaTable(
            str(cfg.silver_path / "fact_weather_hourly")
        ).to_pyarrow_table()

        con = duckdb.connect()
        con.register("dim_loc", dim_loc_tbl)
        con.register("fact_daily", fact_daily_tbl)
        con.register("fact_hourly", fact_hourly_tbl)

        sql = f"""
        WITH active_loc AS (
            SELECT * FROM dim_loc WHERE is_current = true
        ),
        hourly_agg AS (
            SELECT
                location_id,
                observation_local_date,
                ROUND(AVG(temperature_2m_c), 2) AS hourly_avg_temp_c,
                ROUND(AVG(relative_humidity_pct), 2) AS hourly_avg_humidity_pct,
                CAST(SUM(CASE WHEN temperature_2m_c >= {cfg.alert_heatwave_temp_c} THEN 1 ELSE 0 END) AS INTEGER) AS extreme_heat_hours_count,
                CAST(SUM(CASE WHEN temperature_2m_c <= {cfg.alert_cold_snap_temp_c} THEN 1 ELSE 0 END) AS INTEGER) AS extreme_cold_hours_count
            FROM fact_hourly
            GROUP BY location_id, observation_local_date
        ),
        joined AS (
            SELECT
                d.location_id,
                loc.location_sk,
                loc.location_name,
                loc.state_code,
                loc.timezone_name,
                d.observation_local_date,
                d.temp_max_c,
                d.temp_min_c,
                d.diurnal_temp_range_c,
                COALESCE(h.hourly_avg_temp_c, ROUND((d.temp_max_c + d.temp_min_c) / 2.0, 2)) AS hourly_avg_temp_c,
                h.hourly_avg_humidity_pct,
                COALESCE(d.precipitation_sum_mm, 0.0) AS precip_mm,
                COALESCE(h.extreme_heat_hours_count, 0) AS extreme_heat_hours_count,
                COALESCE(h.extreme_cold_hours_count, 0) AS extreme_cold_hours_count,
                d.weather_description AS dominant_weather_condition
            FROM fact_daily d
            INNER JOIN active_loc loc ON d.location_id = loc.location_id
            LEFT JOIN hourly_agg h
                ON d.location_id = h.location_id
               AND d.observation_local_date = h.observation_local_date
        )
        SELECT
            location_id,
            location_sk,
            location_name,
            state_code,
            timezone_name,
            observation_local_date,
            temp_max_c,
            temp_min_c,
            diurnal_temp_range_c,
            hourly_avg_temp_c,
            hourly_avg_humidity_pct,
            ROUND(
                AVG(hourly_avg_temp_c) OVER (
                    PARTITION BY location_id
                    ORDER BY observation_local_date ASC
                    ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
                ),
                2
            ) AS rolling_7d_avg_temp_c,
            ROUND(
                SUM(precip_mm) OVER (
                    PARTITION BY location_id
                    ORDER BY observation_local_date ASC
                    ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
                ),
                2
            ) AS rolling_7d_precip_mm,
            extreme_heat_hours_count,
            extreme_cold_hours_count,
            dominant_weather_condition
        FROM joined
        ORDER BY location_id, observation_local_date
        """
        df = con.execute(sql).df()
        con.close()

        if df.empty:
            return 0

        now_utc = datetime.now(timezone.utc)
        df["observation_local_date"] = pd.to_datetime(df["observation_local_date"]).dt.date
        df["temp_anomaly_vs_7d_c"] = (
            df["hourly_avg_temp_c"] - df["rolling_7d_avg_temp_c"]
        ).round(2)

        is_forecast_list: List[bool] = []
        horizon_list: List[str] = []
        for _, row in df.iterrows():
            local_today = self._facility_local_today(str(row["timezone_name"]), now_utc)
            obs_date = row["observation_local_date"]
            is_f = obs_date >= local_today
            is_forecast_list.append(is_f)
            if obs_date < local_today:
                horizon_list.append("PAST_MODELLED")
            elif obs_date == local_today:
                horizon_list.append("TODAY_MIXED")
            else:
                horizon_list.append("FORWARD_FORECAST")

        df["is_forecast"] = is_forecast_list
        df["data_horizon_type"] = horizon_list
        df["mart_updated_at_utc"] = now_utc

        ordered_cols = [f.name for f in MART_DAILY_SUMMARY_SCHEMA]
        df = df[ordered_cols]

        return self._upsert_gold_mart(
            df=df,
            mart_subfolder="mart_daily_weather_summary",
            key_cols=["location_id", "observation_local_date"],
            arrow_schema=MART_DAILY_SUMMARY_SCHEMA,
        )

    # ==========================================================================
    # 2. Gold Mart: gold.mart_care_facility_risk_alerts
    # ==========================================================================
    def build_mart_care_facility_risk_alerts(self) -> int:
        """Build gold.mart_care_facility_risk_alerts for Australian Aged Care hubs."""
        cfg = self.settings
        dim_loc_df = DeltaTable(str(cfg.silver_path / "dim_location_scd2")).to_pandas()
        dim_loc_df = dim_loc_df[dim_loc_df["is_current"] == True]  # noqa: E712
        fact_daily_df = DeltaTable(str(cfg.silver_path / "fact_weather_daily")).to_pandas()

        merged = fact_daily_df.merge(
            dim_loc_df[
                [
                    "location_id",
                    "location_name",
                    "state_code",
                    "timezone_name",
                    "climate_risk_tier",
                ]
            ],
            on="location_id",
            how="inner",
        )
        if merged.empty:
            return 0

        now_utc = datetime.now(timezone.utc)
        alert_rows: List[Dict[str, Any]] = []

        for _, r in merged.iterrows():
            loc_id = str(r["location_id"])
            obs_date = pd.to_datetime(r["observation_local_date"]).date()
            tz_name = str(r["timezone_name"])
            local_today = self._facility_local_today(tz_name, now_utc)

            is_forecast = obs_date >= local_today
            if obs_date < local_today:
                horizon = "PAST_MODELLED"
            elif obs_date == local_today:
                horizon = "TODAY_MIXED"
            else:
                horizon = "FORWARD_FORECAST"

            tmax = float(r["temp_max_c"])
            tmin = float(r["temp_min_c"])
            app_max = (
                float(r["apparent_temp_max_c"])
                if pd.notna(r.get("apparent_temp_max_c"))
                else tmax
            )
            precip = (
                float(r["precipitation_sum_mm"])
                if pd.notna(r.get("precipitation_sum_mm"))
                else 0.0
            )
            wind = (
                float(r["wind_speed_max_kmh"])
                if pd.notna(r.get("wind_speed_max_kmh"))
                else 0.0
            )
            uv = float(r["uv_index_max"]) if pd.notna(r.get("uv_index_max")) else 0.0
            w_code = int(r["weather_code"]) if pd.notna(r.get("weather_code")) else 0
            w_desc = str(r["weather_description"])

            is_heat = (tmax >= cfg.alert_heatwave_temp_c) or (
                app_max >= (cfg.alert_heatwave_temp_c + 2.0)
            )
            is_cold = tmin <= cfg.alert_cold_snap_temp_c
            is_storm = (
                (wind >= cfg.alert_high_wind_kmh)
                or (precip >= 25.0)
                or (w_code in SEVERE_STORM_WMO_CODES)
            )
            is_uv = uv >= cfg.alert_high_uv_index

            hazard_count = sum([is_heat, is_cold, is_storm, is_uv])
            is_watch = (
                (tmax >= (cfg.alert_heatwave_temp_c - 3.0))
                or (tmin <= (cfg.alert_cold_snap_temp_c + 2.0))
                or (wind >= (cfg.alert_high_wind_kmh - 15.0))
            )

            if (
                hazard_count >= 2
                or tmax >= (cfg.alert_heatwave_temp_c + 5.0)
                or w_code in {95, 96, 97, 99}
            ):
                severity = "CRITICAL"
            elif hazard_count == 1:
                severity = "WARNING"
            elif is_watch:
                severity = "WATCH"
            else:
                severity = "NORMAL"

            reasons: List[str] = []
            actions: List[str] = []
            if is_heat:
                reasons.append(f"Extreme Heat (Max {tmax}°C / Feels-like {app_max}°C)")
                actions.append(
                    "Initiate Heatwave Hydration Protocol; verify HVAC cooling in resident wings"
                )
            if is_cold:
                reasons.append(f"Cold Snap (Min {tmin}°C)")
                actions.append(
                    "Activate Hypothermia Prevention Protocol; check overnight ward heating"
                )
            if is_storm:
                reasons.append(f"Severe Storm/Wind ({w_desc}, Max Wind {wind} km/h)")
                actions.append(
                    "Secure outdoor mobility courtyards; verify backup generator readiness"
                )
            if is_uv:
                reasons.append(f"High UV (Index {uv})")
                actions.append(
                    "Enforce sun-protection & shade protocol between 10:00-16:00 local"
                )

            alert_id = hashlib.sha256(f"{loc_id}||{obs_date}".encode("utf-8")).hexdigest()

            alert_rows.append(
                {
                    "alert_id": alert_id,
                    "location_id": loc_id,
                    "location_name": str(r["location_name"]),
                    "state_code": str(r["state_code"]),
                    "climate_risk_tier": str(r["climate_risk_tier"]),
                    "observation_local_date": obs_date,
                    "is_forecast": is_forecast,
                    "data_horizon_type": horizon,
                    "overall_risk_severity": severity,
                    "is_heatwave_risk": is_heat,
                    "is_cold_snap_risk": is_cold,
                    "is_severe_storm_risk": is_storm,
                    "is_high_uv_risk": is_uv,
                    "primary_hazard_reason": (
                        " & ".join(reasons)
                        if reasons
                        else "All environmental parameters within safe care thresholds"
                    ),
                    "recommended_care_action": (
                        "; ".join(actions)
                        if actions
                        else "Standard routine resident care & environmental monitoring"
                    ),
                    "source_ingestion_id": str(r["source_ingestion_id"]),
                    "evaluated_at_utc": now_utc,
                }
            )

        df_alerts = pd.DataFrame(alert_rows)
        return self._upsert_gold_mart(
            df=df_alerts,
            mart_subfolder="mart_care_facility_risk_alerts",
            key_cols=["location_id", "observation_local_date"],
            arrow_schema=MART_RISK_ALERTS_SCHEMA,
        )

    def build_all_marts(self) -> Dict[str, int]:
        """Build both Gold analytical marts and return total processed rows."""
        c1 = self.build_mart_daily_weather_summary()
        c2 = self.build_mart_care_facility_risk_alerts()
        return {"rows_processed": c1 + c2, "rows_quarantined": 0}
