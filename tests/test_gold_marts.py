"""Tests for Gold Layer Marts: local date joins, horizon labels, and care facility risk alerts."""

from __future__ import annotations

import json

import pandas as pd
from deltalake import DeltaTable

from src.config.settings import PipelineSettings
from src.ingestion.bronze_writer import BronzeWriter
from src.transformation.gold_transformer import GoldTransformer
from src.transformation.silver_transformer import SilverTransformer


def test_gold_marts_horizon_and_risk_alerts(
    spark, isolated_settings: PipelineSettings, sample_open_meteo_batch_payload
) -> None:
    """Verify Gold marts join on observation_local_date, classify risk severity accurately,
    and flag Darwin's 36.8°C + WMO 97 Heavy Thunderstorm as CRITICAL.
    """
    f1 = isolated_settings.landing_raw_json_dir / "batch_gold.json"
    env1 = {
        "call_id": "call-gold",
        "pipeline_run_id": "run-gold",
        "ingested_at_utc": "2026-10-23T01:00:00+00:00",
        "ingestion_date": "2026-10-23",
        "endpoint_url": isolated_settings.open_meteo_base_url,
        "locations_metadata": [loc.model_dump() for loc in isolated_settings.load_locations()],
        "payloads": sample_open_meteo_batch_payload,
    }
    f1.write_text(json.dumps(env1), encoding="utf-8")

    bw = BronzeWriter(spark, isolated_settings)
    bw.write_minutely_15(f1, pipeline_run_id="run-gold")
    bw.write_hourly(f1, pipeline_run_id="run-gold")
    bw.write_daily(f1, pipeline_run_id="run-gold")

    st = SilverTransformer(spark, isolated_settings)
    st.transform_dim_location_scd2(pipeline_run_id="run-gold")
    st.transform_all_facts(pipeline_run_id="run-gold")

    gt = GoldTransformer(spark, isolated_settings)
    res = gt.build_all_marts()
    assert res["rows_processed"] == 12  # 6 daily summary rows + 6 risk alert rows

    summary_df = DeltaTable(
        str(isolated_settings.gold_path / "mart_daily_weather_summary")
    ).to_pandas()
    assert "peak_15m_precip_mm" not in summary_df.columns
    horizons = set(summary_df["data_horizon_type"])
    assert horizons.issubset({"PAST_MODELLED", "TODAY_MIXED", "FORWARD_FORECAST"})

    alerts_df = DeltaTable(
        str(isolated_settings.gold_path / "mart_care_facility_risk_alerts")
    ).to_pandas()
    alerts_df["obs_str"] = pd.to_datetime(alerts_df["observation_local_date"]).dt.strftime(
        "%Y-%m-%d"
    )
    drw_oct23 = alerts_df[
        (alerts_df["location_id"] == "AU-NT-DRW-01") & (alerts_df["obs_str"] == "2026-10-23")
    ].iloc[0]

    assert drw_oct23["overall_risk_severity"] == "CRITICAL"
    assert bool(drw_oct23["is_heatwave_risk"]) is True
    assert bool(drw_oct23["is_severe_storm_risk"]) is True
    assert bool(drw_oct23["is_high_uv_risk"]) is True
    assert "Heatwave Hydration Protocol" in drw_oct23["recommended_care_action"]
