"""Tests for Silver Time Semantics (Fix 2.1), SCD Type 2 Dimension, and 3-Guard Idempotent MERGE (Fix 2.3)."""

from __future__ import annotations

import json

import pandas as pd
from deltalake import DeltaTable

from src.config.settings import PipelineSettings
from src.ingestion.bronze_writer import BronzeWriter
from src.transformation.silver_transformer import SilverTransformer


def test_local_time_and_utc_offset_conversion(
    spark, isolated_settings: PipelineSettings, sample_open_meteo_batch_payload
) -> None:
    """Verify Fix 2.1:
    Sydney '2026-10-23T01:00' with utc_offset_seconds=39600 (+11h) must produce:
    - observation_local_date = 2026-10-23 (local calendar day)
    - observation_time_utc   = 2026-10-22 14:00:00 UTC
    """
    landing_file = isolated_settings.landing_raw_json_dir / "batch_time_test.json"
    envelope = {
        "call_id": "call-time-01",
        "pipeline_run_id": "run-time-01",
        "ingested_at_utc": "2026-10-23T00:00:00+00:00",
        "ingestion_date": "2026-10-23",
        "endpoint_url": isolated_settings.open_meteo_base_url,
        "locations_metadata": [loc.model_dump() for loc in isolated_settings.load_locations()],
        "payloads": sample_open_meteo_batch_payload,
    }
    landing_file.write_text(json.dumps(envelope), encoding="utf-8")

    bw = BronzeWriter(spark, isolated_settings)
    bw.write_hourly(landing_file, pipeline_run_id="run-time-01")

    st = SilverTransformer(spark, isolated_settings)
    st.transform_fact_hourly(pipeline_run_id="run-time-01")

    hr_df = DeltaTable(
        str(isolated_settings.silver_path / "fact_weather_hourly")
    ).to_pandas()
    matched = hr_df[
        (hr_df["location_id"] == "AU-NSW-SYD-01")
        & (hr_df["observation_local_time"] == "2026-10-23T01:00")
    ].iloc[0]

    assert str(pd.to_datetime(matched["observation_local_date"]).date()) == "2026-10-23"
    utc_ts = pd.to_datetime(matched["observation_time_utc"], utc=True)
    assert utc_ts.strftime("%Y-%m-%d %H:%M:%S") == "2026-10-22 14:00:00"

    # Also verify Darwin (AU-NT-DRW-01) non-integer +09:30 (utc_offset_seconds=34200):
    # 1) In-table row '2026-10-23T14:00' ACST -> 2026-10-23 04:30:00 UTC
    drw_matched = hr_df[
        (hr_df["location_id"] == "AU-NT-DRW-01")
        & (hr_df["observation_local_time"] == "2026-10-23T14:00")
    ].iloc[0]
    assert str(pd.to_datetime(drw_matched["observation_local_date"]).date()) == "2026-10-23"
    drw_utc_ts = pd.to_datetime(drw_matched["observation_time_utc"], utc=True)
    assert drw_utc_ts.strftime("%Y-%m-%d %H:%M:%S") == "2026-10-23 04:30:00"

    # 2) Half-hour midnight boundary crossing: '2026-10-23T01:00' ACST (+09:30) -> 2026-10-22 15:30:00 UTC
    local_d, utc_dt = st._parse_local_iso_and_utc("2026-10-23T01:00", 34200)
    assert str(local_d) == "2026-10-23"
    assert utc_dt is not None and utc_dt.strftime("%Y-%m-%d %H:%M:%S") == "2026-10-22 15:30:00"


def test_scd2_dim_location_and_3_guard_idempotent_merge(
    spark, isolated_settings: PipelineSettings, sample_open_meteo_batch_payload
) -> None:
    """Verify:
    1. SCD Type 2 expires old active row and inserts new version when attribute changes.
    2. 3-Guard Silver MERGE does NOT overwrite newer data when an older batch is replayed,
       and only updates when api_ingested_at_utc is newer AND row_value_hash changed.
    """
    bw = BronzeWriter(spark, isolated_settings)
    st = SilverTransformer(spark, isolated_settings)

    # Batch 1 at 2026-10-23T10:00:00Z (Sydney temp at 2026-10-23T01:00 = 19.5 C)
    f1 = isolated_settings.landing_raw_json_dir / "batch_1.json"
    env1 = {
        "call_id": "call-1",
        "pipeline_run_id": "run-1",
        "ingested_at_utc": "2026-10-23T10:00:00+00:00",
        "ingestion_date": "2026-10-23",
        "endpoint_url": isolated_settings.open_meteo_base_url,
        "locations_metadata": [loc.model_dump() for loc in isolated_settings.load_locations()],
        "payloads": sample_open_meteo_batch_payload,
    }
    f1.write_text(json.dumps(env1), encoding="utf-8")
    bw.write_hourly(f1, pipeline_run_id="run-1")
    st.transform_dim_location_scd2(pipeline_run_id="run-1")
    st.transform_fact_hourly(pipeline_run_id="run-1")

    # Batch 2 (OLDER timestamp: 2026-10-23T08:00:00Z) trying to overwrite Sydney temp with 9.9 C
    # Guard 3 must BLOCK this stale update!
    stale_payloads = json.loads(json.dumps(sample_open_meteo_batch_payload))
    stale_payloads[0]["hourly"]["temperature_2m"][1] = 9.9
    f_stale = isolated_settings.landing_raw_json_dir / "batch_stale.json"
    env_stale = {
        "call_id": "call-stale",
        "pipeline_run_id": "run-stale",
        "ingested_at_utc": "2026-10-23T08:00:00+00:00",
        "ingestion_date": "2026-10-23",
        "endpoint_url": isolated_settings.open_meteo_base_url,
        "locations_metadata": [loc.model_dump() for loc in isolated_settings.load_locations()],
        "payloads": stale_payloads,
    }
    f_stale.write_text(json.dumps(env_stale), encoding="utf-8")
    bw.write_hourly(f_stale, pipeline_run_id="run-stale")
    st.transform_fact_hourly(pipeline_run_id="run-stale")

    hr_df = DeltaTable(
        str(isolated_settings.silver_path / "fact_weather_hourly")
    ).to_pandas()
    val_after_stale = float(
        hr_df[
            (hr_df["location_id"] == "AU-NSW-SYD-01")
            & (hr_df["observation_local_time"] == "2026-10-23T01:00")
        ].iloc[0]["temperature_2m_c"]
    )
    assert val_after_stale == 19.5  # Unchanged by older replay!

    # Batch 3 (NEWER timestamp: 2026-10-23T12:00:00Z) updates Sydney elevation (42.0 -> 45.0)
    # and updates Sydney temp (19.5 -> 21.0 C)
    newer_payloads = json.loads(json.dumps(sample_open_meteo_batch_payload))
    newer_payloads[0]["elevation"] = 45.0
    newer_payloads[0]["hourly"]["temperature_2m"][1] = 21.0
    f_new = isolated_settings.landing_raw_json_dir / "batch_new.json"
    env_new = {
        "call_id": "call-new",
        "pipeline_run_id": "run-new",
        "ingested_at_utc": "2026-10-23T12:00:00+00:00",
        "ingestion_date": "2026-10-23",
        "endpoint_url": isolated_settings.open_meteo_base_url,
        "locations_metadata": [loc.model_dump() for loc in isolated_settings.load_locations()],
        "payloads": newer_payloads,
    }
    f_new.write_text(json.dumps(env_new), encoding="utf-8")
    bw.write_hourly(f_new, pipeline_run_id="run-new")
    st.transform_dim_location_scd2(pipeline_run_id="run-new")
    st.transform_fact_hourly(pipeline_run_id="run-new")

    # Verify SCD2 history on silver.dim_location_scd2 for AU-NSW-SYD-01 (1 expired + 1 current = 2 rows)
    dim_df = DeltaTable(
        str(isolated_settings.silver_path / "dim_location_scd2")
    ).to_pandas()
    syd_versions = dim_df[dim_df["location_id"] == "AU-NSW-SYD-01"].sort_values(
        "effective_from_utc"
    )
    assert len(syd_versions) == 2
    assert bool(syd_versions.iloc[0]["is_current"]) is False
    assert float(syd_versions.iloc[0]["elevation_m"]) == 42.0
    assert bool(syd_versions.iloc[1]["is_current"]) is True
    assert float(syd_versions.iloc[1]["elevation_m"]) == 45.0

    # Verify newer fact update succeeded (19.5 -> 21.0)
    hr_df_new = DeltaTable(
        str(isolated_settings.silver_path / "fact_weather_hourly")
    ).to_pandas()
    val_after_new = float(
        hr_df_new[
            (hr_df_new["location_id"] == "AU-NSW-SYD-01")
            & (hr_df_new["observation_local_time"] == "2026-10-23T01:00")
        ].iloc[0]["temperature_2m_c"]
    )
    assert val_after_new == 21.0

    # Replay the exact same batch (run-new) with identical timestamp and values:
    # Verify row count AND row_value_hash / temperature remain completely unchanged
    row_count_before_replay = len(hr_df_new)
    st.transform_fact_hourly(pipeline_run_id="run-new")
    hr_df_after_replay = DeltaTable(
        str(isolated_settings.silver_path / "fact_weather_hourly")
    ).to_pandas()
    assert len(hr_df_after_replay) == row_count_before_replay
    syd_row_replay = hr_df_after_replay[
        (hr_df_after_replay["location_id"] == "AU-NSW-SYD-01")
        & (hr_df_after_replay["observation_local_time"] == "2026-10-23T01:00")
    ].iloc[0]
    assert float(syd_row_replay["temperature_2m_c"]) == 21.0


def test_guard2_same_run_duplicate_primary_keys_deduplicated(
    spark, isolated_settings: PipelineSettings, sample_open_meteo_batch_payload
) -> None:
    """Verify Guard 2: When two Bronze records for the same location_id exist in the same
    pipeline_run_id with different ingested_at_utc, Silver deduplicates on primary grain
    and keeps only the record with the latest api_ingested_at_utc.
    """
    bw = BronzeWriter(spark, isolated_settings)
    st = SilverTransformer(spark, isolated_settings)

    # First Bronze batch in run-dup at 10:00Z (Sydney hour 1 = 19.5 C)
    f_early = isolated_settings.landing_raw_json_dir / "batch_dup_early.json"
    env_early = {
        "call_id": "call-dup-1",
        "pipeline_run_id": "run-dup",
        "ingested_at_utc": "2026-10-23T10:00:00+00:00",
        "ingestion_date": "2026-10-23",
        "endpoint_url": isolated_settings.open_meteo_base_url,
        "locations_metadata": [loc.model_dump() for loc in isolated_settings.load_locations()],
        "payloads": sample_open_meteo_batch_payload,
    }
    f_early.write_text(json.dumps(env_early), encoding="utf-8")
    bw.write_hourly(f_early, pipeline_run_id="run-dup")

    # Second Bronze batch in the SAME run-dup at 10:05Z (Sydney hour 1 = 23.4 C)
    later_payloads = json.loads(json.dumps(sample_open_meteo_batch_payload))
    later_payloads[0]["hourly"]["temperature_2m"][1] = 23.4
    f_late = isolated_settings.landing_raw_json_dir / "batch_dup_late.json"
    env_late = {
        "call_id": "call-dup-2",
        "pipeline_run_id": "run-dup",
        "ingested_at_utc": "2026-10-23T10:05:00+00:00",
        "ingestion_date": "2026-10-23",
        "endpoint_url": isolated_settings.open_meteo_base_url,
        "locations_metadata": [loc.model_dump() for loc in isolated_settings.load_locations()],
        "payloads": later_payloads,
    }
    f_late.write_text(json.dumps(env_late), encoding="utf-8")
    bw.write_hourly(f_late, pipeline_run_id="run-dup")

    res = st.transform_fact_hourly(pipeline_run_id="run-dup")
    assert res["rows_processed"] == 6  # 3 Sydney + 3 Darwin deduplicated rows (not 12!)

    hr_df = DeltaTable(
        str(isolated_settings.silver_path / "fact_weather_hourly")
    ).to_pandas()
    assert len(hr_df) == 6
    syd_h1 = hr_df[
        (hr_df["location_id"] == "AU-NSW-SYD-01")
        & (hr_df["observation_local_time"] == "2026-10-23T01:00")
    ]
    assert len(syd_h1) == 1
    assert float(syd_h1.iloc[0]["temperature_2m_c"]) == 23.4
