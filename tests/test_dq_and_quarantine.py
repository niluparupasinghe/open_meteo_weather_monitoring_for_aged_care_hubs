"""Tests for the 13 Stage-1 & Stage-2 Data Quality checks and silver.quarantine_weather_records."""

from __future__ import annotations

import json
from datetime import datetime, timezone

from deltalake import DeltaTable

from src.config.settings import PipelineSettings
from src.ingestion.bronze_writer import BronzeWriter
from src.transformation.silver_transformer import SilverTransformer


def test_stage1_and_stage2_dq_rules_and_wmo_97_pass(
    spark, isolated_settings: PipelineSettings, sample_open_meteo_batch_payload
) -> None:
    """Verify:
    1. WMO code 97 (Heavy thunderstorm without hail) passes DQ-DOM-001 (Fix 2.2).
    2. Out-of-bounds temperature (145.0 C), humidity (112.0%), invalid WMO code (999),
       and daily temp_min > temp_max are routed to silver.quarantine_weather_records.
    3. Stage-1 array length mismatch is quarantined before explosion.
    """
    payloads = json.loads(json.dumps(sample_open_meteo_batch_payload))

    # Inject Stage 2 row violations in Location 0 (Sydney):
    # Hour 0: valid
    # Hour 1: temperature = 145.0 (DQ-PHY-001) and humidity = 112.0 (DQ-PHY-002)
    # Hour 2: weather_code = 999 (DQ-DOM-001)
    payloads[0]["hourly"]["temperature_2m"][1] = 145.0
    payloads[0]["hourly"]["relative_humidity_2m"][1] = 112.0
    payloads[0]["hourly"]["weather_code"][2] = 999

    # Inject Daily logic violation in Sydney day 0: temp_min (30.0) > temp_max (24.0) -> DQ-LOGIC-001
    payloads[0]["daily"]["temperature_2m_min"][0] = 30.0

    # Inject Stage 1 structural array length mismatch in Sydney minutely_15:
    # time has 2 elements, but temperature_2m has only 1 element -> DQ-STRUCT-002
    payloads[0]["minutely_15"]["temperature_2m"] = [19.5]

    landing_file = isolated_settings.landing_raw_json_dir / "batch_dq_test.json"
    envelope = {
        "call_id": "call-dq-01",
        "pipeline_run_id": "run-dq-01",
        "ingested_at_utc": datetime.now(timezone.utc).isoformat(),
        "ingestion_date": "2026-10-23",
        "endpoint_url": isolated_settings.open_meteo_base_url,
        "locations_metadata": [loc.model_dump() for loc in isolated_settings.load_locations()],
        "payloads": payloads,
    }
    landing_file.write_text(json.dumps(envelope), encoding="utf-8")

    bw = BronzeWriter(spark, isolated_settings)
    bw.write_minutely_15(landing_file, pipeline_run_id="run-dq-01")
    bw.write_hourly(landing_file, pipeline_run_id="run-dq-01")
    bw.write_daily(landing_file, pipeline_run_id="run-dq-01")

    st = SilverTransformer(spark, isolated_settings)
    res_m15 = st.transform_fact_minutely_15(pipeline_run_id="run-dq-01")
    res_hr = st.transform_fact_hourly(pipeline_run_id="run-dq-01")
    res_dy = st.transform_fact_daily(pipeline_run_id="run-dq-01")

    # 1. Verify Stage 1 structural failure quarantined Sydney minutely_15 payload (1 row),
    # while Darwin minutely_15 (2 rows, including WMO 97!) passed!
    assert res_m15["rows_quarantined"] == 1
    assert res_m15["rows_processed"] == 2

    # 2. Verify Hourly quarantined 2 bad Sydney rows, while 1 valid Sydney row + 3 Darwin rows (incl. WMO 97) passed!
    assert res_hr["rows_quarantined"] == 2
    assert res_hr["rows_processed"] == 4

    # 3. Verify Daily quarantined 1 Sydney row (DQ-LOGIC-001), while 2 Sydney + 3 Darwin rows passed!
    assert res_dy["rows_quarantined"] == 1
    assert res_dy["rows_processed"] == 5

    # Inspect silver.quarantine_weather_records
    q_df = DeltaTable(
        str(isolated_settings.silver_path / "quarantine_weather_records")
    ).to_pandas()
    q_rules = list(q_df["dq_rule_violated"])
    assert "DQ-STRUCT-002" in q_rules
    assert "DQ-PHY-001,DQ-PHY-002" in q_rules
    assert "DQ-DOM-001" in q_rules
    assert "DQ-LOGIC-001" in q_rules

    # Verify WMO 97 in Darwin was mapped to "Thunderstorm: Heavy without hail" in Silver
    hr_df = DeltaTable(
        str(isolated_settings.silver_path / "fact_weather_hourly")
    ).to_pandas()
    wmo97_rows = hr_df[hr_df["weather_code"] == 97]
    assert len(wmo97_rows) == 1
    assert wmo97_rows.iloc[0]["weather_description"] == "Thunderstorm: Heavy without hail"
