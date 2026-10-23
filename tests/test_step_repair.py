"""Tests for Step-Level Audit Logging and Checkpoint Repair Runs (cli.py repair)."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest
import requests

from src.cli import execute_pipeline
from src.config.settings import PipelineSettings
from src.ingestion.call_weight_ledger import CallWeightLedger
from src.ingestion.open_meteo_client import OpenMeteoClient
from src.utils.step_tracker import StepTracker


def test_pipeline_failure_and_checkpoint_repair_without_recalling_api(
    spark, isolated_settings: PipelineSettings, sample_open_meteo_batch_payload
) -> None:
    """Simulate a failure at step_6_bronze_to_silver_facts, then run repair and verify:
    1. Steps 1-5 are NOT re-executed (marked SKIPPED_ON_REPAIR).
    2. The Open-Meteo HTTP API is called only ONCE across both runs.
    3. Steps 6 and 7 succeed on repair.
    """
    mock_session = MagicMock(spec=requests.Session)
    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.json.return_value = sample_open_meteo_batch_payload
    mock_session.get.return_value = mock_resp

    ledger = CallWeightLedger(isolated_settings)
    client = OpenMeteoClient(settings=isolated_settings, ledger=ledger, session=mock_session)

    # Run 1: Fail intentionally at step_6_bronze_to_silver_facts
    with pytest.raises(RuntimeError, match="Simulated failure at step_6_bronze_to_silver_facts"):
        execute_pipeline(
            settings=isolated_settings,
             resume_run_id="run-repair-test-01",
            spark_override=spark,
            client_override=client,
            fail_at_step="step_6_bronze_to_silver_facts",
        )

    # Confirm HTTP API was called once during Run 1
    assert mock_session.get.call_count == 1

    # Run 2: Execute Repair Run for "run-repair-test-01"
    res = execute_pipeline(
        settings=isolated_settings,
        resume_run_id="run-repair-test-01",
        spark_override=spark,
        client_override=client,
        fail_at_step=None,
    )
    assert res["status"] == "SUCCESS"

    # Confirm HTTP API was NOT called again during the repair run!
    assert mock_session.get.call_count == 1

    tracker = StepTracker(pipeline_run_id="run-repair-test-01", settings=isolated_settings)
    history = tracker.get_run_history("run-repair-test-01")
    skipped = [
        e["step_name"] for e in history if e["step_status"] == "SKIPPED_ON_REPAIR"
    ]
    assert "step_1_ingest_to_landing" in skipped
    assert "step_2_landing_to_bronze_minutely_15" in skipped
    assert "step_3_landing_to_bronze_hourly" in skipped
    assert "step_4_landing_to_bronze_daily" in skipped
    assert "step_5_bronze_to_silver_dim_location" in skipped
