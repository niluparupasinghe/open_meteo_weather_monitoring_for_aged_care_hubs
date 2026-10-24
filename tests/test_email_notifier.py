"""Tests for the 7-step pipeline run email notification builder and SMTP dispatcher."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from src.config.settings import PipelineSettings
from src.ingestion.call_weight_ledger import CallWeightLedger
from src.utils.email_notifier import send_pipeline_run_email
from src.utils.step_tracker import StepTracker


def _build_test_settings(tmp_path: Path, email_enabled: bool = False) -> PipelineSettings:
    return PipelineSettings(
        LOG_DIR=tmp_path / "logs",
        PIPELINE_STEP_AUDIT_FILE=tmp_path / "logs" / "pipeline_steps_audit.jsonl",
        DATA_ROOT_DIR=tmp_path / "data",
        LANDING_RAW_JSON_DIR=tmp_path / "data" / "landing" / "raw_json",
        OPS_PATH=tmp_path / "data" / "lakehouse" / "ops",
        BRONZE_PATH=tmp_path / "data" / "lakehouse" / "bronze",
        SILVER_PATH=tmp_path / "data" / "lakehouse" / "silver",
        GOLD_PATH=tmp_path / "data" / "lakehouse" / "gold",
        ALERT_EMAIL_ENABLED=email_enabled,
        SMTP_HOST="smtp.example.com",
        SMTP_PORT=587,
        SMTP_USE_TLS=True,
        SMTP_USERNAME="alerts@example.com",
        SMTP_PASSWORD="secret-app-password",
        ALERT_EMAIL_FROM="alerts@example.com",
        ALERT_EMAIL_TO="data-eng@example.com",
    )


def test_email_report_on_step_3_failure_renders_all_7_steps(tmp_path: Path) -> None:
    """Verify that when Step 3 fails, the email report contains all 7 steps (1-2 SUCCESS, 3 FAILED, 4-7 NOT_RUN)."""
    cfg = _build_test_settings(tmp_path, email_enabled=False)
    tracker = StepTracker(pipeline_run_id="run-test-fail-step3", settings=cfg)
    ledger = CallWeightLedger(cfg)

    tracker.run_step(
        "step_1_ingest_to_landing",
        lambda: {"rows_processed": 8, "input_artifact_ref": "batch_123.json"},
    )
    tracker.run_step(
        "step_2_landing_to_bronze_minutely_15",
        lambda: {"rows_processed": 8, "input_artifact_ref": "batch_123.json"},
    )

    with pytest.raises(ValueError, match="Corrupted hourly JSON block"):
        tracker.run_step(
            "step_3_landing_to_bronze_hourly",
            lambda: (_ for _ in ()).throw(ValueError("Corrupted hourly JSON block")),
        )

    report = send_pipeline_run_email(tracker=tracker, ledger=ledger, settings=cfg)

    assert report["overall_status"] == "FAILED"
    assert "[FAILED at Step 3]" in report["subject"]
    assert Path(report["preview_path"]).exists()

    rows = report["step_rows"]
    assert len(rows) == 7
    assert rows[0]["step_status"] == "SUCCESS"
    assert rows[1]["step_status"] == "SUCCESS"
    assert rows[2]["step_status"] == "FAILED"
    assert "Corrupted hourly JSON block" in rows[2]["details"]
    assert all(r["step_status"] == "NOT_RUN" for r in rows[3:])
    assert "python -m src.cli repair --run-id run-test-fail-step3" in report["html_body"]


def test_email_report_smtp_dispatch_on_success(tmp_path: Path) -> None:
    """Verify that when ALERT_EMAIL_ENABLED=true, the 7-step email is dispatched via smtplib.SMTP."""
    cfg = _build_test_settings(tmp_path, email_enabled=True)
    tracker = StepTracker(pipeline_run_id="run-test-success-all", settings=cfg)
    ledger = CallWeightLedger(cfg)

    for step_name in [
        "step_1_ingest_to_landing",
        "step_2_landing_to_bronze_minutely_15",
        "step_3_landing_to_bronze_hourly",
        "step_4_landing_to_bronze_daily",
        "step_5_bronze_to_silver_dim_location",
        "step_6_bronze_to_silver_facts",
        "step_7_silver_to_gold_marts",
    ]:
        tracker.run_step(step_name, lambda: {"rows_processed": 8})

    with patch("src.utils.email_notifier.smtplib.SMTP") as mock_smtp_cls:
        mock_server = MagicMock()
        mock_smtp_cls.return_value.__enter__.return_value = mock_server

        report = send_pipeline_run_email(tracker=tracker, ledger=ledger, settings=cfg)

        assert report["overall_status"] == "SUCCESS"
        assert report["sent_via_smtp"] is True
        assert "[SUCCESS]" in report["subject"]
        mock_server.starttls.assert_called_once()
        mock_server.login.assert_called_once_with("alerts@example.com", "secret-app-password")
        mock_server.send_message.assert_called_once()
