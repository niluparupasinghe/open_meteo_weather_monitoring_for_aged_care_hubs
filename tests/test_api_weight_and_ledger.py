"""Unit tests for Open-Meteo call weight calculation, response normalization, and post-consumption ledger."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import pytest
import requests
from deltalake import DeltaTable

from src.config.settings import PipelineSettings
from src.ingestion.call_weight_ledger import CallWeightLedger, calculate_open_meteo_weight
from src.ingestion.open_meteo_client import OpenMeteoClient


def test_calculate_open_meteo_weight_standard_and_floor() -> None:
    """Verify standard call weight calculation and the max(1.0, ...) baseline floor (Fix Section 5)."""
    # 8 locations, 14 days, 21 variables -> 8 * (14/14) * (21/10) = 16.8
    assert calculate_open_meteo_weight(num_locations=8, num_days=14, num_variables=21) == 16.8

    # Small query below 14 days and 10 variables must floor each multiplier at 1.0:
    # 1 location, 3 days, 4 variables -> 1 * max(1.0, 3/14) * max(1.0, 4/10) = 1.0
    assert calculate_open_meteo_weight(num_locations=1, num_days=3, num_variables=4) == 1.0

    # 3 locations, 7 days, 5 variables -> 3 * 1.0 * 1.0 = 3.0
    assert calculate_open_meteo_weight(num_locations=3, num_days=7, num_variables=5) == 3.0


def test_normalize_response_payload_single_vs_multi() -> None:
    """Verify that 1-location dict and N-location list responses normalize to List[Dict]."""
    single_obj = {"latitude": -33.85, "longitude": 151.20}
    res_single = OpenMeteoClient.normalize_response_payload(single_obj, expected_count=1)
    assert isinstance(res_single, list)
    assert len(res_single) == 1

    multi_obj = [{"latitude": -33.85}, {"latitude": -12.45}]
    res_multi = OpenMeteoClient.normalize_response_payload(multi_obj, expected_count=2)
    assert len(res_multi) == 2

    # Error payload from Open-Meteo raises ValueError
    with pytest.raises(ValueError, match="Open-Meteo API returned error payload"):
        OpenMeteoClient.normalize_response_payload(
            {"error": True, "reason": "Invalid parameter"}, expected_count=1
        )


def test_post_consumption_ledger_200_vs_non200_and_delta_flush(
    spark, isolated_settings: PipelineSettings, sample_open_meteo_batch_payload
) -> None:
    """Verify strict post-consumption rule:
    - HTTP 200 records consumed_call_weight > 0 and is_quota_consumed=True
    - HTTP 429/500 records consumed_call_weight = 0.0 and is_quota_consumed=False
    - Pre-flight network failure records nothing
    """
    ledger = CallWeightLedger(isolated_settings)
    locations = isolated_settings.load_locations()

    # 1. Simulate HTTP 200 OK response
    mock_session = MagicMock(spec=requests.Session)
    mock_resp_200 = MagicMock()
    mock_resp_200.status_code = 200
    mock_resp_200.json.return_value = sample_open_meteo_batch_payload
    mock_session.get.return_value = mock_resp_200

    client = OpenMeteoClient(settings=isolated_settings, ledger=ledger, session=mock_session)
    out = client.fetch_and_land_batch(locations=locations, pipeline_run_id="run-test-01")
    assert Path(out["landing_file_path"]).exists()

    # Check query parameters included per-location timezones (Fix 2.1)
    called_params = mock_session.get.call_args.kwargs["params"]
    assert called_params["timezone"] == "Australia/Sydney,Australia/Darwin"

    # 2. Simulate HTTP 429 response (server hit, but rejected -> weight = 0.0)
    mock_resp_429 = MagicMock()
    mock_resp_429.status_code = 429
    mock_resp_429.raise_for_status.side_effect = requests.HTTPError("429 Too Many Requests")
    mock_session.get.return_value = mock_resp_429

    with pytest.raises(requests.HTTPError):
        client.fetch_and_land_batch(locations=locations, pipeline_run_id="run-test-01")

    # Flush to Delta and verify ops.api_call_weight_ledger contents
    flushed = ledger.flush_to_delta(spark)
    assert flushed == 2

    df = DeltaTable(str(isolated_settings.ops_path / "api_call_weight_ledger")).to_pandas()
    rows = {int(r["http_status_code"]): r for _, r in df.iterrows()}

    assert bool(rows[200]["is_quota_consumed"]) is True
    assert float(rows[200]["consumed_call_weight"]) == 4.2  # 2 * max(1, 2/14) * max(1, 21/10) = 4.2
    assert bool(rows[429]["is_quota_consumed"]) is False
    assert float(rows[429]["consumed_call_weight"]) == 0.0
