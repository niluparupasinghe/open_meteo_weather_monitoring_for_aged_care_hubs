"""Post-consumption API Call Weight Ledger (ops.api_call_weight_ledger)."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, List, Optional

import pandas as pd
import pyarrow as pa
from deltalake import write_deltalake

from src.config.settings import PipelineSettings, get_settings
from src.utils.logger import get_logger

logger = get_logger(__name__)


def calculate_open_meteo_weight(
    num_locations: int,
    num_days: int,
    num_variables: int,
) -> float:
    """Calculate Open-Meteo weighted call cost with baseline floor of 1.0 per factor.

    Formula:
        Weight = nLocations * max(1.0, nDays / 14.0) * max(1.0, nVariables / 10.0)
    """
    if num_locations <= 0 or num_days <= 0 or num_variables <= 0:
        return 0.0
    day_factor = max(1.0, float(num_days) / 14.0)
    var_factor = max(1.0, float(num_variables) / 10.0)
    return round(float(num_locations) * day_factor * var_factor, 4)


@dataclass
class ApiCallLedgerRecord:
    """Schema for ops.api_call_weight_ledger."""

    call_id: str
    pipeline_run_id: str
    endpoint_url: str
    http_status_code: int
    is_quota_consumed: bool
    num_locations: int
    num_days: int
    num_variables: int
    consumed_call_weight: float
    rolling_1m_weight: float
    rolling_1h_weight: float
    rolling_24h_weight: float
    daily_quota_remaining_pct: float
    response_latency_ms: float
    landing_file_path: Optional[str]
    called_at_utc: str
    call_date: str


class CallWeightLedger:
    """Manages pre-flight rate-limit checks and strict post-HTTP response ledger writes."""

    def __init__(self, settings: Optional[PipelineSettings] = None) -> None:
        self.settings = settings or get_settings()
        self.local_index_file: Path = self.settings.ops_path / "_ledger_history.jsonl"
        self.local_index_file.parent.mkdir(parents=True, exist_ok=True)
        self._pending_spark_records: List[ApiCallLedgerRecord] = []

    def _load_recent_records(self, since_utc: datetime) -> List[dict]:
        """Read historical ledger records since a given UTC timestamp."""
        if not self.local_index_file.exists():
            return []
        recent: List[dict] = []
        with self.local_index_file.open("r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                rec = json.loads(line)
                called_at = datetime.fromisoformat(rec["called_at_utc"])
                if called_at >= since_utc:
                    recent.append(rec)
        return recent

    def compute_rolling_weights(
        self, now_utc: Optional[datetime] = None
    ) -> tuple[float, float, float]:
        """Compute trailing 1-minute, 1-hour, and 24-hour consumed weights."""
        now = now_utc or datetime.now(timezone.utc)
        records_24h = self._load_recent_records(now - timedelta(hours=24))

        cut_1m = now - timedelta(minutes=1)
        cut_1h = now - timedelta(hours=1)

        w_1m = 0.0
        w_1h = 0.0
        w_24h = 0.0

        for r in records_24h:
            w = float(r.get("consumed_call_weight", 0.0))
            t = datetime.fromisoformat(r["called_at_utc"])
            w_24h += w
            if t >= cut_1h:
                w_1h += w
            if t >= cut_1m:
                w_1m += w

        return round(w_1m, 4), round(w_1h, 4), round(w_24h, 4)

    def verify_pre_flight_budget(
        self,
        num_locations: int,
        num_days: int,
        num_variables: int,
    ) -> float:
        """Verify that executing the upcoming call will not breach safety-buffered rate limits."""
        planned_weight = calculate_open_meteo_weight(num_locations, num_days, num_variables)
        w_1m, w_1h, w_24h = self.compute_rolling_weights()
        buf = self.settings.api_safety_buffer_ratio

        max_1m = self.settings.api_rate_limit_per_minute * buf
        max_1h = self.settings.api_rate_limit_per_hour * buf
        max_24h = self.settings.api_rate_limit_per_day * buf

        if (w_1m + planned_weight) > max_1m:
            raise RuntimeError(
                f"Rate-limit guard triggered: 1-minute weight ({w_1m + planned_weight:.2f}) "
                f"would exceed safety cap ({max_1m:.2f})."
            )
        if (w_1h + planned_weight) > max_1h:
            raise RuntimeError(
                f"Rate-limit guard triggered: 1-hour weight ({w_1h + planned_weight:.2f}) "
                f"would exceed safety cap ({max_1h:.2f})."
            )
        if (w_24h + planned_weight) > max_24h:
            raise RuntimeError(
                f"Rate-limit guard triggered: 24-hour weight ({w_24h + planned_weight:.2f}) "
                f"would exceed safety cap ({max_24h:.2f})."
            )
        return planned_weight

    def record_post_response(
        self,
        call_id: str,
        pipeline_run_id: str,
        endpoint_url: str,
        http_status_code: int,
        num_locations: int,
        num_days: int,
        num_variables: int,
        response_latency_ms: float,
        landing_file_path: Optional[str] = None,
        called_at_utc: Optional[datetime] = None,
    ) -> ApiCallLedgerRecord:
        """Record an API call in the ledger STRICTLY AFTER receiving an HTTP server response.

        - HTTP 200 OK -> is_quota_consumed=True, consumed_call_weight > 0
        - HTTP 4xx/5xx -> is_quota_consumed=False, consumed_call_weight = 0.0
        """
        now = called_at_utc or datetime.now(timezone.utc)
        is_consumed = int(http_status_code) == 200
        weight = (
            calculate_open_meteo_weight(num_locations, num_days, num_variables)
            if is_consumed
            else 0.0
        )

        w_1m, w_1h, w_24h = self.compute_rolling_weights(now)
        new_1m = round(w_1m + weight, 4)
        new_1h = round(w_1h + weight, 4)
        new_24h = round(w_24h + weight, 4)

        remaining_pct = round(
            max(0.0, (1.0 - (new_24h / self.settings.api_rate_limit_per_day)) * 100.0),
            2,
        )

        record = ApiCallLedgerRecord(
            call_id=call_id,
            pipeline_run_id=pipeline_run_id,
            endpoint_url=endpoint_url,
            http_status_code=int(http_status_code),
            is_quota_consumed=is_consumed,
            num_locations=int(num_locations),
            num_days=int(num_days),
            num_variables=int(num_variables),
            consumed_call_weight=weight,
            rolling_1m_weight=new_1m,
            rolling_1h_weight=new_1h,
            rolling_24h_weight=new_24h,
            daily_quota_remaining_pct=remaining_pct,
            response_latency_ms=round(float(response_latency_ms), 2),
            landing_file_path=landing_file_path,
            called_at_utc=now.isoformat(),
            call_date=now.strftime("%Y-%m-%d"),
        )

        with self.local_index_file.open("a", encoding="utf-8") as f:
            f.write(json.dumps(asdict(record)) + "\n")

        self._pending_spark_records.append(record)
        logger.info(
            "Recorded post-consumption API ledger entry: call_id=%s status=%d weight=%.2f "
            "rolling_24h=%.2f remaining_daily_quota=%.2f%%",
            call_id,
            http_status_code,
            weight,
            new_24h,
            remaining_pct,
        )
        return record

    def flush_to_delta(self, spark: Optional[Any] = None) -> int:
        """Persist pending ledger records to the ops.api_call_weight_ledger Delta table."""
        if not self._pending_spark_records:
            return 0

        schema = pa.schema(
            [
                pa.field("call_id", pa.string(), nullable=False),
                pa.field("pipeline_run_id", pa.string(), nullable=False),
                pa.field("endpoint_url", pa.string(), nullable=False),
                pa.field("http_status_code", pa.int32(), nullable=False),
                pa.field("is_quota_consumed", pa.bool_(), nullable=False),
                pa.field("num_locations", pa.int32(), nullable=False),
                pa.field("num_days", pa.int32(), nullable=False),
                pa.field("num_variables", pa.int32(), nullable=False),
                pa.field("consumed_call_weight", pa.float64(), nullable=False),
                pa.field("rolling_1m_weight", pa.float64(), nullable=False),
                pa.field("rolling_1h_weight", pa.float64(), nullable=False),
                pa.field("rolling_24h_weight", pa.float64(), nullable=False),
                pa.field("daily_quota_remaining_pct", pa.float64(), nullable=False),
                pa.field("response_latency_ms", pa.float64(), nullable=False),
                pa.field("landing_file_path", pa.string(), nullable=True),
                pa.field("called_at_utc", pa.timestamp("us", tz="UTC"), nullable=False),
                pa.field("call_date", pa.date32(), nullable=False),
            ]
        )

        rows = [asdict(r) for r in self._pending_spark_records]
        df = pd.DataFrame(rows)
        df["called_at_utc"] = pd.to_datetime(df["called_at_utc"], utc=True)
        df["call_date"] = pd.to_datetime(df["call_date"]).dt.date

        target_path = self.settings.ops_path / "api_call_weight_ledger"
        target_path.mkdir(parents=True, exist_ok=True)
        table = pa.Table.from_pandas(df, schema=schema, preserve_index=False)
        write_deltalake(
            str(target_path),
            table,
            mode="append",
            partition_by=["call_date"],
        )

        count = len(self._pending_spark_records)
        self._pending_spark_records.clear()
        return count
