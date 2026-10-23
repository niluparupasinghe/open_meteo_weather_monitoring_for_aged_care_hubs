"""Step-by-step execution audit logger and checkpoint manager for repair runs."""

from __future__ import annotations

import json
import time
import traceback
import uuid
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

import pandas as pd
import pyarrow as pa
from deltalake import write_deltalake

from src.config.settings import PipelineSettings, get_settings
from src.utils.logger import get_logger

logger = get_logger(__name__)

PIPELINE_STEPS_ORDER: Dict[str, int] = {
    "step_1_ingest_to_landing": 1,
    "step_2_landing_to_bronze_minutely_15": 2,
    "step_3_landing_to_bronze_hourly": 3,
    "step_4_landing_to_bronze_daily": 4,
    "step_5_bronze_to_silver_dim_location": 5,
    "step_6_bronze_to_silver_facts": 6,
    "step_7_silver_to_gold_marts": 7,
}


@dataclass
class StepAuditEvent:
    """Represents a single step execution status event in ops.pipeline_step_execution_log."""

    step_log_id: str
    pipeline_run_id: str
    step_sequence_order: int
    step_name: str
    step_status: str  # STARTED, SUCCESS, FAILED, SKIPPED_ON_REPAIR
    input_artifact_ref: Optional[str]
    rows_processed: int
    rows_quarantined: int
    duration_seconds: float
    error_type: Optional[str]
    error_message: Optional[str]
    logged_at_utc: str


class StepTracker:
    """Tracks pipeline step outcomes to both local JSONL and Delta table for repair runs."""

    def __init__(
        self,
        pipeline_run_id: Optional[str] = None,
        settings: Optional[PipelineSettings] = None,
    ) -> None:
        self.settings = settings or get_settings()
        now_str = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
        short_id = uuid.uuid4().hex[:6]
        self.pipeline_run_id = pipeline_run_id or f"run-{now_str}-{short_id}"
        self.audit_file: Path = self.settings.pipeline_step_audit_file
        self.audit_file.parent.mkdir(parents=True, exist_ok=True)
        self._events: List[StepAuditEvent] = []

    def _append_event(self, event: StepAuditEvent) -> None:
        """Persist event synchronously to local JSONL file so crashes never lose state."""
        self._events.append(event)
        with self.audit_file.open("a", encoding="utf-8") as f:
            f.write(json.dumps(asdict(event)) + "\n")

    def record_event(
        self,
        step_name: str,
        step_status: str,
        input_artifact_ref: Optional[str] = None,
        rows_processed: int = 0,
        rows_quarantined: int = 0,
        duration_seconds: float = 0.0,
        error_type: Optional[str] = None,
        error_message: Optional[str] = None,
    ) -> StepAuditEvent:
        """Create and persist a StepAuditEvent."""
        seq = PIPELINE_STEPS_ORDER.get(step_name, 99)
        event = StepAuditEvent(
            step_log_id=str(uuid.uuid4()),
            pipeline_run_id=self.pipeline_run_id,
            step_sequence_order=seq,
            step_name=step_name,
            step_status=step_status,
            input_artifact_ref=input_artifact_ref,
            rows_processed=int(rows_processed),
            rows_quarantined=int(rows_quarantined),
            duration_seconds=round(float(duration_seconds), 4),
            error_type=error_type,
            error_message=error_message,
            logged_at_utc=datetime.now(timezone.utc).isoformat(),
        )
        self._append_event(event)
        return event

    def run_step(
        self,
        step_name: str,
        func: Callable[[], Dict[str, Any]],
        input_artifact_ref: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Execute a pipeline step with automatic STARTED/SUCCESS/FAILED audit logging."""
        logger.info(
            "[%s] >>> STARTING step '%s' (input_ref=%s)",
            self.pipeline_run_id,
            step_name,
            input_artifact_ref,
        )
        self.record_event(
            step_name=step_name,
            step_status="STARTED",
            input_artifact_ref=input_artifact_ref,
        )
        start_t = time.perf_counter()
        try:
            result = func() or {}
            elapsed = time.perf_counter() - start_t
            rows_proc = int(result.get("rows_processed", 0))
            rows_quar = int(result.get("rows_quarantined", 0))
            artifact_ref = result.get("input_artifact_ref", input_artifact_ref)

            self.record_event(
                step_name=step_name,
                step_status="SUCCESS",
                input_artifact_ref=artifact_ref,
                rows_processed=rows_proc,
                rows_quarantined=rows_quar,
                duration_seconds=elapsed,
            )
            logger.info(
                "[%s] <<< SUCCESS step '%s' in %.2fs (rows_processed=%d, rows_quarantined=%d)",
                self.pipeline_run_id,
                step_name,
                elapsed,
                rows_proc,
                rows_quar,
            )
            return result
        except Exception as exc:
            elapsed = time.perf_counter() - start_t
            tb_str = traceback.format_exc(limit=5)
            err_msg = f"{exc}\n{tb_str}"
            self.record_event(
                step_name=step_name,
                step_status="FAILED",
                input_artifact_ref=input_artifact_ref,
                duration_seconds=elapsed,
                error_type=type(exc).__name__,
                error_message=err_msg,
            )
            logger.error(
                "[%s] !!! FAILED step '%s' after %.2fs: %s: %s",
                self.pipeline_run_id,
                step_name,
                elapsed,
                type(exc).__name__,
                exc,
            )
            raise

    def get_run_history(self, run_id: Optional[str] = None) -> List[Dict[str, Any]]:
        """Read all logged step events for a given run_id from the local JSONL audit file."""
        target_id = run_id or self.pipeline_run_id
        if not self.audit_file.exists():
            return []
        events: List[Dict[str, Any]] = []
        with self.audit_file.open("r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                data = json.loads(line)
                if data.get("pipeline_run_id") == target_id:
                    events.append(data)
        return events

    def get_succeeded_steps_and_context(self, run_id: str) -> tuple[set[str], Dict[str, str]]:
        """Return set of succeeded step names and their saved input_artifact_ref pointers."""
        history = self.get_run_history(run_id)
        succeeded: set[str] = set()
        refs: Dict[str, str] = {}
        for event in history:
            if event.get("step_status") == "SUCCESS":
                step_name = event["step_name"]
                succeeded.add(step_name)
                if event.get("input_artifact_ref"):
                    refs[step_name] = event["input_artifact_ref"]
        return succeeded, refs

    def flush_to_delta(self, spark: Optional[Any] = None) -> int:
        """Sync in-memory step audit events to the ops.pipeline_step_execution_log Delta table."""
        if not self._events:
            return 0

        schema = pa.schema(
            [
                pa.field("step_log_id", pa.string(), nullable=False),
                pa.field("pipeline_run_id", pa.string(), nullable=False),
                pa.field("step_sequence_order", pa.int32(), nullable=False),
                pa.field("step_name", pa.string(), nullable=False),
                pa.field("step_status", pa.string(), nullable=False),
                pa.field("input_artifact_ref", pa.string(), nullable=True),
                pa.field("rows_processed", pa.int64(), nullable=False),
                pa.field("rows_quarantined", pa.int64(), nullable=False),
                pa.field("duration_seconds", pa.float64(), nullable=False),
                pa.field("error_type", pa.string(), nullable=True),
                pa.field("error_message", pa.string(), nullable=True),
                pa.field("logged_at_utc", pa.timestamp("us", tz="UTC"), nullable=False),
            ]
        )

        rows = [asdict(e) for e in self._events]
        df = pd.DataFrame(rows)
        df["logged_at_utc"] = pd.to_datetime(df["logged_at_utc"], utc=True)

        target_path = self.settings.ops_path / "pipeline_step_execution_log"
        target_path.mkdir(parents=True, exist_ok=True)
        table = pa.Table.from_pandas(df, schema=schema, preserve_index=False)
        write_deltalake(str(target_path), table, mode="append")

        count = len(self._events)
        self._events.clear()
        return count
