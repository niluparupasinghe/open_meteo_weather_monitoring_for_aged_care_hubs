"""CLI Entrypoint for the Loop IQ Open-Meteo Medallion Lakehouse Pipeline."""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Any, Dict, Optional

from src.config.settings import PipelineSettings, get_settings
from src.ingestion.bronze_writer import BronzeWriter
from src.ingestion.call_weight_ledger import CallWeightLedger
from src.ingestion.open_meteo_client import OpenMeteoClient
from src.transformation.gold_transformer import GoldTransformer
from src.transformation.silver_transformer import SilverTransformer
from src.transformation.spark_session import build_spark_session
from src.utils.email_notifier import send_pipeline_run_email
from src.utils.logger import get_logger
from src.utils.step_tracker import StepTracker

logger = get_logger(__name__)


def execute_pipeline(
    settings: Optional[PipelineSettings] = None,
    resume_run_id: Optional[str] = None,
    spark_override: Any = None,
    client_override: Optional[OpenMeteoClient] = None,
    fail_at_step: Optional[str] = None,
) -> Dict[str, Any]:
    """Execute the 7-step Medallion pipeline or repair a previously failed run_id."""
    cfg = settings or get_settings()
    tracker = StepTracker(pipeline_run_id=resume_run_id, settings=cfg)
    run_id = tracker.pipeline_run_id

    succeeded_steps: set[str] = set()
    saved_refs: Dict[str, str] = {}
    if resume_run_id:
        succeeded_steps, saved_refs = tracker.get_succeeded_steps_and_context(resume_run_id)
        logger.info(
            "Resuming repair run for pipeline_run_id=%s (already succeeded steps=%s)",
            run_id,
            sorted(list(succeeded_steps)),
        )

    ledger = CallWeightLedger(cfg)
    client = client_override or OpenMeteoClient(settings=cfg, ledger=ledger)
    spark = spark_override or build_spark_session(settings=cfg)

    bronze_writer = BronzeWriter(spark=spark, settings=cfg)
    silver_transformer = SilverTransformer(spark=spark, settings=cfg)
    gold_transformer = GoldTransformer(spark=spark, settings=cfg)

    landing_file_path: Optional[str] = saved_refs.get("step_1_ingest_to_landing")

    try:
        # ------------------------------------------------------------------
        # Step 1: Ingest from Open-Meteo API to Landing Zone (+ Call Weight Ledger)
        # ------------------------------------------------------------------
        step1_name = "step_1_ingest_to_landing"
        if step1_name in succeeded_steps and landing_file_path:
            tracker.record_event(
                step_name=step1_name,
                step_status="SKIPPED_ON_REPAIR",
                input_artifact_ref=landing_file_path,
            )
            logger.info("Skipping %s (reusing %s)", step1_name, landing_file_path)
        else:
            def _step1() -> Dict[str, Any]:
                if fail_at_step == step1_name:
                    raise RuntimeError(f"Simulated failure at {step1_name}")
                locations = cfg.load_locations()
                res = client.fetch_and_land_batch(locations=locations, pipeline_run_id=run_id)
                ledger.flush_to_delta(spark)
                return {
                    "rows_processed": len(locations),
                    "rows_quarantined": 0,
                    "input_artifact_ref": res["landing_file_path"],
                }

            s1_out = tracker.run_step(step1_name, _step1, input_artifact_ref="open_meteo_api")
            landing_file_path = s1_out["input_artifact_ref"]

        assert landing_file_path is not None, "landing_file_path must be set after Step 1"

        # ------------------------------------------------------------------
        # Step 2: Landing Zone -> bronze.weather_minutely_15
        # ------------------------------------------------------------------
        step2_name = "step_2_landing_to_bronze_minutely_15"
        if step2_name in succeeded_steps:
            tracker.record_event(
                step_name=step2_name,
                step_status="SKIPPED_ON_REPAIR",
                input_artifact_ref=landing_file_path,
            )
        else:
            def _step2() -> Dict[str, Any]:
                if fail_at_step == step2_name:
                    raise RuntimeError(f"Simulated failure at {step2_name}")
                cnt = bronze_writer.write_minutely_15(
                    landing_file_path=landing_file_path,
                    pipeline_run_id=run_id,
                )
                return {"rows_processed": cnt, "input_artifact_ref": landing_file_path}

            tracker.run_step(step2_name, _step2, input_artifact_ref=landing_file_path)

        # ------------------------------------------------------------------
        # Step 3: Landing Zone -> bronze.weather_hourly
        # ------------------------------------------------------------------
        step3_name = "step_3_landing_to_bronze_hourly"
        if step3_name in succeeded_steps:
            tracker.record_event(
                step_name=step3_name,
                step_status="SKIPPED_ON_REPAIR",
                input_artifact_ref=landing_file_path,
            )
        else:
            def _step3() -> Dict[str, Any]:
                if fail_at_step == step3_name:
                    raise RuntimeError(f"Simulated failure at {step3_name}")
                cnt = bronze_writer.write_hourly(
                    landing_file_path=landing_file_path,
                    pipeline_run_id=run_id,
                )
                return {"rows_processed": cnt, "input_artifact_ref": landing_file_path}

            tracker.run_step(step3_name, _step3, input_artifact_ref=landing_file_path)

        # ------------------------------------------------------------------
        # Step 4: Landing Zone -> bronze.weather_daily
        # ------------------------------------------------------------------
        step4_name = "step_4_landing_to_bronze_daily"
        if step4_name in succeeded_steps:
            tracker.record_event(
                step_name=step4_name,
                step_status="SKIPPED_ON_REPAIR",
                input_artifact_ref=landing_file_path,
            )
        else:
            def _step4() -> Dict[str, Any]:
                if fail_at_step == step4_name:
                    raise RuntimeError(f"Simulated failure at {step4_name}")
                cnt = bronze_writer.write_daily(
                    landing_file_path=landing_file_path,
                    pipeline_run_id=run_id,
                )
                return {"rows_processed": cnt, "input_artifact_ref": landing_file_path}

            tracker.run_step(step4_name, _step4, input_artifact_ref=landing_file_path)

        # ------------------------------------------------------------------
        # Step 5: Bronze -> silver.dim_location_scd2 (SCD Type 2)
        # ------------------------------------------------------------------
        step5_name = "step_5_bronze_to_silver_dim_location"
        if step5_name in succeeded_steps:
            tracker.record_event(
                step_name=step5_name,
                step_status="SKIPPED_ON_REPAIR",
                input_artifact_ref=run_id,
            )
        else:
            def _step5() -> Dict[str, Any]:
                if fail_at_step == step5_name:
                    raise RuntimeError(f"Simulated failure at {step5_name}")
                return silver_transformer.transform_dim_location_scd2(pipeline_run_id=run_id)

            tracker.run_step(step5_name, _step5, input_artifact_ref=run_id)

        # ------------------------------------------------------------------
        # Step 6: Bronze -> Silver Fact Tables + Quarantine Table
        # ------------------------------------------------------------------
        step6_name = "step_6_bronze_to_silver_facts"
        if step6_name in succeeded_steps:
            tracker.record_event(
                step_name=step6_name,
                step_status="SKIPPED_ON_REPAIR",
                input_artifact_ref=run_id,
            )
        else:
            def _step6() -> Dict[str, Any]:
                if fail_at_step == step6_name:
                    raise RuntimeError(f"Simulated failure at {step6_name}")
                return silver_transformer.transform_all_facts(pipeline_run_id=run_id)

            tracker.run_step(step6_name, _step6, input_artifact_ref=run_id)

        # ------------------------------------------------------------------
        # Step 7: Silver -> Gold Analytical & Facility Risk Alert Marts
        # ------------------------------------------------------------------
        step7_name = "step_7_silver_to_gold_marts"
        if step7_name in succeeded_steps:
            tracker.record_event(
                step_name=step7_name,
                step_status="SKIPPED_ON_REPAIR",
                input_artifact_ref=run_id,
            )
        else:
            def _step7() -> Dict[str, Any]:
                if fail_at_step == step7_name:
                    raise RuntimeError(f"Simulated failure at {step7_name}")
                return gold_transformer.build_all_marts()

            tracker.run_step(step7_name, _step7, input_artifact_ref=run_id)

    finally:
        # Always flush step audit events to Delta and dispatch 7-step email report (SUCCESS or FAILED)
        try:
            ledger.flush_to_delta(spark)
            tracker.flush_to_delta(spark)
            send_pipeline_run_email(tracker=tracker, ledger=ledger, settings=cfg)
        except Exception as flush_err:
            logger.warning("Could not flush ops events or email notification: %s", flush_err)

    return {
        "pipeline_run_id": run_id,
        "landing_file_path": landing_file_path,
        "status": "SUCCESS",
    }


def verify_lakehouse(settings: Optional[PipelineSettings] = None) -> None:
    """Inspect all 10 Lakehouse Delta tables using DuckDB / Delta-rs (zero-JVM read)."""
    import duckdb
    from deltalake import DeltaTable as RustDeltaTable

    cfg = settings or get_settings()
    tables = [
        ("ops.api_call_weight_ledger", cfg.ops_path / "api_call_weight_ledger"),
        ("ops.pipeline_step_execution_log", cfg.ops_path / "pipeline_step_execution_log"),
        ("bronze.weather_minutely_15", cfg.bronze_path / "weather_minutely_15"),
        ("bronze.weather_hourly", cfg.bronze_path / "weather_hourly"),
        ("bronze.weather_daily", cfg.bronze_path / "weather_daily"),
        ("silver.dim_location_scd2", cfg.silver_path / "dim_location_scd2"),
        ("silver.fact_weather_minutely_15", cfg.silver_path / "fact_weather_minutely_15"),
        ("silver.fact_weather_hourly", cfg.silver_path / "fact_weather_hourly"),
        ("silver.fact_weather_daily", cfg.silver_path / "fact_weather_daily"),
        (
            "silver.quarantine_weather_records",
            cfg.silver_path / "quarantine_weather_records",
        ),
        ("gold.mart_daily_weather_summary", cfg.gold_path / "mart_daily_weather_summary"),
        (
            "gold.mart_care_facility_risk_alerts",
            cfg.gold_path / "mart_care_facility_risk_alerts",
        ),
    ]

    print("\n" + "=" * 85)
    print(" LOOP IQ OPEN-METEO MEDALLION LAKEHOUSE — VERIFICATION SUMMARY")
    print("=" * 85)
    print(f"{'Table Name':<42} | {'Status':<12} | {'Row Count':>10} | {'Delta Version':>13}")
    print("-" * 85)

    for name, path in tables:
        delta_log = path / "_delta_log"
        if delta_log.exists():
            dt = RustDeltaTable(str(path))
            df = dt.to_pandas()
            print(f"{name:<42} | {'INITIALIZED':<12} | {len(df):>10,d} | {dt.version():>13d}")
        else:
            print(f"{name:<42} | {'EMPTY/NONE':<12} | {0:>10d} | {'-':>13}")
    print("=" * 85)

    # Sync all Delta tables into a persistent DuckDB database file (data/lakehouse/loopiq_lakehouse.duckdb)
    # so external SQL GUIs (DBeaver, DataGrip, VS Code SQLTools, duckdb -ui) can open it directly.
    duckdb_file = cfg.ops_path.parent / "loopiq_lakehouse.duckdb"
    duckdb_file.parent.mkdir(parents=True, exist_ok=True)
    db_con = duckdb.connect(str(duckdb_file))
    for schema_name in ("ops", "bronze", "silver", "gold"):
        db_con.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
    for name, path in tables:
        if (path / "_delta_log").exists():
            arrow_tbl = RustDeltaTable(str(path)).to_pyarrow_table()
            db_con.register("_tmp_arrow", arrow_tbl)
            db_con.execute(f"CREATE OR REPLACE TABLE {name} AS SELECT * FROM _tmp_arrow")
            db_con.unregister("_tmp_arrow")
    db_con.close()
    print(f"Synced persistent DuckDB catalog: {duckdb_file}")

    # Display sample Gold Facility Risk Alerts via DuckDB
    alerts_path = cfg.gold_path / "mart_care_facility_risk_alerts"
    if (alerts_path / "_delta_log").exists():
        dt_alerts = RustDeltaTable(str(alerts_path)).to_pyarrow_table()
        con = duckdb.connect()
        con.register("alerts", dt_alerts)
        sample_df = con.execute(
            """
            SELECT
                location_id,
                location_name,
                observation_local_date,
                data_horizon_type,
                overall_risk_severity,
                primary_hazard_reason
            FROM alerts
            ORDER BY
                CASE overall_risk_severity
                    WHEN 'CRITICAL' THEN 1
                    WHEN 'WARNING' THEN 2
                    WHEN 'WATCH' THEN 3
                    ELSE 4
                END,
                observation_local_date DESC
            LIMIT 10
            """
        ).df()
        print("\nTop 10 Aged Care Facility Risk Evaluations (Gold Layer):")
        print(sample_df.to_string(index=False))
        print("=" * 85 + "\n")


def run_adhoc_query(sql_query: str, settings: Optional[PipelineSettings] = None) -> None:
    """Register all existing Lakehouse Delta tables in DuckDB and run an ad-hoc SQL query."""
    import duckdb
    from deltalake import DeltaTable as RustDeltaTable

    cfg = settings or get_settings()
    con = duckdb.connect()

    for schema_name in ("ops", "bronze", "silver", "gold"):
        con.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")

    schema_mapping = {
        "ops.api_call_weight_ledger": ("api_call_weight_ledger", cfg.ops_path / "api_call_weight_ledger"),
        "ops.pipeline_step_execution_log": ("pipeline_step_execution_log", cfg.ops_path / "pipeline_step_execution_log"),
        "bronze.weather_minutely_15": ("bronze_weather_minutely_15", cfg.bronze_path / "weather_minutely_15"),
        "bronze.weather_hourly": ("bronze_weather_hourly", cfg.bronze_path / "weather_hourly"),
        "bronze.weather_daily": ("bronze_weather_daily", cfg.bronze_path / "weather_daily"),
        "silver.dim_location_scd2": ("dim_location_scd2", cfg.silver_path / "dim_location_scd2"),
        "silver.fact_weather_minutely_15": ("fact_weather_minutely_15", cfg.silver_path / "fact_weather_minutely_15"),
        "silver.fact_weather_hourly": ("fact_weather_hourly", cfg.silver_path / "fact_weather_hourly"),
        "silver.fact_weather_daily": ("fact_weather_daily", cfg.silver_path / "fact_weather_daily"),
        "silver.quarantine_weather_records": ("quarantine_weather_records", cfg.silver_path / "quarantine_weather_records"),
        "gold.mart_daily_weather_summary": ("mart_daily_weather_summary", cfg.gold_path / "mart_daily_weather_summary"),
        "gold.mart_care_facility_risk_alerts": ("mart_care_facility_risk_alerts", cfg.gold_path / "mart_care_facility_risk_alerts"),
    }

    for qualified_name, (short_alias, path) in schema_mapping.items():
        if (Path(path) / "_delta_log").exists():
            arrow_tbl = RustDeltaTable(str(path)).to_pyarrow_table()
            con.register(short_alias, arrow_tbl)
            con.execute(f"CREATE OR REPLACE VIEW {qualified_name} AS SELECT * FROM {short_alias}")

    res = con.execute(sql_query).df()
    print(res.to_string(index=False))


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Loop IQ Open-Meteo Medallion Lakehouse CLI"
    )
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("run-all", help="Run all 7 Medallion pipeline steps end-to-end")

    repair_p = sub.add_parser(
        "repair",
        help="Repair a failed pipeline run from its last failed step without re-calling the API",
    )
    repair_p.add_argument("--run-id", required=True, help="Pipeline run ID to repair")

    sub.add_parser("verify", help="Verify all Lakehouse Delta tables and print summary stats")

    query_p = sub.add_parser("query", help="Execute a DuckDB SQL query against Lakehouse tables")
    query_p.add_argument("--sql", required=True, help="SQL query string")

    args = parser.parse_args()

    if args.command == "run-all":
        res = execute_pipeline()
        logger.info("Pipeline completed: %s", res)
        verify_lakehouse()
    elif args.command == "repair":
        res = execute_pipeline(resume_run_id=args.run_id)
        logger.info("Repair run completed: %s", res)
        verify_lakehouse()
    elif args.command == "verify":
        verify_lakehouse()
    elif args.command == "query":
        run_adhoc_query(args.sql)


if __name__ == "__main__":
    main()
