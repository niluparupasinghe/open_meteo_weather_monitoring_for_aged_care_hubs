"""Streamlit Dashboard for inspecting Gold Marts, Silver SCD2/Quarantine, Bronze, and Ops Ledger."""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Optional

# Ensure repository root is on sys.path when invoked via `streamlit run src/dashboard/app.py`
_PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

import time

import duckdb
import pandas as pd
import plotly.express as px
import streamlit as st
from deltalake import DeltaTable as RustDeltaTable

from src.config.settings import PipelineSettings, get_settings

st.set_page_config(
    page_title="Loop IQ | Aged Care Weather & Thermal Risk Lakehouse",
    page_icon="🏥",
    layout="wide",
)


def read_delta_df(path: Path) -> Optional[pd.DataFrame]:
    """Read a Delta Lake table into a Pandas DataFrame via delta-rs."""
    if not (path / "_delta_log").exists():
        return None
    return RustDeltaTable(str(path)).to_pandas()


def get_duckdb_connection(cfg: PipelineSettings) -> tuple[duckdb.DuckDBPyConnection, dict[str, int]]:
    """Create a DuckDB connection with all Medallion Lakehouse Delta tables registered under schemas & short aliases."""
    con = duckdb.connect()
    for schema_name in ("ops", "bronze", "silver", "gold"):
        con.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")

    table_defs = {
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

    registered_counts: dict[str, int] = {}
    for qualified_name, (short_alias, path) in table_defs.items():
        if (Path(path) / "_delta_log").exists():
            arrow_tbl = RustDeltaTable(str(path)).to_pyarrow_table()
            con.register(short_alias, arrow_tbl)
            con.execute(f"CREATE OR REPLACE VIEW {qualified_name} AS SELECT * FROM {short_alias}")
            registered_counts[qualified_name] = arrow_tbl.num_rows

    return con, registered_counts


def main() -> None:
    cfg = get_settings()

    st.title("🏥 Loop IQ — Aged Care Environmental & Thermal Risk Lakehouse")
    st.caption(
        "Medallion Lakehouse Architecture (Landing → 3 Bronze Tables → Silver SCD2/3-Guard MERGE/Quarantine → Gold Marts)"
    )

    tab_gold, tab_silver, tab_ops, tab_sql = st.tabs(
        [
            "🥇 Gold Layer: Facility Risk Alerts & Weather Summary",
            "🥈 Silver Layer: SCD2 Locations, Facts & DQ Quarantine",
            "⚙️ Ops & Bronze: API Call Weight Ledger & Step Audit",
            "💻 Interactive DuckDB SQL Workbench",
        ]
    )

    # ==========================================================================
    # TAB 1: GOLD LAYER
    # ==========================================================================
    with tab_gold:
        alerts_df = read_delta_df(cfg.gold_path / "mart_care_facility_risk_alerts")
        summary_df = read_delta_df(cfg.gold_path / "mart_daily_weather_summary")

        if alerts_df is None or summary_df is None:
            st.warning(
                "Gold tables have not been materialized yet. Run `docker compose run --rm pipeline python -m src.cli run-all` first."
            )
        else:
            alerts_df["observation_local_date"] = pd.to_datetime(
                alerts_df["observation_local_date"]
            ).dt.date
            summary_df["observation_local_date"] = pd.to_datetime(
                summary_df["observation_local_date"]
            ).dt.date

            c1, c2, c3, c4 = st.columns(4)
            c1.metric("Monitored Care Hubs", alerts_df["location_id"].nunique())
            c2.metric(
                "Critical / Warning Facility-Days",
                int(alerts_df["overall_risk_severity"].isin(["CRITICAL", "WARNING"]).sum()),
            )
            c3.metric(
                "Heatwave Risk Flags",
                int(alerts_df["is_heatwave_risk"].sum()),
            )
            c4.metric(
                "High UV / Storm Flags",
                int(
                    (alerts_df["is_high_uv_risk"] | alerts_df["is_severe_storm_risk"]).sum()
                ),
            )

            st.subheader("🚨 Facility Environmental Risk Alerts (`gold.mart_care_facility_risk_alerts`)")
            col_f1, col_f2 = st.columns(2)
            with col_f1:
                selected_hubs = st.multiselect(
                    "Filter by Care Hub",
                    options=sorted(alerts_df["location_name"].unique()),
                    default=sorted(alerts_df["location_name"].unique()),
                )
            with col_f2:
                selected_horizons = st.multiselect(
                    "Filter by Data Horizon (Facility Local Date)",
                    options=["PAST_MODELLED", "TODAY_MIXED", "FORWARD_FORECAST"],
                    default=["TODAY_MIXED", "FORWARD_FORECAST", "PAST_MODELLED"],
                )

            filtered_alerts = alerts_df[
                alerts_df["location_name"].isin(selected_hubs)
                & alerts_df["data_horizon_type"].isin(selected_horizons)
            ].sort_values(["observation_local_date", "location_id"], ascending=[False, True])

            st.dataframe(
                filtered_alerts[
                    [
                        "location_id",
                        "location_name",
                        "state_code",
                        "observation_local_date",
                        "data_horizon_type",
                        "overall_risk_severity",
                        "primary_hazard_reason",
                        "recommended_care_action",
                    ]
                ],
                use_container_width=True,
                hide_index=True,
            )

            st.subheader("📈 14-Day Temperature & 7-Day Rolling Baseline (`gold.mart_daily_weather_summary`)")
            filtered_summary = summary_df[
                summary_df["location_name"].isin(selected_hubs)
            ].sort_values("observation_local_date")

            fig = px.line(
                filtered_summary,
                x="observation_local_date",
                y=["temp_max_c", "hourly_avg_temp_c", "rolling_7d_avg_temp_c", "temp_min_c"],
                facet_col="location_name",
                facet_col_wrap=4,
                title="Daily Max / Mean / 7d Rolling / Min Temperature (°C) by Aged Care Hub",
                labels={"value": "Temperature (°C)", "observation_local_date": "Local Date"},
            )
            st.plotly_chart(fig, use_container_width=True)

    # ==========================================================================
    # TAB 2: SILVER LAYER
    # ==========================================================================
    with tab_silver:
        st.subheader("📍 Monitored Locations — SCD Type 2 (`silver.dim_location_scd2`)")
        dim_df = read_delta_df(cfg.silver_path / "dim_location_scd2")
        if dim_df is not None:
            st.dataframe(dim_df, use_container_width=True, hide_index=True)
        else:
            st.info("`silver.dim_location_scd2` not yet created.")

        st.subheader("🛡️ Data Quality Quarantine Table (`silver.quarantine_weather_records`)")
        quar_df = read_delta_df(cfg.silver_path / "quarantine_weather_records")
        if quar_df is not None and len(quar_df) > 0:
            st.warning(f"{len(quar_df)} quarantined record(s) captured by Stage 1 / Stage 2 DQ checks:")
            st.dataframe(quar_df, use_container_width=True, hide_index=True)
        else:
            st.success("Zero records currently in quarantine — all ingested API records passed the 13 DQ checks!")

        col_s1, col_s2, col_s3 = st.columns(3)
        m15_df = read_delta_df(cfg.silver_path / "fact_weather_minutely_15")
        hr_df = read_delta_df(cfg.silver_path / "fact_weather_hourly")
        dy_df = read_delta_df(cfg.silver_path / "fact_weather_daily")
        col_s1.metric(
            "silver.fact_weather_minutely_15 rows",
            f"{len(m15_df):,}" if m15_df is not None else "0",
        )
        col_s2.metric(
            "silver.fact_weather_hourly rows",
            f"{len(hr_df):,}" if hr_df is not None else "0",
        )
        col_s3.metric(
            "silver.fact_weather_daily rows",
            f"{len(dy_df):,}" if dy_df is not None else "0",
        )

    # ==========================================================================
    # TAB 3: OPS & BRONZE LAYER
    # ==========================================================================
    with tab_ops:
        st.subheader("⚖️ Post-Consumption API Call Weight Ledger (`ops.api_call_weight_ledger`)")
        ledger_df = read_delta_df(cfg.ops_path / "api_call_weight_ledger")
        if ledger_df is not None:
            st.dataframe(
                ledger_df.sort_values("called_at_utc", ascending=False),
                use_container_width=True,
                hide_index=True,
            )
        else:
            st.info("No API calls logged yet.")

        st.subheader("📋 Step-by-Step Execution & Repair Log (`ops.pipeline_step_execution_log`)")
        step_df = read_delta_df(cfg.ops_path / "pipeline_step_execution_log")
        if step_df is not None:
            st.dataframe(
                step_df.sort_values(["logged_at_utc", "step_sequence_order"], ascending=[False, True]),
                use_container_width=True,
                hide_index=True,
            )

    # ==========================================================================
    # TAB 4: INTERACTIVE DUCKDB SQL WORKBENCH
    # ==========================================================================
    with tab_sql:
        st.subheader("💻 Interactive DuckDB SQL Query Workbench")
        st.markdown(
            "Run standard DuckDB SQL queries directly against all Medallion Lakehouse Delta tables (`ops.*`, `bronze.*`, `silver.*`, `gold.*`)."
        )

        con, registered_counts = get_duckdb_connection(cfg)

        with st.expander("📚 Available Medallion Lakehouse Tables & Row Counts", expanded=False):
            if registered_counts:
                catalog_df = pd.DataFrame(
                    [
                        {"Schema.Table": k, "Short Alias": k.split(".")[1], "Row Count": f"{v:,}"}
                        for k, v in registered_counts.items()
                    ]
                )
                st.dataframe(catalog_df, use_container_width=True, hide_index=True)
            else:
                st.info("No Delta tables found yet. Run the pipeline first.")

        preset_queries = {
            "1. Critical & Warning Aged Care Facility Alerts (Gold)": (
                "SELECT location_id, location_name, state_code, climate_risk_tier,\n"
                "       observation_local_date, data_horizon_type, overall_risk_severity,\n"
                "       primary_hazard_reason, recommended_care_action\n"
                "FROM gold.mart_care_facility_risk_alerts\n"
                "WHERE overall_risk_severity IN ('CRITICAL', 'WARNING')\n"
                "ORDER BY observation_local_date DESC, location_id;"
            ),
            "2. Daily Weather Summary vs 7-Day Rolling Baseline (Gold)": (
                "SELECT location_id, location_name, observation_local_date, data_horizon_type,\n"
                "       temp_min_c, temp_max_c, hourly_avg_temp_c, rolling_7d_avg_temp_c,\n"
                "       ROUND(hourly_avg_temp_c - rolling_7d_avg_temp_c, 2) AS temp_anomaly_c,\n"
                "       dominant_weather_description\n"
                "FROM gold.mart_daily_weather_summary\n"
                "ORDER BY location_id, observation_local_date;"
            ),
            "3. Monitored Care Hubs SCD Type 2 Dimension (Silver)": (
                "SELECT location_sk, location_id, location_name, state_code, timezone_name,\n"
                "       climate_risk_tier, requested_lat, requested_lon, snapped_grid_lat, snapped_grid_lon,\n"
                "       grid_snap_distance_km, effective_from_utc, effective_to_utc, is_current\n"
                "FROM silver.dim_location_scd2\n"
                "ORDER BY location_id;"
            ),
            "4. Hourly Peak Apparent Temperature by Facility (Silver Join)": (
                "SELECT d.location_name, d.climate_risk_tier,\n"
                "       h.observation_local_date,\n"
                "       MAX(h.temperature_2m_c) AS max_hourly_temp_c,\n"
                "       MAX(h.apparent_temperature_c) AS max_feels_like_c,\n"
                "       MAX(h.wind_gusts_10m_kmh) AS max_gust_kmh\n"
                "FROM silver.fact_weather_hourly h\n"
                "JOIN silver.dim_location_scd2 d\n"
                "  ON h.location_id = d.location_id AND d.is_current = TRUE\n"
                "GROUP BY 1, 2, 3\n"
                "ORDER BY max_feels_like_c DESC\n"
                "LIMIT 25;"
            ),
            "5. Post-Consumption API Call Weight Ledger (Ops)": (
                "SELECT call_id, pipeline_run_id, http_status_code, is_quota_consumed,\n"
                "       num_locations, num_days, num_variables, consumed_call_weight,\n"
                "       rolling_24h_weight, daily_quota_remaining_pct, response_latency_ms, called_at_utc\n"
                "FROM ops.api_call_weight_ledger\n"
                "ORDER BY called_at_utc DESC;"
            ),
            "6. Pipeline Step Execution & Repair Audit Log (Ops)": (
                "SELECT pipeline_run_id, step_sequence_order, step_name, step_status,\n"
                "       rows_processed, rows_quarantined, execution_duration_sec, logged_at_utc\n"
                "FROM ops.pipeline_step_execution_log\n"
                "ORDER BY logged_at_utc DESC, step_sequence_order;"
            ),
        }

        selected_preset = st.selectbox(
            "Load a Sample SQL Query Template (or edit your own query below):",
            options=list(preset_queries.keys()),
        )

        sql_input = st.text_area(
            "SQL Query Editor (DuckDB SQL):",
            value=preset_queries[selected_preset],
            height=180,
        )

        run_btn = st.button("▶️ Execute SQL Query", type="primary")
        if run_btn or sql_input:
            try:
                t0 = time.perf_counter()
                result_df = con.execute(sql_input).df()
                elapsed_ms = (time.perf_counter() - t0) * 1000.0
                st.success(f"Query returned **{len(result_df):,} row(s)** in **{elapsed_ms:.1f} ms**.")
                st.dataframe(result_df, use_container_width=True, hide_index=True)
                st.download_button(
                    label="⬇️ Download Query Result as CSV",
                    data=result_df.to_csv(index=False).encode("utf-8"),
                    file_name="duckdb_query_result.csv",
                    mime="text/csv",
                )
            except Exception as exc:
                st.error(f"SQL Execution Error: {exc}")


if __name__ == "__main__":
    main()
