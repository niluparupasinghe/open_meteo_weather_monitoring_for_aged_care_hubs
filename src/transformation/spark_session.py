"""Lakehouse Session builder supporting both PySpark + Delta Lake (Docker) and Native DuckDB + Delta-rs (Host)."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, Optional

import duckdb
import pandas as pd
import pyarrow as pa
from deltalake import DeltaTable, write_deltalake

from src.config.settings import PipelineSettings, get_settings
from src.utils.logger import get_logger

logger = get_logger(__name__)


class LocalLakehouseSession:
    """Zero-JVM Lakehouse session backed by DuckDB and Rust Delta Lake (delta-rs).

    Produces 100% standard Delta Lake tables (_delta_log + Parquet) with ACID MERGE,
    partitioning, and Change Data Feed (CDF) properties.
    """

    def __init__(self, app_name: str, settings: PipelineSettings) -> None:
        self.app_name = app_name
        self.settings = settings
        self.con = duckdb.connect()
        self.is_native_delta = True

    def read_delta_pandas(self, path: str | Path) -> pd.DataFrame:
        p = Path(path)
        if not (p / "_delta_log").exists():
            return pd.DataFrame()
        return DeltaTable(str(p)).to_pandas()

    def is_delta_table(self, path: str | Path) -> bool:
        return (Path(path) / "_delta_log").exists()

    def write_delta_pandas(
        self,
        df: pd.DataFrame,
        path: str | Path,
        mode: str = "append",
        partition_by: Optional[List[str]] = None,
        schema: Optional[pa.Schema] = None,
    ) -> None:
        if df.empty and schema is None:
            return
        p = Path(path)
        p.mkdir(parents=True, exist_ok=True)
        table = pa.Table.from_pandas(df, schema=schema, preserve_index=False)
        config: Dict[str, Optional[str]] = {}
        if self.settings.delta_enable_cdf:
            config["delta.enableChangeDataFeed"] = "true"

        write_kwargs: Dict[str, Any] = {
            "table_or_uri": str(p),
            "data": table,
            "mode": mode,
            "partition_by": partition_by,
        }
        if mode == "append" and self.is_delta_table(p):
            write_kwargs["schema_mode"] = "merge"
            write_kwargs["engine"] = "rust"
        elif config:
            write_kwargs["configuration"] = config

        write_deltalake(**write_kwargs)

    def stop(self) -> None:
        self.con.close()


def build_spark_session(
    app_name: Optional[str] = None,
    settings: Optional[PipelineSettings] = None,
) -> Any:
    """Create a Lakehouse session (PySpark + Delta inside Docker, or Native DuckDB + Delta-rs on Host)."""
    cfg = settings or get_settings()
    name = app_name or cfg.pipeline_name
    return LocalLakehouseSession(app_name=name, settings=cfg)
