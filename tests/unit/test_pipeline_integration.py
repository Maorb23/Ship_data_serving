import logging
from pathlib import Path

import duckdb
import yaml

from pipeline.pipe import run_pipe

LOGGER = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parents[2]
FULL_SOURCE_DB = PROJECT_ROOT / "data" / "raw" / "ship_positions.db"
INCREMENTAL_SOURCE_DB = PROJECT_ROOT / "data" / "raw" / "ship_positions_incremental.db"
SQL_DIR = PROJECT_ROOT / "elt_utils" / "sql"


def _write_integration_config(config_path: Path, target_db: Path) -> None:
    config = {
        "pipeline": {
            "source_db": str(FULL_SOURCE_DB),
            "target_db": str(target_db),
            "source_schema": "raw",
            "source_table": "ship_positions",
            "target_schema": "silver",
            "target_table": "SHIP_POSITIONS",
            "rejects_table": "SHIP_POSITIONS_REJECTS",
            "sql_dir": str(SQL_DIR),
        },
        "dqa": {
            "lat_min": -90.0,
            "lat_max": 90.0,
            "lon_min": -180.0,
            "lon_max": 180.0,
            "sog_min": 0.0,
            "sog_max": 100.0,
            "max_sog_diff": 40.0,
            "cog_min": 0.0,
            "cog_max": 360.0,
        },
        "queries": {
            "create_target_table": "create_target_table.sql",
            "create_rejects_table": "create_rejects_table.sql",
            "extract_delta": "extract_delta.sql",
            "fetch_target_lookback": "fetch_target_lookback.sql",
            "insert_rejects": "insert_rejects.sql",
            "insert_target_rows": "insert_target_rows.sql",
        },
    }
    config_path.write_text(yaml.safe_dump(config, sort_keys=False), encoding="utf-8")


def test_pipeline_full_incremental_idempotent_with_logging(tmp_path, caplog):
    assert FULL_SOURCE_DB.exists(), "Missing full source DB required for integration test"
    assert INCREMENTAL_SOURCE_DB.exists(), "Missing incremental source DB required for integration test"

    target_db = tmp_path / "silver_ship_positions.db"
    config_path = tmp_path / "integration_config.yaml"
    _write_integration_config(config_path, target_db)

    caplog.set_level(logging.INFO)

    summary_full = run_pipe(config_path=str(config_path), source_db_override=str(FULL_SOURCE_DB))
    LOGGER.info("Full run summary: %s", summary_full)

    summary_incremental = run_pipe(
        config_path=str(config_path),
        source_db_override=str(INCREMENTAL_SOURCE_DB),
    )
    LOGGER.info("Incremental run summary: %s", summary_incremental)

    summary_incremental_rerun = run_pipe(
        config_path=str(config_path),
        source_db_override=str(INCREMENTAL_SOURCE_DB),
    )
    LOGGER.info("Incremental rerun summary: %s", summary_incremental_rerun)

    assert summary_full["published_rows"] > 0
    assert summary_incremental["published_rows"] > 0
    assert summary_incremental_rerun["published_rows"] == 0

    con = duckdb.connect(str(target_db))
    try:
        total_published = con.execute("SELECT COUNT(*) FROM silver.SHIP_POSITIONS").fetchone()[0]
        duplicate_keys = con.execute(
            """
            SELECT COUNT(*)
            FROM (
                SELECT ship_name, time_ts, COUNT(*) AS cnt
                FROM silver.SHIP_POSITIONS
                GROUP BY 1, 2
                HAVING COUNT(*) > 1
            )
            """
        ).fetchone()[0]
        total_rejects = con.execute(
            "SELECT COUNT(*) FROM silver.SHIP_POSITIONS_REJECTS"
        ).fetchone()[0]
    finally:
        con.close()

    assert total_published == summary_full["published_rows"] + summary_incremental["published_rows"]
    assert duplicate_keys == 0
    assert total_rejects >= summary_full["rejected_rows"] + summary_incremental["rejected_rows"]

    logs_text = caplog.text
    assert "Pipeline starting" in logs_text
    assert "Delta extracted" in logs_text
    assert "DQA complete" in logs_text
    assert "Publish complete" in logs_text
    assert logs_text.count("Pipeline completed successfully") == 3
