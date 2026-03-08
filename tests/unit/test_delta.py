from copy import deepcopy
from pathlib import Path

import duckdb

from elt_utils.db.db_ops import DBops
from elt_utils.transform.config import Config
from elt_utils.transform.delta import Delta
from elt_utils.transform.publish import Publish


PROJECT_ROOT = Path(__file__).resolve().parents[2]
CONFIG_PATH = PROJECT_ROOT / "src" / "config" / "config.yaml"


def test_delta_applies_dedup_then_anti_join(tmp_path):
    source_db = tmp_path / "source.db"
    target_db = tmp_path / "target.db"

    source_con = duckdb.connect(str(source_db))
    source_con.execute("CREATE SCHEMA raw;")
    source_con.execute(
        """
        CREATE TABLE raw.ship_positions (
            ship_name VARCHAR,
            time DOUBLE,
            lat DOUBLE,
            lon DOUBLE,
            sog DOUBLE,
            cog DOUBLE
        );
        """
    )
    source_con.executemany(
        "INSERT INTO raw.ship_positions VALUES (?, ?, ?, ?, ?, ?);",
        [
            ("Ship_A", 1.0, 0.0, 0.0, 10.0, 10.0),
            ("Ship_A", 1.0, 0.0, 0.0, 900.0, 10.0),
            ("Ship_A", 2.0, 0.0, 0.1, 11.0, 20.0),
            ("Ship_A", 2.0, 0.0, 0.1, 600.0, 20.0),
        ],
    )
    source_con.close()

    configs = deepcopy(Config(str(CONFIG_PATH)).configs)
    configs["pipeline"]["source_db"] = str(source_db)
    configs["pipeline"]["target_db"] = str(target_db)

    db_ops = DBops(
        db_path=configs["pipeline"]["source_db"],
        sql_dir=configs["pipeline"]["sql_dir"],
        target_db_path=configs["pipeline"]["target_db"],
    )

    try:
        Publish.initialize_tables(db_ops, configs)
        db_ops.execute_sql(
            """
            INSERT INTO target_db.silver.SHIP_POSITIONS (
                ship_name, time_ts, lat, lon, sog, cog, rot, acceleration, distance_traveled, elt_published_at
            ) VALUES (
                'Ship_A', CAST(to_timestamp(1.0) AS TIMESTAMP), 0.0, 0.0, 10.0, 10.0,
                NULL, NULL, NULL, CURRENT_TIMESTAMP
            );
            """,
            fetch=False,
        )

        delta_df = Delta.delta(db_ops, configs)
    finally:
        db_ops.close()

    assert len(delta_df) == 1
    row = delta_df.iloc[0]
    assert row["ship_name"] == "Ship_A"
    assert row["time"] == 2.0
