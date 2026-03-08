"""
Publish class responsible for writing processed delta records to the target tables.
"""

from __future__ import annotations

import pandas as pd
from typing import Any


class Publish:
    """
    Responsible for writing processed delta records to the target tables.
    """

    @staticmethod
    def initialize_tables(db_ops, configs: dict[str, Any]) -> None:
        db_ops.execute_query_file(
            configs["queries"]["create_target_table"],
            fetch=False,
        )
        db_ops.execute_query_file(
            configs["queries"]["create_rejects_table"],
            fetch=False,
        )

    @staticmethod
    def publish(
        db_ops,
        publish_df: pd.DataFrame,
        rejects_df: pd.DataFrame,
        configs: dict[str, Any],
    ) -> dict[str, int]:
        """Write qualifying records from the delta to target tables."""
        inserted_rows = 0
        rejected_rows = 0

        db_ops.begin()
        try:
            if rejects_df is not None and not rejects_df.empty:
                db_ops.register_df("rejects_df", rejects_df)
                db_ops.execute_query_file(
                    configs["queries"]["insert_rejects"],
                    fetch=False,
                )
                db_ops.unregister("rejects_df")
                rejected_rows = len(rejects_df)

            if publish_df is not None and not publish_df.empty:
                db_ops.register_df("publish_df", publish_df) # Register the DataFrame as a temporary table for SQL access
                db_ops.execute_query_file(
                    configs["queries"]["insert_target_rows"],
                    fetch=False,
                ) # Execute the insert query which references the temporary table
                db_ops.unregister("publish_df") # Unregister the temporary table after use
                inserted_rows = len(publish_df) # We can count the number of rows we attempted to insert.

            db_ops.commit() # Commit the transaction if all operations succeeded
        except Exception as e:
            print("Publish step failed:", e)
            db_ops.rollback()
            db_ops.unregister("publish_df")
            db_ops.unregister("rejects_df")
            raise

        return {
            "inserted_rows": inserted_rows,
            "rejected_rows": rejected_rows,
        }
