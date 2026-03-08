"""
Delta class responsible for extracting a new batch of unprocessed source records and 
fetching lookback data from the target table for comparison.
"""

from __future__ import annotations

import pandas as pd
from typing import Any


class Delta:
    """
    Responsible for extracting a new batch of unprocessed source records
    """

    @staticmethod
    def delta(db_ops, configs: dict[str, Any]) -> pd.DataFrame:
        """
        Extract the next batch of source records
        """
        query_file = configs["queries"]["extract_delta"]
        delta_df = db_ops.execute_query_file(
            query_file,
            as_df=True,
        )

        if delta_df.empty:
            return pd.DataFrame(columns=["ship_name", "time", "lat", "lon", "sog", "cog"])

        delta_df["time"] = pd.to_numeric(delta_df["time"], errors="coerce") # Time is numeric in the data
        return delta_df

    @staticmethod
    def fetch_target_lookback(db_ops, configs: dict[str, Any]) -> pd.DataFrame:
        """Fetch the lookback data from the target table. 
        This is used to compare against the delta data to determine which records are new/updated.
        """
        query_file = configs["queries"]["fetch_target_lookback"]
        lookback_df = db_ops.execute_query_file(
            query_file,
            as_df=True,
        )

        if lookback_df.empty:
            return pd.DataFrame(columns=["ship_name", "time", "lat", "lon", "sog", "cog"])

        lookback_df["time"] = pd.to_numeric(lookback_df["time"], errors="coerce")
        return lookback_df
