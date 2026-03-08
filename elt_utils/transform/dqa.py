"""
Data Quality Assurance (DQA) class for handling data quality issues in the delta records.
"""

from __future__ import annotations

from typing import Any

import pandas as pd


class Dqa:
    """
    Responsible for handle data quality issues in the delta records.
    """

    @staticmethod
    def _append_reason(df: pd.DataFrame, mask: pd.Series, reason: str) -> None:
        if not mask.any():
            return

        existing = df.loc[mask, "dq_reason"]
        df.loc[mask, "dq_reason"] = existing.where(existing == "", existing + ";") + reason

    @staticmethod
    def dqa(delta_df: pd.DataFrame, configs: dict[str, Any]) -> tuple[pd.DataFrame, pd.DataFrame]:
        """Run all quality checks with mixed policy."""
        # Define the columns for the clean and rejects dataframes
        clean_columns = ["ship_name", "time", "lat", "lon", "sog", "cog"]
        reject_columns = ["ship_name", "time", "lat", "lon", "sog", "cog", "dq_reason"]

        if delta_df is None or delta_df.empty:
            return pd.DataFrame(columns=clean_columns), pd.DataFrame(columns=reject_columns)

        dqa_cfg = configs["dqa"]
        data = delta_df.copy()

        # Ship name handling: convert to string, strip whitespace, and treat empty strings as missing
        data["ship_name"] = data["ship_name"].astype("string").str.strip()
        for numeric_col in ("time", "lat", "lon", "sog", "cog"):
            data[numeric_col] = pd.to_numeric(data[numeric_col], errors="coerce")

        # Sequential comparisons per ship for anomaly detection
        ordered = data.sort_values(["ship_name", "time"], kind="mergesort").copy()
        grouped = ordered.groupby("ship_name", dropna=False)
        ordered["prev_time"] = grouped["time"].shift(1)
        ordered["prev_lat"] = grouped["lat"].shift(1)
        ordered["prev_lon"] = grouped["lon"].shift(1)
        ordered["prev_sog"] = grouped["sog"].shift(1)
        ordered["prev_cog"] = grouped["cog"].shift(1)
        ordered["time_diff_sec"] = ordered["time"] - ordered["prev_time"]
        ordered["sog_diff"] = (ordered["sog"] - ordered["prev_sog"]).abs()

        for derived_col in (
            "prev_time",
            "prev_lat",
            "prev_lon",
            "prev_sog",
            "prev_cog",
            "time_diff_sec",
            "sog_diff",
        ):
            data[derived_col] = ordered[derived_col].reindex(data.index)

        ship_name_missing = data["ship_name"].isna() | (data["ship_name"] == "")
        time_missing = data["time"].isna()
        time_invalid = data["time"] <= 0

        # Initialize dq_reason column to empty string for all records
        data["dq_reason"] = ""

        # Latitude and longitude checks
        lat_missing = data["lat"].isna()
        lat_invalid = (~lat_missing) & (
            (data["lat"] < dqa_cfg["lat_min"]) | (data["lat"] > dqa_cfg["lat_max"])
        )

        lon_missing = data["lon"].isna()
        lon_invalid = (~lon_missing) & (
            (data["lon"] < dqa_cfg["lon_min"]) | (data["lon"] > dqa_cfg["lon_max"])
        )
        gps_error = (~lat_missing) & (~lon_missing) & (data["lat"] == 0) & (data["lon"] == 0)

        critical_mask = (
            ship_name_missing
            | time_missing
            | time_invalid
            | lat_missing
            | lat_invalid
            | lon_missing
            | lon_invalid
            | gps_error
        )

        Dqa._append_reason(data, ship_name_missing, "ship_name_missing")
        Dqa._append_reason(data, time_missing, "time_missing")
        Dqa._append_reason(data, time_invalid, "time_non_positive")
        Dqa._append_reason(data, lat_missing, "lat_missing")
        Dqa._append_reason(data, lat_invalid, "lat_out_of_range")
        Dqa._append_reason(data, lon_missing, "lon_missing")
        Dqa._append_reason(data, lon_invalid, "lon_out_of_range")
        Dqa._append_reason(data, gps_error, "gps_zero_coordinates")

        sog_missing = data["sog"].isna()
        sog_invalid = (~sog_missing) & (
            (data["sog"] < dqa_cfg["sog_min"]) | (data["sog"] > dqa_cfg["sog_max"])
        )

        cog_missing = data["cog"].isna()
        cog_invalid = (~cog_missing) & (
            (data["cog"] < dqa_cfg["cog_min"]) | (data["cog"] > dqa_cfg["cog_max"])
        )

        Dqa._append_reason(data, sog_missing, "sog_missing")
        Dqa._append_reason(data, sog_invalid, "sog_out_of_range")
        Dqa._append_reason(data, cog_missing, "cog_missing")
        Dqa._append_reason(data, cog_invalid, "cog_out_of_range")

        data.loc[sog_missing | sog_invalid, "sog"] = pd.NA
        data.loc[cog_missing | cog_invalid, "cog"] = pd.NA

        max_sog_diff = float(dqa_cfg.get("max_sog_diff", 40.0))
        valid_ship = data["ship_name"].notna() & (data["ship_name"] != "")
        has_prev_observation = valid_ship & data["prev_time"].notna()
        time_not_increasing = has_prev_observation & (data["time_diff_sec"] <= 0)
        sog_extreme_diff = has_prev_observation & data["sog_diff"].notna() & (data["sog_diff"] > max_sog_diff)

        Dqa._append_reason(data, time_not_increasing, "time_not_increasing")
        Dqa._append_reason(data, sog_extreme_diff, "sog_extreme_diff")

        critical_mask = critical_mask | time_not_increasing | sog_extreme_diff

        # Chose to reject records with critical issues, but allow 
        # non-critical issues to pass with nulls in the clean data, like missing/invalid SOG or COG which 
        # could be common but we don't want to reject the whole record for.
        rejects_df = data.loc[critical_mask, reject_columns].reset_index(drop=True)
        clean_df = data.loc[~critical_mask, clean_columns].reset_index(drop=True)
        return clean_df, rejects_df
