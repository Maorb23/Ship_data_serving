
"""
BL class responsible for job-specific business logic, such as computing derived or enriched fields
"""

from __future__ import annotations

import numpy as np
import pandas as pd


class Bl:
    """
    Job-specific business logic. Computes derived or enriched fields
    """

    EARTH_RADIUS_NM = 3440.065 # Earth radius in nautical miles, used for distance calculation

    @staticmethod
    def _haversine_nm(
        prev_lat: pd.Series,
        prev_lon: pd.Series,
        curr_lat: pd.Series,
        curr_lon: pd.Series,
    ) -> np.ndarray:
        """
        Calculate the haversine distance in nautical miles between two points.
        """
        lat1 = np.radians(prev_lat.astype(float))
        lon1 = np.radians(prev_lon.astype(float))
        lat2 = np.radians(curr_lat.astype(float))
        lon2 = np.radians(curr_lon.astype(float))

        dlat = lat2 - lat1
        dlon = lon2 - lon1
        # haversine formula:
        # a = sin²(Δlat/2) + cos(lat1) * cos(lat2) * sin²(Δlon/2)
        a = np.sin(dlat / 2.0) ** 2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon / 2.0) ** 2
        return 2.0 * Bl.EARTH_RADIUS_NM * np.arcsin(np.sqrt(a))

    @staticmethod
    def calc_enrichments(clean_df: pd.DataFrame, lookback_df: pd.DataFrame) -> pd.DataFrame:
        """
        Compute enrichments
        """
        output_columns = [
            "ship_name",
            "time",
            "lat",
            "lon",
            "sog",
            "cog",
            "rot",
            "acceleration",
            "distance_traveled",
        ]
        base_columns = ["ship_name", "time", "lat", "lon", "sog", "cog"]

        if clean_df is None or clean_df.empty:
            return pd.DataFrame(columns=output_columns)

        current = clean_df[base_columns].copy()
        current["__is_lookback"] = False
        
        if lookback_df is not None and not lookback_df.empty:
            lookback = lookback_df[base_columns].copy()
            lookback["__is_lookback"] = True
            merged = pd.concat([lookback, current], ignore_index=True)
            merged = merged.sort_values(["ship_name", "time", "__is_lookback"])
            merged = merged.drop_duplicates(subset=["ship_name", "time"], keep="last")
        else:
            merged = current

        for numeric_col in ("time", "lat", "lon", "sog", "cog"):
            merged[numeric_col] = pd.to_numeric(merged[numeric_col], errors="coerce")

        merged = merged.sort_values(["ship_name", "time"]).reset_index(drop=True)
        grouped = merged.groupby("ship_name", dropna=False)

        prev_cog = grouped["cog"].shift(1)
        prev_sog = grouped["sog"].shift(1)
        prev_lat = grouped["lat"].shift(1)
        prev_lon = grouped["lon"].shift(1)

        rot = ((merged["cog"] - prev_cog + 540.0) % 360.0) - 180.0
        rot_valid = merged["cog"].notna() & prev_cog.notna()
        merged["rot"] = rot.where(rot_valid, pd.NA)

        acceleration = merged["sog"] - prev_sog
        acceleration_valid = merged["sog"].notna() & prev_sog.notna()
        merged["acceleration"] = acceleration.where(acceleration_valid, pd.NA)

        distance_raw = pd.Series(
            Bl._haversine_nm(prev_lat, prev_lon, merged["lat"], merged["lon"]),
            index=merged.index,
        )
        distance_valid = prev_lat.notna() & prev_lon.notna() & merged["lat"].notna() & merged["lon"].notna()
        merged["distance_traveled"] = distance_raw.where(distance_valid, pd.NA)

        enriched = merged.loc[~merged["__is_lookback"], output_columns].reset_index(drop=True)
        return enriched
