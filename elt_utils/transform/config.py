"""
Config class to load and validate pipeline configuration from a YAML file.
"""
from __future__ import annotations

from copy import deepcopy
from pathlib import Path
from typing import Any

import yaml


class Config:
    """Loads and validates the pipeline config.yaml. All pipeline behaviour is driven from here."""

    _DEFAULT_CONFIG: dict[str, Any] = {
        "pipeline": {
            "source_db": "data/raw/ship_positions.db",
            "target_db": "data/silver/ship_positions.db",
            "source_schema": "raw",
            "source_table": "ship_positions",
            "target_schema": "silver",
            "target_table": "SHIP_POSITIONS",
            "rejects_table": "SHIP_POSITIONS_REJECTS",
            "sql_dir": "elt_utils/sql",
        },
        "dqa": {
            "lat_min": -90.0,
            "lat_max": 90.0,
            "lon_min": -180.0,
            "lon_max": 180.0,
            "sog_min": 0.0,
            "sog_max": 60.0, # According to chatgpt5.2 army speed boats can reach up to 50-60 knots, 
                             # while the fastest commercial ships typically max out around 30-40 knots. 
                             # Setting a max of 60 should allow for some margin of error while still catching unrealistic values.
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

    def __init__(self, config_path: str):
        """Parse config.yaml at config_path into an accessible config dict."""
        self._config_path = Path(config_path).resolve()
        if not self._config_path.exists():
            raise FileNotFoundError(f"Config file not found: {self._config_path}")

        with self._config_path.open("r", encoding="utf-8") as config_file:
            loaded = yaml.safe_load(config_file) or {}

        if not isinstance(loaded, dict):
            raise ValueError("Configuration file must contain a dictionary-like structure.")

        merged = self._merge_dicts(deepcopy(self._DEFAULT_CONFIG), loaded)
        self._configs = self._normalize_and_validate(merged)

    @property
    def configs(self) -> dict:
        """Return the full config dict."""
        return self._configs

    @staticmethod
    def _merge_dicts(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
        """Recursively merge two dictionaries with values from override, updating base."""
        for key, value in override.items():
            if isinstance(base.get(key), dict) and isinstance(value, dict):
                base[key] = Config._merge_dicts(base[key], value)
            else:
                base[key] = value
        return base

    def _normalize_and_validate(self, configs: dict[str, Any]) -> dict[str, Any]:
        """Perform path resolution, normalization, and validation on config values."""
        project_root = self._config_path.parents[2]
        pipeline = configs["pipeline"]

        for path_key in ("source_db", "target_db", "sql_dir"):
            resolved = Path(pipeline[path_key])
            if not resolved.is_absolute():
                resolved = (project_root / resolved).resolve()
            pipeline[path_key] = str(resolved)

        sql_dir = Path(pipeline["sql_dir"])
        if not sql_dir.exists() or not sql_dir.is_dir():
            raise FileNotFoundError(f"SQL directory does not exist: {sql_dir}")

        for query_name, query_file in configs["queries"].items():
            query_path = sql_dir / query_file
            if not query_path.exists():
                raise FileNotFoundError(f"Missing SQL file for '{query_name}': {query_path}")

        configs["project_root"] = str(project_root)
        return configs
