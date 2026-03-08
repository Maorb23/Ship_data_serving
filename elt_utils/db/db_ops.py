"""
DBops is a wrapper around duckdb connection that abstracts all database operations.
It provides methods to execute SQL queries from files or strings,
write pandas DataFrames to tables,
"""
from __future__ import annotations

import re
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd


class DBops:
    """DuckDB session wrapper. Abstracts all database operations"""

    def __init__(self, db_path: str, sql_dir: str, target_db_path: str | None = None):
        """
        Open the DuckDB database at db_path.
        The raw.ship_positions table is already pre-populated.
        """
        # Path definition
        self.db_path = Path(db_path).resolve()
        self.sql_dir = Path(sql_dir).resolve()
        self.target_db_path = Path(target_db_path).resolve() if target_db_path else None

        # Path validation
        if not self.db_path.exists():
            raise FileNotFoundError(f"Source database does not exist: {self.db_path}")
        if not self.sql_dir.exists() or not self.sql_dir.is_dir():
            raise FileNotFoundError(f"SQL directory does not exist: {self.sql_dir}")

        # DuckDB connection setup
        self.conn = duckdb.connect(str(self.db_path))
        self.conn.execute("SET TimeZone='UTC';")

        # Target database setup
        
        if self.target_db_path:
            self.target_db_path.parent.mkdir(parents=True, exist_ok=True)
            escaped_path = str(self.target_db_path).replace("'", "''")
            self.conn.execute(f"ATTACH '{escaped_path}' AS target_db;")

    @staticmethod
    def _render_query_template(query_text: str, template_params: dict[str, Any]) -> str:
        """Render a SQL query template with the given parameters."""
        rendered = query_text
        for key, value in template_params.items():
            value_text = str(value)
            rendered = rendered.replace(f"{{{key}}}", value_text)
            rendered = rendered.replace(f"__{key.upper()}__", value_text)
        return rendered

    @staticmethod
    def _validate_identifier(value: str) -> None:
        """Validate that a string is a valid SQL identifier (e.g. for table or schema names)."""
        if not re.match(r"^[A-Za-z_][A-Za-z0-9_]*$", value):
            raise ValueError(f"Invalid SQL identifier: {value}")

    def _resolve_query_path(self, file_path: str) -> Path:
        """Resolve the full path to a SQL query file, ensuring it exists."""
        query_path = Path(file_path)
        if not query_path.is_absolute():
            query_path = self.sql_dir / query_path

        query_path = query_path.resolve()
        if not query_path.exists():
            raise FileNotFoundError(f"SQL query file not found: {query_path}")
        return query_path

    def execute_query_file(
        self,
        file_path: str,
        params: list | tuple | None = None,
        template_params: dict[str, Any] | None = None,
        as_df: bool = False,
        fetch: bool = True,
    ) -> list | pd.DataFrame:
        """Read a .sql file from disk, execute it with optional params, return all rows."""
        query_path = self._resolve_query_path(file_path)
        query_text = query_path.read_text(encoding="utf-8")

        if template_params:
            query_text = self._render_query_template(query_text, template_params)

        cursor = self.conn.execute(query_text, params or [])
        if as_df:
            return cursor.df()
        if fetch:
            return cursor.fetchall()
        return []

    def execute_sql(
        self,
        sql: str,
        params: list | tuple | None = None,
        as_df: bool = False,
        fetch: bool = True,
    ) -> list | pd.DataFrame:
        cursor = self.conn.execute(sql, params or [])
        if as_df:
            return cursor.df()
        if fetch:
            return cursor.fetchall()
        return []

    def write_df_to_table(self, df: "pd.DataFrame", schema: str, table_name: str) -> None:
        """Write a pandas DataFrame into sql.table_name."""
        if df.empty:
            return

        self._validate_identifier(schema)
        self._validate_identifier(table_name)

        temp_table_name = f"_tmp_{schema}_{table_name}"
        self.register_df(temp_table_name, df)
        try:
            self.conn.execute(f"CREATE SCHEMA IF NOT EXISTS {schema};")
            self.conn.execute(
                f"INSERT INTO {schema}.{table_name} SELECT * FROM {temp_table_name};"
            )
        finally:
            self.unregister(temp_table_name)

    def register_df(self, name: str, df: pd.DataFrame) -> None:
        self._validate_identifier(name)
        self.conn.register(name, df)

    def unregister(self, name: str) -> None:
        self._validate_identifier(name)
        try:
            self.conn.unregister(name)
        except duckdb.Error:
            pass

    def begin(self) -> None:
        self.conn.execute("BEGIN TRANSACTION;")

    def commit(self) -> None:
        self.conn.execute("COMMIT;")

    def rollback(self) -> None:
        self.conn.execute("ROLLBACK;")

    def close(self) -> None:
        """Close the DuckDB connection."""
        self.conn.close()

    

    


