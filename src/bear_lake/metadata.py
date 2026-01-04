import json
from pathlib import Path
from datetime import datetime
from typing import Any, Union
import polars as pl


class MetadataManager:
    """Manages table metadata including schemas and partition keys."""

    def __init__(self, database_path: str):
        self.database_path = Path(database_path)

    def _get_table_path(self, table_name: str) -> Path:
        """Get the directory path for a table."""
        return self.database_path / table_name

    def _get_metadata_path(self, table_name: str) -> Path:
        """Get the path to the metadata.json file for a table."""
        return self._get_table_path(table_name) / "metadata.json"

    def _polars_type_to_string(self, dtype: type[pl.DataType]) -> str:
        """Convert Polars data type to string representation."""
        if dtype == pl.String:
            return "String"
        elif dtype == pl.Int64:
            return "Int64"
        elif dtype == pl.Int32:
            return "Int32"
        elif dtype == pl.Float64:
            return "Float64"
        elif dtype == pl.Float32:
            return "Float32"
        elif dtype == pl.Date:
            return "Date"
        elif dtype == pl.Datetime:
            return "Datetime"
        elif dtype == pl.Boolean:
            return "Boolean"
        else:
            return str(dtype)

    def _string_to_polars_type(self, type_str: str) -> type[pl.DataType]:
        """Convert string representation back to Polars data type."""
        type_map = {
            "String": pl.String,
            "Int64": pl.Int64,
            "Int32": pl.Int32,
            "Float64": pl.Float64,
            "Float32": pl.Float32,
            "Date": pl.Date,
            "Datetime": pl.Datetime,
            "Boolean": pl.Boolean,
        }
        return type_map.get(type_str, pl.String)

    def create_table_metadata(
        self,
        table_name: str,
        schema: dict[str, type[pl.DataType]],
        partition_keys: Union[list[str], None] = None,
    ) -> None:
        """Create metadata file for a new table."""
        metadata = {
            "table_name": table_name,
            "schema": {
                col: self._polars_type_to_string(dtype)
                for col, dtype in schema.items()
            },
            "partition_keys": partition_keys or [],
            "created_at": datetime.now().isoformat(),
            "last_modified": datetime.now().isoformat(),
        }

        # Ensure table directory exists
        table_path = self._get_table_path(table_name)
        table_path.mkdir(parents=True, exist_ok=True)

        metadata_path = self._get_metadata_path(table_name)
        with open(metadata_path, "w") as f:
            json.dump(metadata, f, indent=2)

    def get_table_metadata(self, table_name: str) -> Union[dict[str, Any], None]:
        """Retrieve metadata for a table."""
        metadata_path = self._get_metadata_path(table_name)
        if not metadata_path.exists():
            return None

        with open(metadata_path, "r") as f:
            return json.load(f)

    def table_exists(self, table_name: str) -> bool:
        """Check if a table exists."""
        return self._get_metadata_path(table_name).exists()

    def update_last_modified(self, table_name: str) -> None:
        """Update the last modified timestamp for a table."""
        metadata = self.get_table_metadata(table_name)
        if metadata:
            metadata["last_modified"] = datetime.now().isoformat()
            metadata_path = self._get_metadata_path(table_name)
            with open(metadata_path, "w") as f:
                json.dump(metadata, f, indent=2)

    def delete_table_metadata(self, table_name: str) -> None:
        """Delete metadata file for a table."""
        metadata_path = self._get_metadata_path(table_name)
        if metadata_path.exists():
            metadata_path.unlink()

    def validate_schema(self, table_name: str, dataframe: pl.DataFrame) -> bool:
        """Validate that a DataFrame matches the table schema."""
        metadata = self.get_table_metadata(table_name)
        if not metadata:
            raise ValueError(f"Table '{table_name}' does not exist")

        table_schema = metadata["schema"]
        df_schema = {col: str(dtype) for col, dtype in zip(dataframe.columns, dataframe.dtypes)}

        for col, expected_type_str in table_schema.items():
            if col not in df_schema:
                raise ValueError(f"Missing column '{col}' in DataFrame")

            expected_type = self._string_to_polars_type(expected_type_str)
            actual_type_str = df_schema[col]

            if expected_type_str not in actual_type_str and str(expected_type) != actual_type_str:
                raise ValueError(
                    f"Column '{col}' type mismatch: expected {expected_type_str}, "
                    f"got {actual_type_str}"
                )

        return True

    def get_schema(self, table_name: str) -> dict[str, type[pl.DataType]]:
        """Get the schema as a dictionary of column names to Polars types."""
        metadata = self.get_table_metadata(table_name)
        if not metadata:
            raise ValueError(f"Table '{table_name}' does not exist")

        return {
            col: self._string_to_polars_type(type_str)
            for col, type_str in metadata["schema"].items()
        }

    def list_tables(self) -> list[str]:
        """List all tables in the database."""
        tables = []
        for path in self.database_path.iterdir():
            if path.is_dir() and (path / "metadata.json").exists():
                tables.append(path.name)
        return tables
