import polars as pl
from pathlib import Path
import json
import shutil

DATABASE_PATH = ""
CONNECTED = False

class Database:
    def __init__(self, path: str) -> None:
        self.path = Path(path)
        self.path.mkdir(parents=True, exist_ok=True)

    def create(self, name: str, schema: dict[str, pl.DataType], partition_keys: list[str]) -> None:
        # Create table folders
        table_path = self.path / name
        table_path.mkdir(parents=True, exist_ok=True)

        # Create metadata - serialize schema to strings
        schema_serialized = {col: str(dtype) for col, dtype in schema.items()}
        metadata = {
            "name": name,
            "schema": schema_serialized,
            "partition_keys": partition_keys
        }

        # Write metadata file
        with open(table_path / "metadata.json", "w") as file:
            json.dump(metadata, file)

    def insert(self, name: str, data: pl.DataFrame):
        # Get metadata
        table_path = self.path / name
        with open(table_path / "metadata.json", "r") as file:
            metadata = json.load(file)

        # Iterate over partition groups
        p_keys = metadata['partition_keys']
        for p_values, group in data.group_by(p_keys):

            # Build file path
            partition_path = table_path
            for p_value in p_values:
                # Create every folder except the last (file name)
                partition_path.mkdir(parents=True, exist_ok=True)
                partition_path = partition_path / str(p_value)

            # Write parquet
            group.write_parquet(partition_path.with_suffix(".parquet"))
    
    def query(self, expression: pl.LazyFrame) -> pl.DataFrame:
        return expression.collect()
    
    def delete(self, name: str, expression: pl.Expr):
        table_path = self.path / name
        parquet_files = list(table_path.rglob("*.parquet"))

        for file_path in parquet_files:
            # Read the parquet file
            df = pl.read_parquet(file_path)

            # Filter out rows matching the delete expression
            filtered_df = df.filter(~expression)

            # Overwrite the file with filtered data
            if len(filtered_df) > 0:
                filtered_df.write_parquet(file_path)
            else:
                # Remove empty files
                file_path.unlink()

    def drop(self, name: str):
        table_path = self.path / name
        shutil.rmtree(table_path)

    def list_tables(self) -> list[str]:
        tables = []
        for item in self.path.iterdir():
            if item.is_dir() and (item / "metadata.json").exists():
                tables.append(item.name)
        return tables

    def get_schema(self, name: str) -> dict[str, pl.DataType]:
        table_path = self.path / name
        with open(table_path / "metadata.json", "r") as file:
            metadata = json.load(file)

        # Deserialize schema strings back to DataTypes
        schema_str = metadata["schema"]
        schema = {col: self._deserialize_dtype(dtype_str) for col, dtype_str in schema_str.items()}
        return schema

    def _deserialize_dtype(self, dtype_str: str) -> pl.DataType:
        # Map common string representations to Polars DataTypes
        dtype_map = {
            "Int8": pl.Int8,
            "Int16": pl.Int16,
            "Int32": pl.Int32,
            "Int64": pl.Int64,
            "UInt8": pl.UInt8,
            "UInt16": pl.UInt16,
            "UInt32": pl.UInt32,
            "UInt64": pl.UInt64,
            "Float32": pl.Float32,
            "Float64": pl.Float64,
            "Boolean": pl.Boolean,
            "Utf8": pl.Utf8,
            "String": pl.String,
            "Binary": pl.Binary,
            "Date": pl.Date,
            "Datetime": pl.Datetime,
            "Time": pl.Time,
            "Duration": pl.Duration,
            "Categorical": pl.Categorical,
        }

        # Return the DataType if found in map, otherwise try eval as fallback
        if dtype_str in dtype_map:
            return dtype_map[dtype_str]
        else:
            # For complex types like List, Struct, etc., use eval
            return eval(dtype_str, {"pl": pl})

def connect(path: str) -> Database:
    global DATABASE_PATH, CONNECTED
    DATABASE_PATH = Path(path)
    CONNECTED = True
    return Database(path)


def table(name: str) -> pl.LazyFrame:
    if not CONNECTED:
        raise RuntimeError("Not connected to database!")
    
    path = DATABASE_PATH / f"{name}/**/*.parquet"
    return pl.scan_parquet(path)