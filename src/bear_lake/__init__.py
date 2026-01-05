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

        # Create metadata
        metadata = {
            "name": name,
            "schema": str(schema),
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