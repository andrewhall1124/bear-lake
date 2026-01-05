import polars as pl
import json
import shutil
import s3fs
import os
import glob

DATABASE_PATH = ""
CONNECTED = False
STORAGE_OPTIONS = None

class Database:
    def __init__(self, path: str, storage_options: dict[str, str] | None = None) -> None:
        self.storage_options = storage_options
        self.is_s3 = path.startswith('s3://') if storage_options else False
        self.path = path

        if not self.is_s3:
            os.makedirs(path, exist_ok=True)

    def _get_table_path(self, name: str) -> str:
        return f"{self.path}/{name}"
    
    def _get_fs(self) -> s3fs.S3FileSystem:
        return s3fs.S3FileSystem(
            key=self.storage_options['aws_access_key_id'],
            secret=self.storage_options['aws_secret_access_key'],
            endpoint_url=self.storage_options['endpoint_url'],
            client_kwargs={'region_name': self.storage_options['region']}
        )

    def create(self, name: str, schema: dict[str, pl.DataType], partition_keys: list[str], primary_keys: list[str], mode: str = "error") -> None:
        table_path = self._get_table_path(name)
        metadata_path = f"{table_path}/metadata.json"

        if self.is_s3:
            fs = self._get_fs()
            exists = fs.exists(metadata_path)
        else:
            exists = os.path.exists(metadata_path)

        # Handle existing table based on mode
        if exists:
            if mode == "error":
                raise FileExistsError(f"Table '{name}' already exists")
            elif mode == "skip":
                return
            elif mode == "replace":
                if self.is_s3:
                    fs = self._get_fs()
                    fs.rm(table_path, recursive=True)
                else:
                    shutil.rmtree(table_path)
            else:
                raise ValueError(f"Invalid mode: '{mode}'. Must be 'error', 'replace', or 'skip'")

        # Create table folders for local storage
        if not self.is_s3:
            os.makedirs(table_path, exist_ok=True)

        # Create metadata - serialize schema to strings
        schema_serialized = {col: str(dtype) for col, dtype in schema.items()}
        metadata = {
            "name": name,
            "schema": schema_serialized,
            "partition_keys": partition_keys,
            "primary_keys": primary_keys
        }

        # Write metadata file
        if self.is_s3:
            fs = self._get_fs()
            with fs.open(metadata_path, "w") as file:
                json.dump(metadata, file)
        else:
            with open(metadata_path, "w") as file:
                json.dump(metadata, file)

    def insert(self, name: str, data: pl.DataFrame, mode: str = "append"):
        # Get metadata
        table_path = self._get_table_path(name)
        metadata_path = f"{table_path}/metadata.json"

        if self.is_s3:
            fs = self._get_fs()
            with fs.open(metadata_path, "r") as file:
                metadata = json.load(file)
        else:
            with open(metadata_path, "r") as file:
                metadata = json.load(file)

        # Iterate over partition groups
        p_keys = metadata['partition_keys']
        for p_values, group in data.group_by(p_keys):

            # Build file path
            partition_path = table_path
            for p_value in p_values:
                if not self.is_s3:
                    # Create every folder except the last (file name)
                    os.makedirs(partition_path, exist_ok=True)
                partition_path = f"{partition_path}/{p_value}"

            # Get final parquet file path
            parquet_file = f"{partition_path}.parquet"

            # Handle mode
            if self.is_s3:
                fs = self._get_fs()
                file_exists = fs.exists(parquet_file)
            else:
                file_exists = os.path.exists(parquet_file)

            if file_exists:
                if mode == "error":
                    raise FileExistsError(f"Partition file '{parquet_file}' already exists")
                elif mode == "append":
                    # Read existing data and concatenate
                    existing_df = pl.read_parquet(parquet_file, storage_options=self.storage_options)
                    group = pl.concat([existing_df, group])
                elif mode == "overwrite":
                    # Will overwrite below
                    pass
                else:
                    raise ValueError(f"Invalid mode: '{mode}'. Must be 'append', 'overwrite', or 'error'")

            # Write parquet
            group.write_parquet(parquet_file, storage_options=self.storage_options)
    
    def query(self, expression: pl.LazyFrame) -> pl.DataFrame:
        return expression.collect()
    
    def delete(self, name: str, expression: pl.Expr):
        table_path = self._get_table_path(name)

        if self.is_s3:
            fs = self._get_fs()
            parquet_files = [f"s3://{f}" for f in fs.glob(f"{table_path}/**/*.parquet")]
        else:
            parquet_files = glob.glob(f"{table_path}/**/*.parquet", recursive=True)

        for file_path in parquet_files:
            # Read the parquet file
            df = pl.read_parquet(file_path, storage_options=self.storage_options)

            # Filter out rows matching the delete expression
            filtered_df = df.filter(~expression)

            # Overwrite the file with filtered data
            if len(filtered_df) > 0:
                filtered_df.write_parquet(file_path, storage_options=self.storage_options)
            else:
                # Remove empty files
                if self.is_s3:
                    fs = self._get_fs()
                    # Remove s3:// prefix for s3fs
                    fs.rm(file_path.replace("s3://", ""))
                else:
                    os.remove(file_path)

    def drop(self, name: str):
        table_path = self._get_table_path(name)

        if self.is_s3:
            fs = self._get_fs()
            fs.rm(table_path, recursive=True)
        else:
            shutil.rmtree(table_path)

    def list_tables(self) -> list[str]:
        tables = []

        if self.is_s3:
            fs = self._get_fs()
            # List directories in the base path
            try:
                items = fs.ls(self.path)
                for item in items:
                    # Check if metadata.json exists
                    metadata_path = f"{item}/metadata.json"
                    if fs.exists(metadata_path):
                        # Extract table name from path
                        table_name = item.split('/')[-1]
                        tables.append(table_name)
            except FileNotFoundError:
                pass
        else:
            if os.path.exists(self.path):
                for item in os.listdir(self.path):
                    item_path = os.path.join(self.path, item)
                    metadata_path = os.path.join(item_path, "metadata.json")
                    if os.path.isdir(item_path) and os.path.exists(metadata_path):
                        tables.append(item)

        return tables

    def get_schema(self, name: str) -> dict[str, pl.DataType]:
        table_path = self._get_table_path(name)
        metadata_path = f"{table_path}/metadata.json"

        if self.is_s3:
            fs = self._get_fs()
            with fs.open(metadata_path, "r") as file:
                metadata = json.load(file)
        else:
            with open(metadata_path, "r") as file:
                metadata = json.load(file)

        # Deserialize schema strings back to DataTypes
        schema_str = metadata["schema"]
        schema = {col: self._deserialize_dtype(dtype_str) for col, dtype_str in schema_str.items()}
        return schema

    def get_partition_keys(self, name: str) -> list[str]:
        table_path = self._get_table_path(name)
        metadata_path = f"{table_path}/metadata.json"

        if self.is_s3:
            fs = self._get_fs()
            with fs.open(metadata_path, "r") as file:
                metadata = json.load(file)
        else:
            with open(metadata_path, "r") as file:
                metadata = json.load(file)

        return metadata["partition_keys"]

    def get_primary_keys(self, name: str) -> list[str]:
        table_path = self._get_table_path(name)
        metadata_path = f"{table_path}/metadata.json"

        if self.is_s3:
            fs = self._get_fs()
            metadata_path = f"{table_path}/metadata.json"
            with fs.open(metadata_path, "r") as file:
                metadata = json.load(file)
        else:
            with open(metadata_path, "r") as file:
                metadata = json.load(file)

        return metadata["primary_keys"]

    def optimize(self, name: str) -> None:
        table_path = self._get_table_path(name)
        metadata_path = f"{table_path}/metadata.json"

        if self.is_s3:
            fs = self._get_fs()
            # Get metadata
            with fs.open(metadata_path, "r") as file:
                metadata = json.load(file)
            # Get all parquet files
            parquet_files = [f"s3://{f}" for f in fs.glob(f"{table_path}/**/*.parquet")]
        else:
            # Get metadata
            with open(metadata_path, "r") as file:
                metadata = json.load(file)
            # Get all parquet files
            parquet_files = glob.glob(f"{table_path}/**/*.parquet", recursive=True)

        primary_keys = metadata["primary_keys"]

        for file_path in parquet_files:
            # Read the parquet file
            df = pl.read_parquet(file_path, storage_options=self.storage_options)

            # Deduplicate by primary_keys if they exist
            if primary_keys:
                # Keep last occurrence of each unique combination of primary_keys
                df = df.unique(subset=primary_keys, keep="last")

            df = df.sort(primary_keys)

            # Overwrite the file with optimized data
            df.write_parquet(file_path, storage_options=self.storage_options)

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

def connect(path: str, storage_options: dict[str, str] | None = None) -> Database:
    global DATABASE_PATH, CONNECTED, STORAGE_OPTIONS

    DATABASE_PATH = path
    CONNECTED = True
    STORAGE_OPTIONS = storage_options
    return Database(path, storage_options)


def table(name: str) -> pl.LazyFrame:
    if not CONNECTED:
        raise RuntimeError("Not connected to database!")

    path = f"{DATABASE_PATH}/{name}/**/*.parquet"

    return pl.scan_parquet(path, storage_options=STORAGE_OPTIONS)