import polars as pl
import json
from tqdm import tqdm
from .filesystem_client import FileSystemClient, LocalClient, S3Client

DATABASE_PATH = ""
CONNECTED = False
STORAGE_OPTIONS = None


class Database:
    def __init__(
        self, path: str, storage_options: dict[str, str] | None = None
    ) -> None:
        self.storage_options = storage_options
        self.path = path

        # Select the appropriate filesystem client based on path
        if path.startswith("s3://") and storage_options:
            self.file_system_client: FileSystemClient = S3Client(storage_options)
        else:
            self.file_system_client: FileSystemClient = LocalClient()
            self.file_system_client.makedirs(path)

    def _get_table_path(self, name: str) -> str:
        return f"{self.path}/{name}"

    def create(
        self,
        name: str,
        schema: dict[str, pl.DataType],
        partition_keys: list[str],
        primary_keys: list[str],
        mode: str = "error",
    ) -> None:
        table_path = self._get_table_path(name)
        metadata_path = f"{table_path}/metadata.json"

        exists = self.file_system_client.exists(metadata_path)

        # Handle existing table based on mode
        if exists:
            if mode == "error":
                raise FileExistsError(f"Table '{name}' already exists")
            elif mode == "skip":
                return
            elif mode == "replace":
                self.file_system_client.remove(table_path, recursive=True)
            else:
                raise ValueError(
                    f"Invalid mode: '{mode}'. Must be 'error', 'replace', or 'skip'"
                )

        # Create table folders
        self.file_system_client.makedirs(table_path)

        # Create metadata - serialize schema to strings
        schema_serialized = {col: str(dtype) for col, dtype in schema.items()}
        metadata = {
            "name": name,
            "schema": schema_serialized,
            "partition_keys": partition_keys,
            "primary_keys": primary_keys,
        }

        # Write metadata file
        with self.file_system_client.open(metadata_path, "w") as file:
            json.dump(metadata, file)

    def insert(self, name: str, data: pl.DataFrame, mode: str = "append"):
        # Get metadata
        table_path = self._get_table_path(name)
        metadata_path = f"{table_path}/metadata.json"

        with self.file_system_client.open(metadata_path, "r") as file:
            metadata = json.load(file)

        # Iterate over partition groups
        p_keys = metadata["partition_keys"]
        partition_groups = list(data.group_by(p_keys))

        for p_values, group in tqdm(
            partition_groups, desc="Inserting partitions", unit="partition"
        ):
            # Build file path
            partition_path = table_path
            for p_value in p_values:
                # Create every folder except the last (file name)
                self.file_system_client.makedirs(partition_path)
                partition_path = f"{partition_path}/{p_value}"

            # Get final parquet file path
            parquet_file = f"{partition_path}.parquet"

            # Handle mode
            file_exists = self.file_system_client.exists(parquet_file)

            if file_exists:
                if mode == "error":
                    raise FileExistsError(
                        f"Partition file '{parquet_file}' already exists"
                    )
                elif mode == "append":
                    # Read existing data and concatenate
                    existing_df = pl.read_parquet(
                        parquet_file, storage_options=self.storage_options
                    )
                    group = pl.concat([existing_df, group])
                elif mode == "overwrite":
                    # Will overwrite below
                    pass
                else:
                    raise ValueError(
                        f"Invalid mode: '{mode}'. Must be 'append', 'overwrite', or 'error'"
                    )

            # Write parquet
            group.write_parquet(parquet_file, storage_options=self.storage_options)

    def query(self, expression: pl.LazyFrame) -> pl.DataFrame:
        return expression.collect()

    def delete(self, name: str, expression: pl.Expr):
        table_path = self._get_table_path(name)

        parquet_files = self.file_system_client.glob(f"{table_path}/**/*.parquet")

        for file_path in tqdm(parquet_files, desc="Deleting records", unit="file"):
            # Read the parquet file
            df = pl.read_parquet(file_path, storage_options=self.storage_options)

            # Filter out rows matching the delete expression
            filtered_df = df.filter(~expression)

            # Overwrite the file with filtered data
            if len(filtered_df) > 0:
                filtered_df.write_parquet(
                    file_path, storage_options=self.storage_options
                )
            else:
                # Remove empty files
                self.file_system_client.remove(file_path)

    def drop(self, name: str):
        table_path = self._get_table_path(name)
        self.file_system_client.remove(table_path, recursive=True)

    def list_tables(self) -> list[str]:
        tables = []

        items = self.file_system_client.listdir(self.path)
        for item in items:
            # For S3, item is the full path; for local, it's just the name
            item_path = item if item.startswith(self.path) else f"{self.path}/{item}"
            metadata_path = f"{item_path}/metadata.json"

            if self.file_system_client.exists(metadata_path):
                # Extract table name from path
                table_name = item.split("/")[-1]
                tables.append(table_name)

        return tables

    def get_schema(self, name: str) -> dict[str, pl.DataType]:
        table_path = self._get_table_path(name)
        metadata_path = f"{table_path}/metadata.json"

        with self.file_system_client.open(metadata_path, "r") as file:
            metadata = json.load(file)

        # Deserialize schema strings back to DataTypes
        schema_str = metadata["schema"]
        schema = {
            col: self._deserialize_dtype(dtype_str)
            for col, dtype_str in schema_str.items()
        }
        return schema

    def get_partition_keys(self, name: str) -> list[str]:
        table_path = self._get_table_path(name)
        metadata_path = f"{table_path}/metadata.json"

        with self.file_system_client.open(metadata_path, "r") as file:
            metadata = json.load(file)

        return metadata["partition_keys"]

    def get_primary_keys(self, name: str) -> list[str]:
        table_path = self._get_table_path(name)
        metadata_path = f"{table_path}/metadata.json"

        with self.file_system_client.open(metadata_path, "r") as file:
            metadata = json.load(file)

        return metadata["primary_keys"]

    def optimize(self, name: str) -> None:
        table_path = self._get_table_path(name)
        metadata_path = f"{table_path}/metadata.json"

        # Get metadata
        with self.file_system_client.open(metadata_path, "r") as file:
            metadata = json.load(file)

        # Get all parquet files
        parquet_files = self.file_system_client.glob(f"{table_path}/**/*.parquet")

        primary_keys = metadata["primary_keys"]

        for file_path in tqdm(parquet_files, desc="Optimizing partitions", unit="file"):
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
