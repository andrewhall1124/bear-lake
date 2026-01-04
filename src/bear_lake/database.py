import polars as pl
from pathlib import Path
import shutil
from typing import Any, Union

from .metadata import MetadataManager
from .table import (
    TableOperation,
    CreateTableOperation,
    DropTableOperation,
    DeleteOperation,
    TableQuery,
)


class Database:
    """Database connection for managing Parquet data lakes."""

    def __init__(self, path: str):
        self.path = Path(path)
        self.path.mkdir(parents=True, exist_ok=True)
        self.metadata = MetadataManager(str(self.path))

    def __repr__(self) -> str:
        tables = self.metadata.list_tables()
        return f"Database(path='{self.path}', tables={tables})"

    def _get_table_path(self, table_name: str) -> Path:
        """Get the directory path for a table."""
        return self.path / table_name

    def _get_partition_path(
        self, table_name: str, partition_values: Union[dict[str, Any], None] = None
    ) -> Path:
        """Get the partition path for a table."""
        table_path = self._get_table_path(table_name)

        if partition_values:
            for key, value in partition_values.items():
                table_path = table_path / f"{key}={value}"

        return table_path

    def command(self, operation: TableOperation) -> None:
        """Execute a DDL or DML operation.

        Args:
            operation: A table operation (create, drop, delete)
        """
        if isinstance(operation, CreateTableOperation):
            self._execute_create(operation)
        elif isinstance(operation, DropTableOperation):
            self._execute_drop(operation)
        elif isinstance(operation, DeleteOperation):
            self._execute_delete(operation)
        else:
            raise ValueError(f"Unknown operation type: {type(operation)}")

    def _execute_create(self, operation: CreateTableOperation) -> None:
        """Execute a CREATE TABLE operation."""
        if self.metadata.table_exists(operation.table_name):
            if operation.mode == "error":
                raise ValueError(f"Table '{operation.table_name}' already exists")
            elif operation.mode == "replace":
                self._execute_drop(DropTableOperation(operation.table_name))
            elif operation.mode == "append":
                existing_schema = self.metadata.get_schema(operation.table_name)
                if existing_schema != operation.schema:
                    raise ValueError(
                        f"Cannot append: schema mismatch for table '{operation.table_name}'"
                    )
                return

        partition_key_names = []
        for key in operation.partition_keys:
            if isinstance(key, pl.Expr):
                key_str = str(key)
                if ".dt.year()" in key_str:
                    import re
                    match = re.search(r'col\(["\']?(\w+)["\']?\)', key_str)
                    if match:
                        col_name = match.group(1)
                        partition_key_names.append(f"{col_name}_year")
                    else:
                        partition_key_names.append("year")
                else:
                    partition_key_names.append(key_str)
            else:
                partition_key_names.append(str(key))

        self.metadata.create_table_metadata(
            operation.table_name, operation.schema, partition_key_names
        )

        table_path = self._get_table_path(operation.table_name)
        table_path.mkdir(parents=True, exist_ok=True)

    def _execute_drop(self, operation: DropTableOperation) -> None:
        """Execute a DROP TABLE operation."""
        if not self.metadata.table_exists(operation.table_name):
            raise ValueError(f"Table '{operation.table_name}' does not exist")

        table_path = self._get_table_path(operation.table_name)
        if table_path.exists():
            shutil.rmtree(table_path)

        self.metadata.delete_table_metadata(operation.table_name)

    def _execute_delete(self, operation: DeleteOperation) -> None:
        """Execute a DELETE operation."""
        if not self.metadata.table_exists(operation.table_name):
            raise ValueError(f"Table '{operation.table_name}' does not exist")

        df = self.query(TableQuery(operation.table_name))

        filtered_df = df.filter(~operation.condition)

        table_path = self._get_table_path(operation.table_name)
        if table_path.exists():
            shutil.rmtree(table_path)
        table_path.mkdir(parents=True, exist_ok=True)

        if len(filtered_df) > 0:
            filtered_df.write_parquet(table_path / "data.parquet")

        self.metadata.update_last_modified(operation.table_name)

    def insert(self, table: str, data: pl.DataFrame) -> None:
        """Insert data into a table.

        Args:
            table: Name of the table
            data: Polars DataFrame to insert
        """
        if not self.metadata.table_exists(table):
            raise ValueError(f"Table '{table}' does not exist")

        self.metadata.validate_schema(table, data)

        metadata = self.metadata.get_table_metadata(table)
        partition_keys = metadata.get("partition_keys", [])

        table_path = self._get_table_path(table)

        if partition_keys:
            data_with_partitions = data
            for key in partition_keys:
                if "_year" in key and key not in data.columns:
                    base_col = key.replace("_year", "")
                    if base_col in data.columns:
                        data_with_partitions = data_with_partitions.with_columns(
                            pl.col(base_col).dt.year().alias(key)
                        )

            data_with_partitions.write_parquet(
                table_path,
                use_pyarrow=True,
                pyarrow_options={"partition_cols": partition_keys},
            )
        else:
            parquet_files = list(table_path.glob("*.parquet"))
            next_file_num = len(parquet_files) + 1
            file_name = f"data_{next_file_num:03d}.parquet"

            data.write_parquet(table_path / file_name)

        self.metadata.update_last_modified(table)

    def query(self, table_query: TableQuery) -> pl.DataFrame:
        """Execute a query and return a DataFrame.

        Args:
            table_query: A TableQuery object

        Returns:
            Polars DataFrame with query results
        """
        if not self.metadata.table_exists(table_query.table_name):
            raise ValueError(f"Table '{table_query.table_name}' does not exist")

        table_path = self._get_table_path(table_query.table_name)

        metadata = self.metadata.get_table_metadata(table_query.table_name)
        partition_keys = metadata.get("partition_keys", [])

        parquet_pattern = str(table_path / "**" / "*.parquet")
        try:
            if partition_keys:
                df = pl.scan_parquet(parquet_pattern, hive_partitioning=True)
            else:
                df = pl.scan_parquet(parquet_pattern)
        except Exception:
            parquet_files = list(table_path.glob("*.parquet"))
            if not parquet_files:
                schema = self.metadata.get_schema(table_query.table_name)
                return pl.DataFrame(schema=schema)
            df = pl.scan_parquet(str(table_path / "*.parquet"))

        for condition in table_query._filters:
            df = df.filter(condition)

        for join_table, join_on, join_how in table_query._joins:
            other_path = self._get_table_path(join_table)
            other_pattern = str(other_path / "**" / "*.parquet")
            try:
                other_df = pl.scan_parquet(other_pattern)
            except Exception:
                other_df = pl.scan_parquet(str(other_path / "*.parquet"))

            df = df.join(other_df, on=join_on, how=join_how)

        if table_query._sort_columns:
            df = df.sort(table_query._sort_columns)

        if table_query._selected_columns is not None:
            df = df.select(table_query._selected_columns)

        return df.collect()

    def optimize(self, table: str) -> None:
        """Optimize a table by deduplicating and compacting parquet files.

        Args:
            table: Name of the table to optimize
        """
        if not self.metadata.table_exists(table):
            raise ValueError(f"Table '{table}' does not exist")

        df = self.query(TableQuery(table))

        df = df.unique()

        table_path = self._get_table_path(table)
        if table_path.exists():
            shutil.rmtree(table_path)
        table_path.mkdir(parents=True, exist_ok=True)

        metadata = self.metadata.get_table_metadata(table)
        partition_keys = metadata.get("partition_keys", [])

        if partition_keys:
            df.write_parquet(
                table_path,
                use_pyarrow=True,
                pyarrow_options={"partition_cols": partition_keys},
            )
        else:
            df.write_parquet(table_path / "data.parquet")

        self.metadata.update_last_modified(table)


def connect(path: str) -> Database:
    """Connect to a database at the given path.

    Args:
        path: Path to the database directory

    Returns:
        Database instance
    """
    return Database(path)
