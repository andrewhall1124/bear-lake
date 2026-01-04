import polars as pl
from typing import Any, Union


class TableOperation:
    """Base class for table operations."""

    def __init__(self, table_name: str):
        self.table_name = table_name


class CreateTableOperation(TableOperation):
    """DDL operation to create a table."""

    def __init__(
        self,
        table_name: str,
        schema: dict[str, type[pl.DataType]],
        partition_keys: Union[list[Any], None] = None,
        mode: str = "error",
    ):
        super().__init__(table_name)
        self.schema = schema
        self.partition_keys = partition_keys or []
        self.mode = mode


class DropTableOperation(TableOperation):
    """DDL operation to drop a table."""

    pass


class DeleteOperation(TableOperation):
    """DML operation to delete rows from a table."""

    def __init__(self, table_name: str, condition: pl.Expr):
        super().__init__(table_name)
        self.condition = condition


class TableQuery:
    """Represents a query to be executed on a table."""

    def __init__(self, table_name: str):
        self.table_name = table_name
        self._selected_columns: Union[list[str], None] = None
        self._filters: list[pl.Expr] = []
        self._joins: list[tuple[str, pl.Expr, str]] = []
        self._sort_columns: list[str] = []

    def select(self, *columns: str) -> "TableQuery":
        """Select specific columns."""
        self._selected_columns = list(columns)
        return self

    def filter(self, condition: pl.Expr) -> "TableQuery":
        """Add a filter condition."""
        self._filters.append(condition)
        return self

    def join(
        self, other: "TableQuery", on: Union[str, pl.Expr], how: str = "inner"
    ) -> "TableQuery":
        """Join with another table."""
        other_table = other.table_name if isinstance(other, TableQuery) else other
        self._joins.append((other_table, on, how))
        return self

    def sort(self, *columns: str) -> "TableQuery":
        """Sort by columns."""
        self._sort_columns = list(columns)
        return self


class TableBuilder:
    """Builder for creating table operations and queries."""

    def __init__(self, table_name: str):
        self.table_name = table_name

    def create(
        self,
        schema: dict[str, type[pl.DataType]],
        partition_keys: Union[list[Any], None] = None,
        mode: str = "error",
    ) -> CreateTableOperation:
        """Create a table with the given schema.

        Args:
            schema: Dictionary mapping column names to Polars data types
            partition_keys: List of partition keys (columns or expressions)
            mode: One of 'error', 'replace', 'append'
                - 'error': Raise error if table exists
                - 'replace': Drop and recreate table if it exists
                - 'append': Append to existing table (must have same schema)
        """
        return CreateTableOperation(self.table_name, schema, partition_keys, mode)

    def drop(self) -> DropTableOperation:
        """Drop the table."""
        return DropTableOperation(self.table_name)

    def delete(self, condition: pl.Expr) -> DeleteOperation:
        """Delete rows matching the condition."""
        return DeleteOperation(self.table_name, condition)

    def select(self, *columns: str) -> TableQuery:
        """Select columns from the table."""
        query = TableQuery(self.table_name)
        if columns:
            query.select(*columns)
        return query

    def filter(self, condition: pl.Expr) -> TableQuery:
        """Filter rows from the table."""
        query = TableQuery(self.table_name)
        return query.filter(condition)

    def join(
        self, other: Union["TableBuilder", TableQuery], on: Union[str, pl.Expr], how: str = "inner"
    ) -> TableQuery:
        """Join with another table."""
        query = TableQuery(self.table_name)
        return query.join(other, on, how)

    def sort(self, *columns: str) -> TableQuery:
        """Sort the table by columns."""
        query = TableQuery(self.table_name)
        return query.sort(*columns)


def table(name: str) -> TableBuilder:
    """Create a table builder for the given table name."""
    return TableBuilder(name)
