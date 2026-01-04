"""bear-lake: A database-like CLI for managing Parquet data lakes using Polars."""

from .database import Database, connect
from .table import (
    table,
    TableBuilder,
    TableQuery,
    TableOperation,
    CreateTableOperation,
    DropTableOperation,
    DeleteOperation,
)
from .metadata import MetadataManager

__version__ = "0.1.0"

__all__ = [
    "connect",
    "table",
    "Database",
    "TableBuilder",
    "TableQuery",
    "TableOperation",
    "CreateTableOperation",
    "DropTableOperation",
    "DeleteOperation",
    "MetadataManager",
]
