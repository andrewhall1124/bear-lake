"""Bear Lake - A lightweight, file-based database built on Polars and Parquet."""

from .database import Database
from .connection import connect, connect_s3
from .table import table
from . import connection


# Re-export connection module attributes directly so global state works
def __getattr__(name):
    """Allow access to global connection state variables."""
    if name in ("DATABASE_PATH", "CONNECTED", "STORAGE_OPTIONS"):
        return getattr(connection, name)
    raise AttributeError(f"module '{__name__}' has no attribute '{name}'")


def __setattr__(name, value):
    """Allow modification of global connection state variables."""
    if name in ("DATABASE_PATH", "CONNECTED", "STORAGE_OPTIONS"):
        setattr(connection, name, value)
    else:
        raise AttributeError(f"module '{__name__}' has no attribute '{name}'")


__all__ = [
    "Database",
    "connect",
    "connect_s3",
    "table",
    "DATABASE_PATH",
    "CONNECTED",
    "STORAGE_OPTIONS",
]
