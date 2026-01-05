import polars as pl
from . import connection


def table(name: str) -> pl.LazyFrame:
    """Get a LazyFrame for a table in the connected database.

    Args:
        name: The name of the table.

    Returns:
        A Polars LazyFrame for querying the table.

    Raises:
        RuntimeError: If not connected to a database.
    """
    if not connection.CONNECTED:
        raise RuntimeError("Not connected to database!")

    path = f"{connection.DATABASE_PATH}/{name}/**/*.parquet"

    return pl.scan_parquet(path, storage_options=connection.STORAGE_OPTIONS)
