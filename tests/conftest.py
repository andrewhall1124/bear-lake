import pytest
import polars as pl
import tempfile
import shutil
from pathlib import Path
from bear_lake import Database


@pytest.fixture
def temp_db_path():
    """Create a temporary directory for database tests."""
    temp_dir = tempfile.mkdtemp(prefix="bear_lake_test_")
    yield temp_dir
    # Cleanup after test
    shutil.rmtree(temp_dir, ignore_errors=True)


@pytest.fixture
def db(temp_db_path):
    """Create a Database instance with a temporary path."""
    return Database(temp_db_path)


@pytest.fixture
def sample_schema():
    """Return a sample schema for testing."""
    return {
        "id": pl.Int64,
        "name": pl.String,
        "age": pl.Int32,
        "city": pl.String,
    }


@pytest.fixture
def sample_data():
    """Return sample data for testing."""
    return pl.DataFrame(
        {
            "id": [1, 2, 3, 4, 5],
            "name": ["Alice", "Bob", "Charlie", "David", "Eve"],
            "age": [25, 30, 35, 28, 32],
            "city": ["NYC", "LA", "NYC", "SF", "LA"],
        }
    )


@pytest.fixture
def partitioned_data():
    """Return partitioned sample data for testing."""
    return pl.DataFrame(
        {
            "id": [1, 2, 3, 4, 5, 6, 7, 8],
            "name": [
                "Alice",
                "Bob",
                "Charlie",
                "David",
                "Eve",
                "Frank",
                "Grace",
                "Henry",
            ],
            "age": [25, 30, 35, 28, 32, 40, 29, 33],
            "city": ["NYC", "LA", "NYC", "SF", "LA", "NYC", "SF", "LA"],
            "country": ["USA", "USA", "USA", "USA", "USA", "USA", "USA", "USA"],
        }
    )


@pytest.fixture
def multi_partition_schema():
    """Return a schema with multiple partition keys."""
    return {
        "id": pl.Int64,
        "name": pl.String,
        "age": pl.Int32,
        "city": pl.String,
        "country": pl.String,
    }
