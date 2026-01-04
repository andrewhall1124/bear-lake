import tempfile
import shutil
from pathlib import Path
import datetime as dt

import polars as pl
import pytest

import sys
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

import bear_lake as bl


@pytest.fixture
def temp_db_path():
    """Create a temporary database path for testing."""
    temp_dir = tempfile.mkdtemp()
    yield temp_dir
    shutil.rmtree(temp_dir)


def test_create_and_insert(temp_db_path):
    """Test creating a table and inserting data."""
    db = bl.connect(temp_db_path)

    db.command(
        bl.table("test_table").create(
            schema={
                "id": pl.Int64,
                "name": pl.String,
                "value": pl.Float64,
            },
            mode="replace",
        )
    )

    df = pl.DataFrame({
        "id": [1, 2, 3],
        "name": ["Alice", "Bob", "Charlie"],
        "value": [10.5, 20.3, 30.1],
    })

    db.insert(table="test_table", data=df)

    result = db.query(bl.table("test_table").select("id", "name", "value"))

    assert len(result) == 3
    assert result.columns == ["id", "name", "value"]
    assert result["id"].to_list() == [1, 2, 3]


def test_query(temp_db_path):
    """Test querying data back."""
    db = bl.connect(temp_db_path)

    db.command(
        bl.table("users").create(
            schema={
                "user_id": pl.Int64,
                "username": pl.String,
                "age": pl.Int64,
            }
        )
    )

    df = pl.DataFrame({
        "user_id": [1, 2, 3, 4],
        "username": ["alice", "bob", "charlie", "dave"],
        "age": [25, 30, 35, 40],
    })

    db.insert(table="users", data=df)

    result = db.query(bl.table("users").select("user_id", "username"))

    assert len(result) == 4
    assert result.columns == ["user_id", "username"]


def test_filter(temp_db_path):
    """Test filtering with polars expressions."""
    db = bl.connect(temp_db_path)

    db.command(
        bl.table("products").create(
            schema={
                "product_id": pl.Int64,
                "name": pl.String,
                "price": pl.Float64,
            }
        )
    )

    df = pl.DataFrame({
        "product_id": [1, 2, 3, 4],
        "name": ["Widget", "Gadget", "Gizmo", "Doohickey"],
        "price": [9.99, 19.99, 29.99, 39.99],
    })

    db.insert(table="products", data=df)

    result = db.query(
        bl.table("products")
        .filter(pl.col("price") > 15.0)
        .select("product_id", "name", "price")
    )

    assert len(result) == 3
    assert result["price"].min() > 15.0


def test_schema_validation(temp_db_path):
    """Test that wrong schema raises error."""
    db = bl.connect(temp_db_path)

    db.command(
        bl.table("strict_table").create(
            schema={
                "id": pl.Int64,
                "value": pl.Float64,
            }
        )
    )

    wrong_df = pl.DataFrame({
        "id": [1, 2],
        "wrong_column": ["a", "b"],
    })

    with pytest.raises(ValueError, match="Missing column"):
        db.insert(table="strict_table", data=wrong_df)


def test_drop_table(temp_db_path):
    """Test dropping a table."""
    db = bl.connect(temp_db_path)

    db.command(
        bl.table("temp_table").create(
            schema={"id": pl.Int64}
        )
    )

    assert db.metadata.table_exists("temp_table")

    db.command(bl.table("temp_table").drop())

    assert not db.metadata.table_exists("temp_table")


def test_example_usage(temp_db_path):
    """Test the example usage from the README."""
    db = bl.connect(temp_db_path)

    db.command(
        bl.table("stock_prices").create(
            schema={"date": pl.Date, "ticker": pl.String, "price": pl.Float64},
            partition_keys=[pl.col("date").dt.year(), "ticker"],
            mode="replace",
        )
    )

    df = pl.DataFrame({
        "date": [dt.date(2024, 1, 1), dt.date(2024, 1, 2), dt.date(2025, 1, 1)],
        "ticker": ["AAPL", "AAPL", "GOOGL"],
        "price": [150.0, 151.0, 2800.0],
    })

    db.insert(table="stock_prices", data=df)

    result = db.query(
        bl.table("stock_prices")
        .filter(pl.col("ticker") == "AAPL")
        .select("date", "ticker", "price")
    )

    assert len(result) == 2
    assert all(result["ticker"] == "AAPL")


def test_multiple_inserts(temp_db_path):
    """Test multiple inserts to the same table."""
    db = bl.connect(temp_db_path)

    db.command(
        bl.table("logs").create(
            schema={
                "timestamp": pl.Datetime,
                "message": pl.String,
            }
        )
    )

    df1 = pl.DataFrame({
        "timestamp": [dt.datetime(2024, 1, 1, 10, 0, 0)],
        "message": ["First message"],
    })

    df2 = pl.DataFrame({
        "timestamp": [dt.datetime(2024, 1, 1, 11, 0, 0)],
        "message": ["Second message"],
    })

    db.insert(table="logs", data=df1)
    db.insert(table="logs", data=df2)

    result = db.query(bl.table("logs").select("timestamp", "message"))

    assert len(result) == 2


def test_sort(temp_db_path):
    """Test sorting results."""
    db = bl.connect(temp_db_path)

    db.command(
        bl.table("scores").create(
            schema={
                "player": pl.String,
                "score": pl.Int64,
            }
        )
    )

    df = pl.DataFrame({
        "player": ["Alice", "Bob", "Charlie"],
        "score": [100, 200, 150],
    })

    db.insert(table="scores", data=df)

    result = db.query(
        bl.table("scores").sort("score").select("player", "score")
    )

    assert result["score"].to_list() == [100, 150, 200]


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
