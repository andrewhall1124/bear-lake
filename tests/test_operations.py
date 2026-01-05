import pytest
import polars as pl
import os
import glob
from bear_lake import Database


class TestInsertOperations:
    """Test data insertion functionality."""

    def test_insert_basic(self, db, sample_schema, sample_data):
        """Test basic data insertion."""
        db.create(
            name="users",
            schema=sample_schema,
            partition_keys=["city"],
            primary_keys=["id"],
        )

        db.insert("users", sample_data)

        # Verify parquet files were created
        table_path = os.path.join(db.path, "users")
        parquet_files = glob.glob(f"{table_path}/**/*.parquet", recursive=True)
        assert len(parquet_files) > 0

    def test_insert_creates_partitions(self, db, sample_schema, sample_data):
        """Test that insert creates correct partition structure."""
        db.create(
            name="users",
            schema=sample_schema,
            partition_keys=["city"],
            primary_keys=["id"],
        )

        db.insert("users", sample_data)

        # Check for partition directories
        table_path = os.path.join(db.path, "users")

        # Should have partitions for NYC, LA, SF
        cities = sample_data["city"].unique().to_list()
        for city in cities:
            partition_file = os.path.join(table_path, f"{city}.parquet")
            assert os.path.exists(partition_file), f"Partition for {city} not found"

    def test_insert_append_mode(self, db, sample_schema, sample_data):
        """Test appending data to existing partitions."""
        db.create(
            name="users",
            schema=sample_schema,
            partition_keys=["city"],
            primary_keys=["id"],
        )

        # First insert
        db.insert("users", sample_data)

        # Create new data to append
        new_data = pl.DataFrame(
            {
                "id": [6, 7],
                "name": ["Frank", "Grace"],
                "age": [40, 29],
                "city": ["NYC", "LA"],
            }
        )

        # Append
        db.insert("users", new_data, mode="append")

        # Query to verify
        result = db.query(pl.scan_parquet(f"{db.path}/users/**/*.parquet"))
        assert len(result) == 7  # 5 original + 2 new

    def test_insert_overwrite_mode(self, db, sample_schema, sample_data):
        """Test overwriting data in existing partitions."""
        db.create(
            name="users",
            schema=sample_schema,
            partition_keys=["city"],
            primary_keys=["id"],
        )

        # First insert
        db.insert("users", sample_data)

        # Create new data to overwrite NYC partition
        new_data = pl.DataFrame(
            {
                "id": [100],
                "name": ["NewUser"],
                "age": [50],
                "city": ["NYC"],
            }
        )

        # Overwrite
        db.insert("users", new_data, mode="overwrite")

        # Query NYC partition
        result = db.query(
            pl.scan_parquet(f"{db.path}/users/**/*.parquet").filter(
                pl.col("city") == "NYC"
            )
        )

        # Should only have the new record for NYC
        assert len(result) == 1
        assert result["id"][0] == 100

    def test_insert_error_mode(self, db, sample_schema, sample_data):
        """Test that inserting to existing partition raises error in error mode."""
        db.create(
            name="users",
            schema=sample_schema,
            partition_keys=["city"],
            primary_keys=["id"],
        )

        # First insert
        db.insert("users", sample_data)

        # Try to insert again with error mode
        new_data = pl.DataFrame(
            {
                "id": [6],
                "name": ["Frank"],
                "age": [40],
                "city": ["NYC"],
            }
        )

        with pytest.raises(FileExistsError):
            db.insert("users", new_data, mode="error")

    def test_insert_multiple_partition_keys(
        self, db, multi_partition_schema, partitioned_data
    ):
        """Test insertion with multiple partition keys."""
        db.create(
            name="users",
            schema=multi_partition_schema,
            partition_keys=["country", "city"],
            primary_keys=["id"],
        )

        db.insert("users", partitioned_data)

        # Verify nested partition structure
        table_path = os.path.join(db.path, "users")

        # Should have USA directory with city subdirectories
        usa_path = os.path.join(table_path, "USA")
        assert os.path.exists(usa_path)

        # Check for city partition files
        parquet_files = glob.glob(f"{table_path}/**/*.parquet", recursive=True)
        assert len(parquet_files) > 0


class TestDeleteOperations:
    """Test data deletion functionality."""

    def test_delete_basic(self, db, sample_schema, sample_data):
        """Test basic row deletion."""
        db.create(
            name="users",
            schema=sample_schema,
            partition_keys=["city"],
            primary_keys=["id"],
        )

        db.insert("users", sample_data)

        # Delete users with age > 30
        db.delete("users", pl.col("age") > 30)

        # Query remaining data
        result = db.query(pl.scan_parquet(f"{db.path}/users/**/*.parquet"))

        # Should have 3 users left (ages 25, 30, 28)
        assert len(result) == 3
        assert all(result["age"] <= 30)

    def test_delete_removes_empty_partitions(self, db, sample_schema, sample_data):
        """Test that delete removes empty partition files."""
        db.create(
            name="users",
            schema=sample_schema,
            partition_keys=["city"],
            primary_keys=["id"],
        )

        db.insert("users", sample_data)

        # Delete all users from SF
        db.delete("users", pl.col("city") == "SF")

        # Verify SF partition file is removed
        table_path = os.path.join(db.path, "users")
        sf_partition = os.path.join(table_path, "SF.parquet")
        assert not os.path.exists(sf_partition)

    def test_delete_all_rows(self, db, sample_schema, sample_data):
        """Test deleting all rows from a table."""
        db.create(
            name="users",
            schema=sample_schema,
            partition_keys=["city"],
            primary_keys=["id"],
        )

        db.insert("users", sample_data)

        # Delete all rows
        db.delete("users", pl.col("id") >= 0)

        # Verify no parquet files remain
        table_path = os.path.join(db.path, "users")
        parquet_files = glob.glob(f"{table_path}/**/*.parquet", recursive=True)
        assert len(parquet_files) == 0


class TestOptimizeOperations:
    """Test table optimization functionality."""

    def test_optimize_deduplicates(self, db, sample_schema):
        """Test that optimize removes duplicate primary keys."""
        db.create(
            name="users",
            schema=sample_schema,
            partition_keys=["city"],
            primary_keys=["id"],
        )

        # Insert data with duplicates
        data_v1 = pl.DataFrame(
            {
                "id": [1, 2, 3],
                "name": ["Alice_v1", "Bob_v1", "Charlie_v1"],
                "age": [25, 30, 35],
                "city": ["NYC", "NYC", "NYC"],
            }
        )

        data_v2 = pl.DataFrame(
            {
                "id": [1, 2],  # Duplicate ids
                "name": ["Alice_v2", "Bob_v2"],  # Updated names
                "age": [26, 31],  # Updated ages
                "city": ["NYC", "NYC"],
            }
        )

        db.insert("users", data_v1, mode="append")
        db.insert("users", data_v2, mode="append")

        # Before optimization, should have 5 rows
        result_before = db.query(pl.scan_parquet(f"{db.path}/users/**/*.parquet"))
        assert len(result_before) == 5

        # Optimize
        db.optimize("users")

        # After optimization, should have 3 rows (duplicates removed, keeping last)
        result_after = db.query(pl.scan_parquet(f"{db.path}/users/**/*.parquet"))
        assert len(result_after) == 3

        # Verify the latest versions are kept
        alice = result_after.filter(pl.col("id") == 1)
        assert alice["name"][0] == "Alice_v2"
        assert alice["age"][0] == 26

    def test_optimize_sorts_by_primary_keys(self, db, sample_schema):
        """Test that optimize sorts data by primary keys."""
        db.create(
            name="users",
            schema=sample_schema,
            partition_keys=["city"],
            primary_keys=["id"],
        )

        # Insert unsorted data
        unsorted_data = pl.DataFrame(
            {
                "id": [3, 1, 2],
                "name": ["Charlie", "Alice", "Bob"],
                "age": [35, 25, 30],
                "city": ["NYC", "NYC", "NYC"],
            }
        )

        db.insert("users", unsorted_data)

        # Optimize
        db.optimize("users")

        # Verify data is sorted
        result = db.query(pl.scan_parquet(f"{db.path}/users/**/*.parquet"))
        assert result["id"].to_list() == [1, 2, 3]

    def test_optimize_empty_primary_keys(self, db, sample_schema):
        """Test optimization with no primary keys."""
        db.create(
            name="users",
            schema=sample_schema,
            partition_keys=["city"],
            primary_keys=[],  # No primary keys
        )

        data = pl.DataFrame(
            {
                "id": [1, 1, 2],  # Duplicates
                "name": ["Alice", "Alice", "Bob"],
                "age": [25, 25, 30],
                "city": ["NYC", "NYC", "NYC"],
            }
        )

        db.insert("users", data)

        # Optimize should not fail even without primary keys
        db.optimize("users")

        # Data should still be present
        result = db.query(pl.scan_parquet(f"{db.path}/users/**/*.parquet"))
        assert len(result) == 3  # No deduplication without primary keys
