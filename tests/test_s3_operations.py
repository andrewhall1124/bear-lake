import pytest
import polars as pl


class TestS3InsertOperations:
    """Test data insertion functionality on S3."""

    def test_insert_basic(self, s3_db, sample_schema, sample_data):
        """Test basic data insertion on S3."""
        s3_db.create(
            name="users",
            schema=sample_schema,
            partition_keys=["city"],
            primary_keys=["id"],
        )

        s3_db.insert("users", sample_data)

        # Verify parquet files were created on S3
        fs = s3_db._get_fs()
        table_path = f"{s3_db.path.replace('s3://', '')}/users"
        parquet_files = fs.glob(f"{table_path}/**/*.parquet")
        assert len(parquet_files) > 0

    def test_insert_creates_partitions(self, s3_db, sample_schema, sample_data):
        """Test that insert creates correct partition structure on S3."""
        s3_db.create(
            name="users",
            schema=sample_schema,
            partition_keys=["city"],
            primary_keys=["id"],
        )

        s3_db.insert("users", sample_data)

        # Check for partition files on S3
        fs = s3_db._get_fs()
        table_path = f"{s3_db.path.replace('s3://', '')}/users"

        # Should have partitions for NYC, LA, SF
        cities = sample_data["city"].unique().to_list()
        for city in cities:
            partition_file = f"{table_path}/{city}.parquet"
            assert fs.exists(partition_file), f"Partition for {city} not found on S3"

    def test_insert_append_mode(self, s3_db, sample_schema, sample_data):
        """Test appending data to existing partitions on S3."""
        s3_db.create(
            name="users",
            schema=sample_schema,
            partition_keys=["city"],
            primary_keys=["id"],
        )

        # First insert
        s3_db.insert("users", sample_data)

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
        s3_db.insert("users", new_data, mode="append")

        # Query to verify
        result = s3_db.query(
            pl.scan_parquet(
                f"{s3_db.path}/users/**/*.parquet",
                storage_options=s3_db.storage_options,
            )
        )
        assert len(result) == 7  # 5 original + 2 new

    def test_insert_overwrite_mode(self, s3_db, sample_schema, sample_data):
        """Test overwriting data in existing partitions on S3."""
        s3_db.create(
            name="users",
            schema=sample_schema,
            partition_keys=["city"],
            primary_keys=["id"],
        )

        # First insert
        s3_db.insert("users", sample_data)

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
        s3_db.insert("users", new_data, mode="overwrite")

        # Query NYC partition
        result = s3_db.query(
            pl.scan_parquet(
                f"{s3_db.path}/users/**/*.parquet",
                storage_options=s3_db.storage_options,
            ).filter(pl.col("city") == "NYC")
        )

        # Should only have the new record for NYC
        assert len(result) == 1
        assert result["id"][0] == 100

    def test_insert_error_mode(self, s3_db, sample_schema, sample_data):
        """Test that inserting to existing partition raises error in error mode on S3."""
        s3_db.create(
            name="users",
            schema=sample_schema,
            partition_keys=["city"],
            primary_keys=["id"],
        )

        # First insert
        s3_db.insert("users", sample_data)

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
            s3_db.insert("users", new_data, mode="error")

    def test_insert_multiple_partition_keys(
        self, s3_db, multi_partition_schema, partitioned_data
    ):
        """Test insertion with multiple partition keys on S3."""
        s3_db.create(
            name="users",
            schema=multi_partition_schema,
            partition_keys=["country", "city"],
            primary_keys=["id"],
        )

        s3_db.insert("users", partitioned_data)

        # Verify nested partition structure on S3
        fs = s3_db._get_fs()
        table_path = f"{s3_db.path.replace('s3://', '')}/users"

        # Should have USA directory with city subdirectories
        usa_path = f"{table_path}/USA"
        assert fs.exists(usa_path)

        # Check for parquet files
        parquet_files = fs.glob(f"{table_path}/**/*.parquet")
        assert len(parquet_files) > 0


class TestS3DeleteOperations:
    """Test data deletion functionality on S3."""

    def test_delete_basic(self, s3_db, sample_schema, sample_data):
        """Test basic row deletion on S3."""
        s3_db.create(
            name="users",
            schema=sample_schema,
            partition_keys=["city"],
            primary_keys=["id"],
        )

        s3_db.insert("users", sample_data)

        # Delete users with age > 30
        s3_db.delete("users", pl.col("age") > 30)

        # Query remaining data
        result = s3_db.query(
            pl.scan_parquet(
                f"{s3_db.path}/users/**/*.parquet",
                storage_options=s3_db.storage_options,
            )
        )

        # Should have 3 users left (ages 25, 30, 28)
        assert len(result) == 3
        assert all(result["age"] <= 30)

    def test_delete_removes_empty_partitions(self, s3_db, sample_schema, sample_data):
        """Test that delete removes empty partition files on S3."""
        s3_db.create(
            name="users",
            schema=sample_schema,
            partition_keys=["city"],
            primary_keys=["id"],
        )

        s3_db.insert("users", sample_data)

        # Delete all users from SF
        s3_db.delete("users", pl.col("city") == "SF")

        # Verify SF partition file is removed on S3
        fs = s3_db._get_fs()
        table_path = f"{s3_db.path.replace('s3://', '')}/users"
        sf_partition = f"{table_path}/SF.parquet"
        assert not fs.exists(sf_partition)

    def test_delete_all_rows(self, s3_db, sample_schema, sample_data):
        """Test deleting all rows from a table on S3."""
        s3_db.create(
            name="users",
            schema=sample_schema,
            partition_keys=["city"],
            primary_keys=["id"],
        )

        s3_db.insert("users", sample_data)

        # Delete all rows
        s3_db.delete("users", pl.col("id") >= 0)

        # Verify no parquet files remain on S3
        fs = s3_db._get_fs()
        table_path = f"{s3_db.path.replace('s3://', '')}/users"
        parquet_files = fs.glob(f"{table_path}/**/*.parquet")
        assert len(parquet_files) == 0


class TestS3OptimizeOperations:
    """Test table optimization functionality on S3."""

    def test_optimize_deduplicates(self, s3_db, sample_schema):
        """Test that optimize removes duplicate primary keys on S3."""
        s3_db.create(
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

        s3_db.insert("users", data_v1, mode="append")
        s3_db.insert("users", data_v2, mode="append")

        # Before optimization, should have 5 rows
        result_before = s3_db.query(
            pl.scan_parquet(
                f"{s3_db.path}/users/**/*.parquet",
                storage_options=s3_db.storage_options,
            )
        )
        assert len(result_before) == 5

        # Optimize
        s3_db.optimize("users")

        # After optimization, should have 3 rows (duplicates removed, keeping last)
        result_after = s3_db.query(
            pl.scan_parquet(
                f"{s3_db.path}/users/**/*.parquet",
                storage_options=s3_db.storage_options,
            )
        )
        assert len(result_after) == 3

        # Verify the latest versions are kept
        alice = result_after.filter(pl.col("id") == 1)
        assert alice["name"][0] == "Alice_v2"
        assert alice["age"][0] == 26

    def test_optimize_sorts_by_primary_keys(self, s3_db, sample_schema):
        """Test that optimize sorts data by primary keys on S3."""
        s3_db.create(
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

        s3_db.insert("users", unsorted_data)

        # Optimize
        s3_db.optimize("users")

        # Verify data is sorted
        result = s3_db.query(
            pl.scan_parquet(
                f"{s3_db.path}/users/**/*.parquet",
                storage_options=s3_db.storage_options,
            )
        )
        assert result["id"].to_list() == [1, 2, 3]

    def test_optimize_empty_primary_keys(self, s3_db, sample_schema):
        """Test optimization with no primary keys on S3."""
        s3_db.create(
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

        s3_db.insert("users", data)

        # Optimize should not fail even without primary keys
        s3_db.optimize("users")

        # Data should still be present
        result = s3_db.query(
            pl.scan_parquet(
                f"{s3_db.path}/users/**/*.parquet",
                storage_options=s3_db.storage_options,
            )
        )
        assert len(result) == 3  # No deduplication without primary keys
