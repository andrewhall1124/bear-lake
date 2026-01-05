import pytest
import polars as pl
from bear_lake import connect, table


class TestS3QueryOperations:
    """Test query functionality on S3."""

    def test_query_all_data(self, s3_db, sample_schema, sample_data):
        """Test querying all data from a table on S3."""
        s3_db.create(
            name="users",
            schema=sample_schema,
            partition_keys=["city"],
            primary_keys=["id"],
        )

        s3_db.insert("users", sample_data)

        # Query all data
        result = s3_db.query(
            pl.scan_parquet(
                f"{s3_db.path}/users/**/*.parquet",
                storage_options=s3_db.storage_options,
            )
        )
        assert len(result) == len(sample_data)

    def test_query_with_filter(self, s3_db, sample_schema, sample_data):
        """Test querying with a filter condition on S3."""
        s3_db.create(
            name="users",
            schema=sample_schema,
            partition_keys=["city"],
            primary_keys=["id"],
        )

        s3_db.insert("users", sample_data)

        # Query with filter
        result = s3_db.query(
            pl.scan_parquet(
                f"{s3_db.path}/users/**/*.parquet",
                storage_options=s3_db.storage_options,
            ).filter(pl.col("age") > 30)
        )

        assert len(result) == 2  # Charlie (35) and Eve (32)
        assert all(result["age"] > 30)

    def test_query_with_select(self, s3_db, sample_schema, sample_data):
        """Test querying with column selection on S3."""
        s3_db.create(
            name="users",
            schema=sample_schema,
            partition_keys=["city"],
            primary_keys=["id"],
        )

        s3_db.insert("users", sample_data)

        # Query with select
        result = s3_db.query(
            pl.scan_parquet(
                f"{s3_db.path}/users/**/*.parquet",
                storage_options=s3_db.storage_options,
            ).select(["name", "city"])
        )

        assert result.columns == ["name", "city"]
        assert len(result) == len(sample_data)

    def test_query_with_aggregation(self, s3_db, sample_schema, sample_data):
        """Test querying with aggregation on S3."""
        s3_db.create(
            name="users",
            schema=sample_schema,
            partition_keys=["city"],
            primary_keys=["id"],
        )

        s3_db.insert("users", sample_data)

        # Query with aggregation - count by city
        result = s3_db.query(
            pl.scan_parquet(
                f"{s3_db.path}/users/**/*.parquet",
                storage_options=s3_db.storage_options,
            )
            .group_by("city")
            .agg(pl.col("id").count().alias("count"))
            .sort("city")
        )

        # NYC: 2, LA: 2, SF: 1
        assert len(result) == 3

    def test_query_with_join(self, s3_db, sample_schema, sample_data):
        """Test querying with joins across tables on S3."""
        # Create users table
        s3_db.create(
            name="users",
            schema=sample_schema,
            partition_keys=["city"],
            primary_keys=["id"],
        )
        s3_db.insert("users", sample_data)

        # Create orders table
        orders_schema = {
            "order_id": pl.Int64,
            "user_id": pl.Int64,
            "amount": pl.Float64,
        }

        orders_data = pl.DataFrame(
            {
                "order_id": [1, 2, 3],
                "user_id": [1, 2, 1],
                "amount": [100.0, 200.0, 150.0],
            }
        )

        s3_db.create(
            name="orders",
            schema=orders_schema,
            partition_keys=["user_id"],  # Use user_id as partition key
            primary_keys=["order_id"],
        )
        s3_db.insert("orders", orders_data)

        # Query with join
        users_lf = pl.scan_parquet(
            f"{s3_db.path}/users/**/*.parquet", storage_options=s3_db.storage_options
        )
        orders_lf = pl.scan_parquet(
            f"{s3_db.path}/orders/**/*.parquet", storage_options=s3_db.storage_options
        )

        result = s3_db.query(
            users_lf.join(orders_lf, left_on="id", right_on="user_id").select(
                ["name", "order_id", "amount"]
            )
        )

        assert len(result) == 3  # 3 orders
        assert "name" in result.columns
        assert "order_id" in result.columns


class TestS3ConnectFunction:
    """Test the connect() helper function with S3."""

    def test_connect_returns_database(self, s3_db_path, s3_storage_options):
        """Test that connect returns a Database instance for S3."""
        from bear_lake import Database
        from bear_lake.filesystem_client import S3Client

        db = connect(s3_db_path, storage_options=s3_storage_options)

        assert isinstance(db, Database)
        assert db.path == s3_db_path
        assert isinstance(db.file_system_client, S3Client)

    def test_connect_sets_global_state(self, s3_db_path, s3_storage_options):
        """Test that connect sets global state for table() function with S3."""
        db = connect(s3_db_path, storage_options=s3_storage_options)

        # Import to get updated global values
        import bear_lake

        assert bear_lake.DATABASE_PATH == s3_db_path
        assert bear_lake.CONNECTED is True
        assert bear_lake.STORAGE_OPTIONS == s3_storage_options


class TestS3TableFunction:
    """Test the table() helper function with S3."""

    def test_table_function_basic(self, s3_db, sample_schema, sample_data):
        """Test the table() helper function with S3."""
        # Connect first
        db = connect(s3_db.path, storage_options=s3_db.storage_options)

        # Create and populate table
        db.create(
            name="users",
            schema=sample_schema,
            partition_keys=["city"],
            primary_keys=["id"],
        )
        db.insert("users", sample_data)

        # Use table() function
        lf = table("users")
        assert isinstance(lf, pl.LazyFrame)

        # Query using the lazy frame
        result = db.query(lf)
        assert len(result) == len(sample_data)

    def test_table_function_with_filter(self, s3_db, sample_schema, sample_data):
        """Test using table() function with filters on S3."""
        db = connect(s3_db.path, storage_options=s3_db.storage_options)

        db.create(
            name="users",
            schema=sample_schema,
            partition_keys=["city"],
            primary_keys=["id"],
        )
        db.insert("users", sample_data)

        # Use table() with filter
        result = db.query(table("users").filter(pl.col("city") == "NYC"))

        assert len(result) == 2
        assert all(result["city"] == "NYC")


class TestS3ComplexQueries:
    """Test complex query scenarios on S3."""

    def test_query_multiple_partitions(
        self, s3_db, multi_partition_schema, partitioned_data
    ):
        """Test querying across multiple partition levels on S3."""
        s3_db.create(
            name="users",
            schema=multi_partition_schema,
            partition_keys=["country", "city"],
            primary_keys=["id"],
        )

        s3_db.insert("users", partitioned_data)

        # Query specific partition
        result = s3_db.query(
            pl.scan_parquet(
                f"{s3_db.path}/users/**/*.parquet",
                storage_options=s3_db.storage_options,
            ).filter(pl.col("city") == "NYC")
        )

        nyc_count = len(partitioned_data.filter(pl.col("city") == "NYC"))
        assert len(result) == nyc_count

    def test_query_with_sorting(self, s3_db, sample_schema, sample_data):
        """Test querying with sorting on S3."""
        s3_db.create(
            name="users",
            schema=sample_schema,
            partition_keys=["city"],
            primary_keys=["id"],
        )

        s3_db.insert("users", sample_data)

        # Query with sorting
        result = s3_db.query(
            pl.scan_parquet(
                f"{s3_db.path}/users/**/*.parquet",
                storage_options=s3_db.storage_options,
            ).sort("age", descending=True)
        )

        ages = result["age"].to_list()
        assert ages == sorted(ages, reverse=True)

    def test_query_with_limit(self, s3_db, sample_schema, sample_data):
        """Test querying with limit on S3."""
        s3_db.create(
            name="users",
            schema=sample_schema,
            partition_keys=["city"],
            primary_keys=["id"],
        )

        s3_db.insert("users", sample_data)

        # Query with limit
        result = s3_db.query(
            pl.scan_parquet(
                f"{s3_db.path}/users/**/*.parquet",
                storage_options=s3_db.storage_options,
            ).limit(3)
        )

        assert len(result) == 3

    def test_query_with_expressions(self, s3_db, sample_schema, sample_data):
        """Test querying with computed expressions on S3."""
        s3_db.create(
            name="users",
            schema=sample_schema,
            partition_keys=["city"],
            primary_keys=["id"],
        )

        s3_db.insert("users", sample_data)

        # Query with expression - create age_group
        result = s3_db.query(
            pl.scan_parquet(
                f"{s3_db.path}/users/**/*.parquet",
                storage_options=s3_db.storage_options,
            ).with_columns((pl.col("age") / 10).cast(pl.Int32).alias("age_decade"))
        )

        assert "age_decade" in result.columns
        assert all(result["age_decade"].is_in([2, 3]))
