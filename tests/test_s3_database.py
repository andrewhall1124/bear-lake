import pytest
import polars as pl
import json
from bear_lake import Database


class TestS3DatabaseInitialization:
    """Test Database initialization and setup with S3."""

    def test_database_init_s3(self, s3_db_path, s3_storage_options):
        """Test that database initializes correctly for S3 storage."""
        from bear_lake.filesystem_client import S3Client

        db = Database(s3_db_path, storage_options=s3_storage_options)
        assert db.path == s3_db_path
        assert isinstance(db.file_system_client, S3Client)
        assert db.storage_options == s3_storage_options


class TestS3TableCreation:
    """Test table creation functionality on S3."""

    def test_create_table_basic(self, s3_db, sample_schema):
        """Test basic table creation on S3."""
        s3_db.create(
            name="users",
            schema=sample_schema,
            partition_keys=["city"],
            primary_keys=["id"],
        )

        # Verify metadata file exists on S3
        metadata_path = f"{s3_db.path}/users/metadata.json"
        assert s3_db.file_system_client.exists(metadata_path)

        # Verify metadata content
        with s3_db.file_system_client.open(metadata_path, "r") as f:
            metadata = json.load(f)

        assert metadata["name"] == "users"
        assert metadata["partition_keys"] == ["city"]
        assert metadata["primary_keys"] == ["id"]
        assert "id" in metadata["schema"]
        assert "name" in metadata["schema"]

    def test_create_table_already_exists_error_mode(self, s3_db, sample_schema):
        """Test that creating an existing table raises error in error mode on S3."""
        s3_db.create(
            name="users",
            schema=sample_schema,
            partition_keys=["city"],
            primary_keys=["id"],
        )

        with pytest.raises(FileExistsError, match="Table 'users' already exists"):
            s3_db.create(
                name="users",
                schema=sample_schema,
                partition_keys=["city"],
                primary_keys=["id"],
                mode="error",
            )

    def test_create_table_skip_mode(self, s3_db, sample_schema):
        """Test that creating an existing table is skipped in skip mode on S3."""
        s3_db.create(
            name="users",
            schema=sample_schema,
            partition_keys=["city"],
            primary_keys=["id"],
        )

        # Should not raise error
        s3_db.create(
            name="users",
            schema=sample_schema,
            partition_keys=["city"],
            primary_keys=["id"],
            mode="skip",
        )

    def test_create_table_replace_mode(self, s3_db, sample_schema, sample_data):
        """Test that creating an existing table replaces it in replace mode on S3."""
        # Create table and insert data
        s3_db.create(
            name="users",
            schema=sample_schema,
            partition_keys=["city"],
            primary_keys=["id"],
        )
        s3_db.insert("users", sample_data)

        # Replace table with new schema
        new_schema = {
            "id": pl.Int64,
            "email": pl.String,
        }
        s3_db.create(
            name="users",
            schema=new_schema,
            partition_keys=[],
            primary_keys=["id"],
            mode="replace",
        )

        # Verify new metadata
        metadata_path = f"{s3_db.path}/users/metadata.json"
        with s3_db.file_system_client.open(metadata_path, "r") as f:
            metadata = json.load(f)

        assert "email" in metadata["schema"]
        assert "name" not in metadata["schema"]

    def test_create_table_invalid_mode(self, s3_db, sample_schema):
        """Test that invalid mode raises ValueError when table exists on S3."""
        # First create the table
        s3_db.create(
            name="users",
            schema=sample_schema,
            partition_keys=["city"],
            primary_keys=["id"],
        )

        # Then try to create again with invalid mode
        with pytest.raises(ValueError, match="Invalid mode"):
            s3_db.create(
                name="users",
                schema=sample_schema,
                partition_keys=["city"],
                primary_keys=["id"],
                mode="invalid",
            )


class TestS3TableMetadata:
    """Test metadata retrieval functions on S3."""

    def test_get_schema(self, s3_db, sample_schema):
        """Test retrieving table schema from S3."""
        s3_db.create(
            name="users",
            schema=sample_schema,
            partition_keys=["city"],
            primary_keys=["id"],
        )

        schema = s3_db.get_schema("users")
        assert schema["id"] == pl.Int64
        assert schema["name"] == pl.String
        assert schema["age"] == pl.Int32
        assert schema["city"] == pl.String

    def test_get_partition_keys(self, s3_db, sample_schema):
        """Test retrieving partition keys from S3."""
        s3_db.create(
            name="users",
            schema=sample_schema,
            partition_keys=["city", "age"],
            primary_keys=["id"],
        )

        partition_keys = s3_db.get_partition_keys("users")
        assert partition_keys == ["city", "age"]

    def test_get_primary_keys(self, s3_db, sample_schema):
        """Test retrieving primary keys from S3."""
        s3_db.create(
            name="users",
            schema=sample_schema,
            partition_keys=["city"],
            primary_keys=["id", "name"],
        )

        primary_keys = s3_db.get_primary_keys("users")
        assert primary_keys == ["id", "name"]


class TestS3ListTables:
    """Test listing tables functionality on S3."""

    def test_list_tables_empty(self, s3_db):
        """Test listing tables when S3 database is empty."""
        tables = s3_db.list_tables()
        assert tables == []

    def test_list_tables_single(self, s3_db, sample_schema):
        """Test listing tables with one table on S3."""
        s3_db.create(
            name="users",
            schema=sample_schema,
            partition_keys=["city"],
            primary_keys=["id"],
        )

        tables = s3_db.list_tables()
        assert len(tables) == 1
        assert "users" in tables

    def test_list_tables_multiple(self, s3_db, sample_schema):
        """Test listing tables with multiple tables on S3."""
        s3_db.create(
            name="users",
            schema=sample_schema,
            partition_keys=["city"],
            primary_keys=["id"],
        )

        s3_db.create(
            name="orders",
            schema={"order_id": pl.Int64, "user_id": pl.Int64},
            partition_keys=["user_id"],
            primary_keys=["order_id"],
        )

        tables = s3_db.list_tables()
        assert len(tables) == 2
        assert "users" in tables
        assert "orders" in tables


class TestS3DropTable:
    """Test table dropping functionality on S3."""

    def test_drop_table(self, s3_db, sample_schema, sample_data):
        """Test dropping a table from S3."""
        s3_db.create(
            name="users",
            schema=sample_schema,
            partition_keys=["city"],
            primary_keys=["id"],
        )
        s3_db.insert("users", sample_data)

        # Verify table exists
        assert "users" in s3_db.list_tables()

        table_path = f"{s3_db.path}/users"
        assert s3_db.file_system_client.exists(table_path)

        # Drop table
        s3_db.drop("users")

        # Verify table is gone
        assert "users" not in s3_db.list_tables()
        assert not s3_db.file_system_client.exists(table_path)
