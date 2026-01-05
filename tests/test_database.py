import pytest
import polars as pl
import os
import json
from bear_lake import Database


class TestDatabaseInitialization:
    """Test Database initialization and setup."""

    def test_database_init_local(self, temp_db_path):
        """Test that database initializes correctly for local storage."""
        from bear_lake.filesystem_client import LocalClient

        db = Database(temp_db_path)
        assert db.path == temp_db_path
        assert isinstance(db.file_system_client, LocalClient)
        assert db.storage_options is None
        assert os.path.exists(temp_db_path)

    def test_database_init_creates_directory(self, temp_db_path):
        """Test that database creates directory if it doesn't exist."""
        new_path = os.path.join(temp_db_path, "new_db")
        db = Database(new_path)
        assert os.path.exists(new_path)


class TestTableCreation:
    """Test table creation functionality."""

    def test_create_table_basic(self, db, sample_schema):
        """Test basic table creation."""
        db.create(
            name="users",
            schema=sample_schema,
            partition_keys=["city"],
            primary_keys=["id"],
        )

        # Verify metadata file exists
        metadata_path = os.path.join(db.path, "users", "metadata.json")
        assert os.path.exists(metadata_path)

        # Verify metadata content
        with open(metadata_path, "r") as f:
            metadata = json.load(f)

        assert metadata["name"] == "users"
        assert metadata["partition_keys"] == ["city"]
        assert metadata["primary_keys"] == ["id"]
        assert "id" in metadata["schema"]
        assert "name" in metadata["schema"]

    def test_create_table_already_exists_error_mode(self, db, sample_schema):
        """Test that creating an existing table raises error in error mode."""
        db.create(
            name="users",
            schema=sample_schema,
            partition_keys=["city"],
            primary_keys=["id"],
        )

        with pytest.raises(FileExistsError, match="Table 'users' already exists"):
            db.create(
                name="users",
                schema=sample_schema,
                partition_keys=["city"],
                primary_keys=["id"],
                mode="error",
            )

    def test_create_table_skip_mode(self, db, sample_schema):
        """Test that creating an existing table is skipped in skip mode."""
        db.create(
            name="users",
            schema=sample_schema,
            partition_keys=["city"],
            primary_keys=["id"],
        )

        # Should not raise error
        db.create(
            name="users",
            schema=sample_schema,
            partition_keys=["city"],
            primary_keys=["id"],
            mode="skip",
        )

    def test_create_table_replace_mode(self, db, sample_schema, sample_data):
        """Test that creating an existing table replaces it in replace mode."""
        # Create table and insert data
        db.create(
            name="users",
            schema=sample_schema,
            partition_keys=["city"],
            primary_keys=["id"],
        )
        db.insert("users", sample_data)

        # Replace table with new schema
        new_schema = {
            "id": pl.Int64,
            "email": pl.String,
        }
        db.create(
            name="users",
            schema=new_schema,
            partition_keys=[],
            primary_keys=["id"],
            mode="replace",
        )

        # Verify new metadata
        metadata_path = os.path.join(db.path, "users", "metadata.json")
        with open(metadata_path, "r") as f:
            metadata = json.load(f)

        assert "email" in metadata["schema"]
        assert "name" not in metadata["schema"]

    def test_create_table_invalid_mode(self, db, sample_schema):
        """Test that invalid mode raises ValueError when table exists."""
        # First create the table
        db.create(
            name="users",
            schema=sample_schema,
            partition_keys=["city"],
            primary_keys=["id"],
        )

        # Then try to create again with invalid mode
        with pytest.raises(ValueError, match="Invalid mode"):
            db.create(
                name="users",
                schema=sample_schema,
                partition_keys=["city"],
                primary_keys=["id"],
                mode="invalid",
            )


class TestTableMetadata:
    """Test metadata retrieval functions."""

    def test_get_schema(self, db, sample_schema):
        """Test retrieving table schema."""
        db.create(
            name="users",
            schema=sample_schema,
            partition_keys=["city"],
            primary_keys=["id"],
        )

        schema = db.get_schema("users")
        assert schema["id"] == pl.Int64
        assert schema["name"] == pl.String
        assert schema["age"] == pl.Int32
        assert schema["city"] == pl.String

    def test_get_partition_keys(self, db, sample_schema):
        """Test retrieving partition keys."""
        db.create(
            name="users",
            schema=sample_schema,
            partition_keys=["city", "age"],
            primary_keys=["id"],
        )

        partition_keys = db.get_partition_keys("users")
        assert partition_keys == ["city", "age"]

    def test_get_primary_keys(self, db, sample_schema):
        """Test retrieving primary keys."""
        db.create(
            name="users",
            schema=sample_schema,
            partition_keys=["city"],
            primary_keys=["id", "name"],
        )

        primary_keys = db.get_primary_keys("users")
        assert primary_keys == ["id", "name"]


class TestListTables:
    """Test listing tables functionality."""

    def test_list_tables_empty(self, db):
        """Test listing tables when database is empty."""
        tables = db.list_tables()
        assert tables == []

    def test_list_tables_single(self, db, sample_schema):
        """Test listing tables with one table."""
        db.create(
            name="users",
            schema=sample_schema,
            partition_keys=["city"],
            primary_keys=["id"],
        )

        tables = db.list_tables()
        assert len(tables) == 1
        assert "users" in tables

    def test_list_tables_multiple(self, db, sample_schema):
        """Test listing tables with multiple tables."""
        db.create(
            name="users",
            schema=sample_schema,
            partition_keys=["city"],
            primary_keys=["id"],
        )

        db.create(
            name="orders",
            schema={"order_id": pl.Int64, "user_id": pl.Int64},
            partition_keys=[],
            primary_keys=["order_id"],
        )

        tables = db.list_tables()
        assert len(tables) == 2
        assert "users" in tables
        assert "orders" in tables


class TestDropTable:
    """Test table dropping functionality."""

    def test_drop_table(self, db, sample_schema, sample_data):
        """Test dropping a table."""
        db.create(
            name="users",
            schema=sample_schema,
            partition_keys=["city"],
            primary_keys=["id"],
        )
        db.insert("users", sample_data)

        # Verify table exists
        assert "users" in db.list_tables()
        table_path = os.path.join(db.path, "users")
        assert os.path.exists(table_path)

        # Drop table
        db.drop("users")

        # Verify table is gone
        assert "users" not in db.list_tables()
        assert not os.path.exists(table_path)
