import pytest
import polars as pl
import tempfile
import shutil
import os
import uuid
from pathlib import Path
from dotenv import load_dotenv
from bear_lake import Database

# Load environment variables from .env file in project root
project_root = Path(__file__).parent.parent
load_dotenv(project_root / ".env")


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


# S3 Test Fixtures
@pytest.fixture
def s3_storage_options():
    """Return S3 storage options from environment variables."""
    return {
        "aws_access_key_id": os.getenv("ACCESS_KEY_ID"),
        "aws_secret_access_key": os.getenv("SECRET_ACCESS_KEY"),
        "endpoint_url": os.getenv("ENDPOINT"),
        "region": os.getenv("REGION", "auto"),
    }


@pytest.fixture
def s3_test_bucket():
    """Return the S3 test bucket name from environment variables."""
    bucket = os.getenv("BUCKET")
    if not bucket:
        pytest.skip("BUCKET not set in environment variables")
    return bucket


@pytest.fixture
def s3_db_path(s3_test_bucket):
    """Create a unique S3 path for testing."""
    # Use UUID to create unique test path
    test_id = str(uuid.uuid4())[:8]
    return f"s3://{s3_test_bucket}/bear_lake_test_{test_id}"


@pytest.fixture
def s3_db(s3_db_path, s3_storage_options):
    """Create a Database instance with S3 storage."""
    # Check if credentials are available
    if not all(s3_storage_options.values()):
        pytest.skip("S3 credentials not available in environment variables")

    db = Database(s3_db_path, storage_options=s3_storage_options)
    yield db

    # Cleanup after test - delete the test directory
    try:
        import s3fs

        fs = s3fs.S3FileSystem(
            key=s3_storage_options["aws_access_key_id"],
            secret=s3_storage_options["aws_secret_access_key"],
            endpoint_url=s3_storage_options["endpoint_url"],
            client_kwargs={"region_name": s3_storage_options["region"]},
        )
        # Remove s3:// prefix for s3fs
        path = s3_db_path.replace("s3://", "")
        if fs.exists(path):
            fs.rm(path, recursive=True)
    except Exception:
        pass  # Best effort cleanup
