from .database import Database
from .filesystem_client import LocalClient, S3Client

# Global state for connection
DATABASE_PATH = ""
CONNECTED = False
STORAGE_OPTIONS = None


def connect(path: str) -> Database:
    """Connect to a local file system database.

    Args:
        path: The local file system path to the database.

    Returns:
        Database instance configured for local file system.
    """
    global DATABASE_PATH, CONNECTED, STORAGE_OPTIONS

    DATABASE_PATH = path
    CONNECTED = True
    STORAGE_OPTIONS = None

    # Create local file system client
    file_system_client = LocalClient()
    file_system_client.makedirs(path)

    return Database(path, file_system_client, storage_options=None)


def connect_s3(path: str, storage_options: dict[str, str]) -> Database:
    """Connect to an S3 database.

    Args:
        path: The S3 path to the database (e.g., 's3://bucket/path').
        storage_options: S3 connection configuration including credentials.

    Returns:
        Database instance configured for S3.
    """
    global DATABASE_PATH, CONNECTED, STORAGE_OPTIONS

    DATABASE_PATH = path
    CONNECTED = True
    STORAGE_OPTIONS = storage_options

    # Create S3 file system client
    file_system_client = S3Client(storage_options)

    return Database(path, file_system_client, storage_options)
