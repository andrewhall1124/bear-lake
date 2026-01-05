import s3fs
from .base import FileSystemClient


class S3Client(FileSystemClient):
    """Filesystem client for S3 storage"""

    def __init__(self, storage_options: dict[str, str]):
        self.fs = s3fs.S3FileSystem(
            key=storage_options["aws_access_key_id"],
            secret=storage_options["aws_secret_access_key"],
            endpoint_url=storage_options["endpoint_url"],
            client_kwargs={"region_name": storage_options["region"]},
        )

    def exists(self, path: str) -> bool:
        return self.fs.exists(path)

    def makedirs(self, path: str) -> None:
        # S3 doesn't need directory creation
        pass

    def remove(self, path: str, recursive: bool = False) -> None:
        # Strip s3:// prefix if present for s3fs
        path = path.replace("s3://", "")
        self.fs.rm(path, recursive=recursive)

    def open(self, path: str, mode: str):
        return self.fs.open(path, mode)

    def glob(self, pattern: str) -> list[str]:
        # s3fs.glob returns paths without s3:// prefix, so add it back
        return [f"s3://{f}" for f in self.fs.glob(pattern)]

    def listdir(self, path: str) -> list[str]:
        try:
            # s3fs.ls returns paths without s3:// prefix, so add it back
            return [f"s3://{f}" for f in self.fs.ls(path)]
        except FileNotFoundError:
            return []

    def isdir(self, path: str) -> bool:
        # For S3, check if metadata.json exists in the path
        # This is a heuristic since S3 doesn't have true directories
        return self.fs.exists(f"{path}/metadata.json")
