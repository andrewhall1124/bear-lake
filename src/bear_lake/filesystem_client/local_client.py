import os
import shutil
import glob
from .base import FileSystemClient


class LocalClient(FileSystemClient):
    """Filesystem client for local storage"""

    def exists(self, path: str) -> bool:
        return os.path.exists(path)

    def makedirs(self, path: str) -> None:
        os.makedirs(path, exist_ok=True)

    def remove(self, path: str, recursive: bool = False) -> None:
        if recursive:
            shutil.rmtree(path)
        else:
            os.remove(path)

    def open(self, path: str, mode: str):
        return open(path, mode)

    def glob(self, pattern: str) -> list[str]:
        return glob.glob(pattern, recursive=True)

    def listdir(self, path: str) -> list[str]:
        if not os.path.exists(path):
            return []
        return os.listdir(path)

    def isdir(self, path: str) -> bool:
        return os.path.isdir(path)
