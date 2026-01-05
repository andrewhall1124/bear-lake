from abc import ABC, abstractmethod
from typing import Any


class FileSystemClient(ABC):
    """Abstract base class for filesystem operations"""

    @abstractmethod
    def exists(self, path: str) -> bool:
        """Check if a file or directory exists"""
        pass

    @abstractmethod
    def makedirs(self, path: str) -> None:
        """Create directories (no-op for S3)"""
        pass

    @abstractmethod
    def remove(self, path: str, recursive: bool = False) -> None:
        """Remove a file or directory"""
        pass

    @abstractmethod
    def open(self, path: str, mode: str) -> Any:
        """Open a file for reading or writing"""
        pass

    @abstractmethod
    def glob(self, pattern: str) -> list[str]:
        """Find files matching a pattern"""
        pass

    @abstractmethod
    def listdir(self, path: str) -> list[str]:
        """List contents of a directory"""
        pass

    @abstractmethod
    def isdir(self, path: str) -> bool:
        """Check if path is a directory"""
        pass
