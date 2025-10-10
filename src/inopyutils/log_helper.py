from pathlib import Path
from enum import Enum
import datetime
import json
import os
import asyncio
import aiofiles

from .file_helper import InoFileHelper


class LogCategory(Enum):
    DEBUG = "DEBUG"
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"


class InoLogHelper:
    """
    A comprehensive async logging helper that saves logs to files with proper categorization,
    timestamps, and automatic file rotation.
    """

    def __init__(self, path_to_save: Path | str, log_name: str, max_file_size_mb: int = 10):
        """
        Initialize the LogHelper with enhanced features.

        Args:
            path_to_save (Path | str): Directory where log file will be stored.
            log_name (str): Base name for the log file (e.g., "UploadWorker").
            max_file_size_mb (int): Maximum size in MB before rotating to new file (default: 10MB).
        """

        self.path = Path(path_to_save) if isinstance(path_to_save, str) else path_to_save
        self.path.mkdir(parents=True, exist_ok=True)
        
        self.log_name = log_name
        self.max_file_size_bytes = max_file_size_mb * 1024 * 1024  # Convert MB to bytes
        
        # Initialize log file (will be set by async_init)
        self.log_file = None
        self._initialized = False

    @classmethod
    async def create(cls, path_to_save: Path | str, log_name: str, max_file_size_mb: int = 10):
        """
        Async factory method to create and initialize InoLogHelper.
        
        Args:
            path_to_save (Path | str): Directory where log file will be stored.
            log_name (str): Base name for the log file (e.g., "UploadWorker").
            max_file_size_mb (int): Maximum size in MB before rotating to new file (default: 10MB).
            
        Returns:
            InoLogHelper: Fully initialized async log helper instance.
        """
        instance = cls(path_to_save, log_name, max_file_size_mb)
        await instance._create_log_file()
        instance._initialized = True
        return instance

    async def _ensure_initialized(self):
        """Ensure the log helper is properly initialized."""
        if not self._initialized:
            await self._create_log_file()
            self._initialized = True

    async def _create_log_file(self):
        """Create or rotate to a new log file."""
        get_last_log = InoFileHelper.get_last_file(self.path)
        if get_last_log["success"]:
            # Check if current file needs rotation
            current_file = get_last_log["file"]
            if current_file.suffix == ".inolog" and current_file.stat().st_size < self.max_file_size_bytes:
                # Use existing file if it's not too large
                self.log_file = current_file
            else:
                # Create new file with incremented name
                new_log_name = InoFileHelper.increment_batch_name(current_file.stem)
                self.log_file = self.path / f"{new_log_name}.inolog"
        else:
            # Create first log file
            self.log_file = self.path / f"{self.log_name}_00001.inolog"

        # Create file asynchronously
        if not self.log_file.exists():
            async with aiofiles.open(self.log_file, 'w', encoding='utf-8') as f:
                pass  # Just create the file

    async def add(self, log_data: dict, msg: str = "", category: LogCategory = None, source: str = None) -> None:
        """
        Append a log entry to the log file in JSON-lines format with comprehensive metadata.

        Args:
            log_data (dict): Dictionary of log details to record.
            msg (str): Message to record along with the log details.
            category (LogCategory): Enum value denoting the log category.
            source (str): Optional source identifier (function, class, module name).
        """

        # Ensure the log helper is initialized
        await self._ensure_initialized()

        # Check if file rotation is needed
        if self.log_file.exists() and self.log_file.stat().st_size >= self.max_file_size_bytes:
            await self._create_log_file()

        # Auto-detect category if not provided
        if category is None:
            if isinstance(log_data, dict) and "success" in log_data:
                category = LogCategory.INFO if log_data.get("success") else LogCategory.ERROR
            else:
                category = LogCategory.INFO

        # Create comprehensive log entry
        now = datetime.datetime.now()
        entry = {
            "timestamp": now.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3],  # Include milliseconds
            "iso_timestamp": now.isoformat(),  # Keep ISO format for compatibility
            "category": category.value,
            "level": category.name,  # Add level name for clarity
            "logger": self.log_name,  # Include logger source
            "source": source,  # Optional source identifier
            "process_id": os.getpid(),  # Include process ID
            "msg": msg,
            "data": log_data
        }

        # Remove None values to keep logs clean
        entry = {k: v for k, v in entry.items() if v is not None}

        # Write to file asynchronously
        async with aiofiles.open(self.log_file, "a", encoding="utf-8") as f:
            await f.write(json.dumps(entry, ensure_ascii=False, default=str) + "\n")

    async def debug(self, log_data: dict, msg: str = "", source: str = None) -> None:
        """Convenience method for DEBUG level logs."""
        await self.add(log_data, msg, LogCategory.DEBUG, source)

    async def info(self, log_data: dict, msg: str = "", source: str = None) -> None:
        """Convenience method for INFO level logs."""
        await self.add(log_data, msg, LogCategory.INFO, source)

    async def warning(self, log_data: dict, msg: str = "", source: str = None) -> None:
        """Convenience method for WARNING level logs."""
        await self.add(log_data, msg, LogCategory.WARNING, source)

    async def error(self, log_data: dict, msg: str = "", source: str = None) -> None:
        """Convenience method for ERROR level logs."""
        await self.add(log_data, msg, LogCategory.ERROR, source)

    async def critical(self, log_data: dict, msg: str = "", source: str = None) -> None:
        """Convenience method for CRITICAL level logs."""
        await self.add(log_data, msg, LogCategory.CRITICAL, source)

    def get_log_file_path(self) -> Path:
        """Get the current log file path."""
        return self.log_file

    def get_log_stats(self) -> dict:
        """Get statistics about the current log file."""
        if not self.log_file.exists():
            return {"exists": False}
        
        stat = self.log_file.stat()
        return {
            "exists": True,
            "path": str(self.log_file),
            "size_bytes": stat.st_size,
            "size_mb": round(stat.st_size / (1024 * 1024), 2),
            "created": datetime.datetime.fromtimestamp(stat.st_ctime).isoformat(),
            "modified": datetime.datetime.fromtimestamp(stat.st_mtime).isoformat()
        }
