from pathlib import Path
from enum import Enum
import datetime
import json
import os
import asyncio
import aiofiles

from .file_helper import InoFileHelper


class LogType(Enum):
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
        self.max_file_size_bytes = max_file_size_mb * 1024 * 1024

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
            current_file = get_last_log["file"]
            if current_file.suffix == ".inolog" and current_file.stat().st_size < self.max_file_size_bytes:
                self.log_file = current_file
            else:
                new_log_name = InoFileHelper.increment_batch_name(current_file.stem)
                self.log_file = self.path / f"{new_log_name}.inolog"
        else:
            self.log_file = self.path / f"{self.log_name}_00001.inolog"

        if not self.log_file.exists():
            async with aiofiles.open(self.log_file, 'w', encoding='utf-8') as f:
                pass

    async def add(self, log_type: LogType = None, msg: str = "", log_data: dict = None, source: str = None) -> None:
        """
        Append a log entry to the log file in JSON-lines format with comprehensive metadata.

        Args:
            log_data (dict): Dictionary of log details to record.
            msg (str): Message to record along with the log details.
            log_type (LogCategory): Enum value denoting the log category.
            source (str): Optional source identifier (function, class, module name).
        """

        await self._ensure_initialized()

        if self.log_file.exists() and self.log_file.stat().st_size >= self.max_file_size_bytes:
            await self._create_log_file()

        if log_type is None:
            if isinstance(log_data, dict) and "success" in log_data:
                category = LogType.INFO if log_data.get("success") else LogType.ERROR
            else:
                category = LogType.INFO

        now = datetime.datetime.now()
        entry = {
            "timestamp": now.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3],
            "iso_timestamp": now.isoformat(),
            "type": log_type.value,
            "source": source,
            "msg": msg,
            "data": log_data
        }

        #entry = {k: v for k, v in entry.items() if v is not None}

        async with aiofiles.open(self.log_file, "a", encoding="utf-8") as f:
            await f.write(json.dumps(entry, ensure_ascii=False, default=str) + "\n")

    async def debug(self, msg: str = "", log_data: dict = None, source: str = None) -> None:
        """Convenience method for DEBUG level logs."""
        await self.add(LogType.DEBUG, msg, log_data, source)

    async def info(self, msg: str = "", log_data: dict = None, source: str = None) -> None:
        """Convenience method for INFO level logs."""
        await self.add(LogType.INFO, msg, log_data, source)

    async def warning(self, msg: str = "", log_data: dict = None, source: str = None) -> None:
        """Convenience method for WARNING level logs."""
        await self.add(LogType.INFO, msg, log_data, source)

    async def error(self, msg: str = "", log_data: dict = None, source: str = None) -> None:
        """Convenience method for ERROR level logs."""
        await self.add(LogType.INFO, msg, log_data, source)

    async def critical(self, msg: str = "", log_data: dict = None, source: str = None) -> None:
        """Convenience method for CRITICAL level logs."""
        await self.add(LogType.INFO, msg, log_data, source)

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
