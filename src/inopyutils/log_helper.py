from pathlib import Path
from enum import Enum
import datetime
import json

from .file_helper import InoFileHelper


class LogCategory(Enum):
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"


class InoLogHelper:
    """
    A helper class to manage logging to a log file with a given base name.
    """

    def __init__(self, path_to_save: Path | str, log_name: str):
        """
        Initialize the LogHelper.

        Args:
            path_to_save (Path | str): Directory where log file will be stored.
            log_name (str): Base name for the log file (e.g., "UploadWorker").
        """

        self.path : Path = path_to_save
        self.path.mkdir(parents=True, exist_ok=True)

        get_last_log = InoFileHelper.get_last_file(self.path)
        if get_last_log["success"]:
            new_log_name = InoFileHelper.increment_batch_name(get_last_log["file"].stem)
            self.log_file = self.path / f"{new_log_name}.inolog"
        else:
            self.log_file = self.path / f"{log_name}_00001.inolog"

        self.log_file.touch(exist_ok=True)

    def add(self, log_data: dict, category: LogCategory = None) -> None:
        """
        Append a log entry to the log file in JSON-lines format.

        Args:
            category (LogCategory): Enum value denoting the log category.
            log_data (dict): Dictionary of log details to record.
        """
        if category is None:
            if isinstance(log_data, dict) and "success" in log_data:
                category = LogCategory.INFO if log_data.get("success") else LogCategory.ERROR
            else:
                category = LogCategory.INFO

        entry = {
            "timestamp": datetime.datetime.now().isoformat(),
            "category": category.value,
            "data": log_data
        }

        with self.log_file.open("a", encoding="utf-8") as f:
            f.write(json.dumps(entry, ensure_ascii=False) + "\n")
