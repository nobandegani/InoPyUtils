from __future__ import annotations

from io import StringIO
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Union

import aiofiles
import csv

from .util_helper import ino_ok, ino_err


class InoCsvHelper:
    """Utility helper for working with CSV files.

    Design goals:
    - Always expose CSV data as a list[dict[str, Any]] (rows as dicts)
    - Async read/write using aiofiles
    - Convenience utilities for headers, row/column access, and sorting
    """

    # -----------------------
    # File IO (async)
    # -----------------------
    @staticmethod
    async def read_csv_from_file_async(
        file_path: str,
        *,
        encoding: str = "utf-8",
        delimiter: str = ",",
    ) -> Dict[str, Any]:
        """Read a CSV file asynchronously and return rows as list[dict] with headers.

        Returns ino_ok(data={"rows": rows, "headers": headers}) or ino_err(...)
        """
        try:
            async with aiofiles.open(file_path, mode="r", encoding=encoding, newline="") as f:
                content = await f.read()

            sio = StringIO(content)
            reader = csv.DictReader(sio, delimiter=delimiter)
            rows: List[Dict[str, Any]] = [dict(row) for row in reader]
            headers: List[str] = list(reader.fieldnames or [])

            return ino_ok("read csv successful", data={"rows": rows, "headers": headers})
        except FileNotFoundError:
            return ino_err(f"File not found: {file_path}")
        except Exception as e:
            return ino_err(f"Error reading CSV: {e}")

    @staticmethod
    async def save_csv_to_file_async(
        rows: Sequence[Dict[str, Any]],
        file_path: str,
        *,
        headers: Optional[Sequence[str]] = None,
        encoding: str = "utf-8",
        delimiter: str = ",",
        include_headers: bool = True,
    ) -> Dict[str, Any]:
        """Write rows (list[dict]) to a CSV file asynchronously.

        - headers can be provided; if not, they are inferred from rows in a stable order
        - returns ino_ok/ino_err
        """
        try:
            # Normalize rows to list of dicts
            norm_rows: List[Dict[str, Any]] = [dict(r) for r in rows]

            if headers is None:
                headers = InoCsvHelper.get_headers(norm_rows)

            if headers is None or len(headers) == 0:
                return ino_err("No headers to write. Provide headers or non-empty rows.")

            # Use StringIO + csv module, then write the text via aiofiles
            sio = StringIO()
            writer = csv.DictWriter(sio, fieldnames=list(headers), delimiter=delimiter, lineterminator="\n")
            if include_headers:
                writer.writeheader()
            for row in norm_rows:
                writer.writerow({k: row.get(k, "") for k in headers})

            text = sio.getvalue()

            path = Path(file_path)
            path.parent.mkdir(parents=True, exist_ok=True)

            async with aiofiles.open(path, mode="w", encoding=encoding, newline="") as f:
                await f.write(text)

            return ino_ok("save csv successful")
        except Exception as e:
            return ino_err(f"Error writing CSV: {e}")

    # -----------------------
    # In-memory utilities
    # -----------------------
    @staticmethod
    def get_headers(rows: Sequence[Dict[str, Any]]) -> List[str]:
        """Infer headers from rows preserving first-seen order.

        If rows is empty, returns []. If rows have differing keys, union is used in
        first-seen order across all rows.
        """
        seen = []
        seen_set = set()
        for row in rows:
            for k in row.keys():
                if k not in seen_set:
                    seen.append(k)
                    seen_set.add(k)
        return seen

    @staticmethod
    def get_row(rows: Sequence[Dict[str, Any]], index: int, default: Optional[Dict[str, Any]] = None) -> Optional[Dict[str, Any]]:
        """Get a single row by index; returns default if out of range."""
        if -len(rows) <= index < len(rows):
            return dict(rows[index])  # return a shallow copy
        return default

    @staticmethod
    def get_column(rows: Sequence[Dict[str, Any]], column_name: str) -> List[Any]:
        """Extract a column by name as list of values (missing keys become None)."""
        return [row.get(column_name) for row in rows]

    @staticmethod
    def sort_rows(
        rows: Sequence[Dict[str, Any]],
        by: Union[str, Sequence[str]],
        *,
        reverse: bool = False,
        missing_last: bool = True,
    ) -> List[Dict[str, Any]]:
        """Return a new list of rows sorted by one or multiple keys.

        - by: column name or list/tuple of column names
        - missing_last: If True, rows with missing keys are placed at the end
        """
        keys: List[str] = [by] if isinstance(by, str) else list(by)

        def sort_key(row: Dict[str, Any]):
            parts = []
            for k in keys:
                v = row.get(k, None)
                if missing_last:
                    # (is_missing, value) so that missing (True) sorts after present (False)
                    parts.append((v is None, v))
                else:
                    parts.append(v)
            return tuple(parts)

        return [dict(r) for r in sorted(rows, key=sort_key, reverse=reverse)]