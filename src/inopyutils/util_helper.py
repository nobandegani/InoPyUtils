import base64
import time
import hashlib
import secrets
from datetime import datetime, timezone

from typing import Any, Dict

def ino_ok(msg: str = "success", **extra: Any) -> Dict[str, Any]:
    return {"success": True, "msg": msg, **extra}

def ino_err(msg: str = "error", **extra: Any) -> Dict[str, Any]:
    return {"success": False, "msg": msg, **extra}

def ino_is_err(res: Any):
    if isinstance(res, tuple):
        if all(i is not None for i in res):
            res = res[0]
        else:
            return True
    if isinstance(res, dict):
        return not res.get("success", False)

    return False

class InoUtilHelper:
    @staticmethod
    def hash_string(s: str, algo: str = "sha256", length: int = 16) -> str:
        h = hashlib.new(algo)
        h.update(s.encode("utf-8"))
        return h.hexdigest()[:length]

    @staticmethod
    def generate_unique_id_by_time():
        """
        Generates a unique identifier based on the current time in nanoseconds.
        """
        payload = str(time.time_ns())
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()

    @staticmethod
    def get_date_time_utc_base64(random_token:int = 2) -> str:
        """
        Generates a unique, base32-encoded timestamp string combined with a random token.
        The timestamp is based on the current UTC date and time down to milliseconds,
        and the random token adds additional entropy for uniqueness.
        """
        now = datetime.now(timezone.utc)
        ts_str = now.strftime("%Y%m%d%H%M%S") + f"{now.microsecond // 1000:03d}"

        n = int(ts_str)
        b = n.to_bytes((n.bit_length() + 7) // 8 or 1, "big")

        core = base64.b32encode(b).decode("ascii").rstrip("=").lower()
        token = base64.b32encode(secrets.token_bytes(random_token)).decode("ascii").rstrip("=").lower()

        return f"{core}{token}"