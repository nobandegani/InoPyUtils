import hashlib

from typing import Any, Dict

def ok(msg: str = "success", **extra: Any) -> Dict[str, Any]:
    return {"success": True, "msg": msg, **extra}

def err(msg: str = "error", **extra: Any) -> Dict[str, Any]:
    return {"success": False, "msg": msg, **extra}


class InoUtilHelper:
    @staticmethod
    def hash_string(s: str, algo: str = "sha256", length: int = 16) -> str:
        h = hashlib.new(algo)
        h.update(s.encode("utf-8"))
        return h.hexdigest()[:length]