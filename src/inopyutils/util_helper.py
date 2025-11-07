import hashlib

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