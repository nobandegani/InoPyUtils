import hashlib

class InoUtilHelper:
    @staticmethod
    def hash_string(s: str, algo: str = "sha256", length: int = 16) -> str:
        h = hashlib.new(algo)
        h.update(s.encode("utf-8"))
        return h.hexdigest()[:length]