import json

class InoJsonHelper:
    @staticmethod
    def string_to_dict(json_string: str) -> dict:
        return json.loads(json_string)

    @staticmethod
    def is_valid(json_string: str) -> bool:
        try:
            json.loads(json_string)
            return True
        except json.JSONDecodeError:
            return False

