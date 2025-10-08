import json
import aiofiles
from typing import Union, Dict

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

    @staticmethod
    async def save_string_as_json(json_string: str, file_path: str) -> Dict:
        """Save a JSON string to a file asynchronously."""
        try:
            json_data = json.loads(json_string)
            
            async with aiofiles.open(file_path, 'w', encoding='utf-8') as file:
                await file.write(json.dumps(json_data, indent=2, ensure_ascii=False))
            
            return {"status": True, "msg": ""}
        except json.JSONDecodeError as e:
            return {"status": False, "msg": f"Invalid JSON string: {str(e)}"}
        except Exception as e:
            return {"status": False, "msg": f"Error saving file: {str(e)}"}

    @staticmethod
    async def save_json_as_json(json_data: Union[dict, list], file_path: str) -> Dict:
        """Save a JSON object (dict or list) to a file asynchronously."""
        try:
            async with aiofiles.open(file_path, 'w', encoding='utf-8') as file:
                await file.write(json.dumps(json_data, indent=2, ensure_ascii=False))
            
            return {"status": True, "msg": ""}
        except Exception as e:
            return {"status": False, "msg": f"Error saving file: {str(e)}"}

    @staticmethod
    async def read_json_from_file(file_path: str) -> Dict:
        """Read JSON data from a file asynchronously."""
        try:
            async with aiofiles.open(file_path, 'r', encoding='utf-8') as file:
                content = await file.read()
                json_data = json.loads(content)
            
            return {"status": True, "msg": "", "data": json_data}
        except FileNotFoundError:
            return {"status": False, "msg": f"File not found: {file_path}", "data": None}
        except json.JSONDecodeError as e:
            return {"status": False, "msg": f"Invalid JSON in file: {str(e)}", "data": None}
        except Exception as e:
            return {"status": False, "msg": f"Error reading file: {str(e)}", "data": None}

