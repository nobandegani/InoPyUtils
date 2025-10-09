import json
from pathlib import Path

import aiofiles
from typing import Union, Dict, Any, Optional, List
from copy import deepcopy

class InoJsonHelper:
    @staticmethod
    def string_to_dict(json_string: str) -> Dict:
        """Convert JSON string to dictionary with proper error handling."""
        try:
            return {"success": True, "msg": "JSON string successfully converted to dictionary", "data": json.loads(json_string)}
        except json.JSONDecodeError as e:
            return {"success": False, "msg": f"Invalid JSON string: {str(e)}", "data": None}
        except Exception as e:
            return {"success": False, "msg": f"Error parsing JSON: {str(e)}", "data": None}

    @staticmethod
    def dict_to_string(json_data: Union[dict, list, Any], indent: Optional[int] = None, ensure_ascii: bool = False) -> Dict:
        """Convert dictionary/list/any JSON-serializable object to JSON string."""
        try:
            json_string = json.dumps(json_data, indent=indent, ensure_ascii=ensure_ascii)
            return {"success": True, "msg": "Data successfully converted to JSON string", "data": json_string}
        except TypeError as e:
            return {"success": False, "msg": f"Object not JSON serializable: {str(e)}", "data": None}
        except Exception as e:
            return {"success": False, "msg": f"Error converting to JSON string: {str(e)}", "data": None}

    @staticmethod
    def is_valid(json_string: str) -> bool:
        """Check if a string is valid JSON."""
        try:
            json.loads(json_string)
            return True
        except json.JSONDecodeError:
            return False

    @staticmethod
    async def save_string_as_json(json_string: str, file_path: str) -> Dict:
        """Save a JSON string to a file asynchronously."""
        try:
            file_path = Path(file_path)
            file_path.parent.mkdir(parents=True, exist_ok=True)

            json_data = json.loads(json_string)
            
            async with aiofiles.open(file_path, 'w', encoding='utf-8') as file:
                await file.write(json.dumps(json_data, indent=2, ensure_ascii=False))
            
            return {"success": True, "msg": "save json successful"}
        except json.JSONDecodeError as e:
            return {"success": False, "msg": f"Invalid JSON string: {str(e)}"}
        except Exception as e:
            return {"success": False, "msg": f"Error saving file: {str(e)}"}

    @staticmethod
    async def save_json_as_json(json_data: Union[dict, list], file_path: str) -> Dict:
        """Save a JSON object (dict or list) to a file asynchronously."""
        try:
            file_path = Path(file_path)
            file_path.parent.mkdir(parents=True, exist_ok=True)

            async with aiofiles.open(file_path, 'w', encoding='utf-8') as file:
                await file.write(json.dumps(json_data, indent=2, ensure_ascii=False))
            
            return {"success": True, "msg": "save json successful"}
        except Exception as e:
            return {"success": False, "msg": f"Error saving file: {str(e)}"}

    @staticmethod
    async def read_json_from_file(file_path: str) -> Dict:
        """Read JSON data from a file asynchronously."""
        try:
            async with aiofiles.open(file_path, 'r', encoding='utf-8') as file:
                content = await file.read()
                json_data = json.loads(content)
            
            return {"success": True, "msg": "JSON file successfully read and parsed", "data": json_data}
        except FileNotFoundError:
            return {"success": False, "msg": f"File not found: {file_path}", "data": None}
        except json.JSONDecodeError as e:
            return {"success": False, "msg": f"Invalid JSON in file: {str(e)}", "data": None}
        except Exception as e:
            return {"success": False, "msg": f"Error reading file: {str(e)}", "data": None}

    @staticmethod
    def pretty_print(json_data: Union[dict, list, Any], indent: int = 2) -> Dict:
        """Pretty print JSON data with proper formatting."""
        try:
            formatted = json.dumps(json_data, indent=indent, ensure_ascii=False, sort_keys=True)
            return {"success": True, "msg": "JSON data successfully formatted", "data": formatted}
        except Exception as e:
            return {"success": False, "msg": f"Error formatting JSON: {str(e)}", "data": None}

    @staticmethod
    def minify(json_data: Union[dict, list, Any]) -> Dict:
        """Minify JSON data by removing all whitespace."""
        try:
            minified = json.dumps(json_data, separators=(',', ':'), ensure_ascii=False)
            return {"success": True, "msg": "JSON data successfully minified", "data": minified}
        except Exception as e:
            return {"success": False, "msg": f"Error minifying JSON: {str(e)}", "data": None}

    @staticmethod
    def deep_merge(dict1: dict, dict2: dict) -> Dict:
        """Deep merge two dictionaries, with dict2 values taking precedence."""
        try:
            result = deepcopy(dict1)
            
            def _merge(target, source):
                for key, value in source.items():
                    if key in target and isinstance(target[key], dict) and isinstance(value, dict):
                        _merge(target[key], value)
                    else:
                        target[key] = value
            
            _merge(result, dict2)
            return {"success": True, "msg": "Dictionaries successfully merged", "data": result}
        except Exception as e:
            return {"success": False, "msg": f"Error merging dictionaries: {str(e)}", "data": None}

    @staticmethod
    def safe_get(json_data: dict, path: str, default: Any = None, separator: str = ".") -> Any:
        """Safely get a value from nested JSON using dot notation path."""
        try:
            keys = path.split(separator)
            current = json_data
            
            for key in keys:
                if isinstance(current, dict) and key in current:
                    current = current[key]
                elif isinstance(current, list) and key.isdigit() and int(key) < len(current):
                    current = current[int(key)]
                else:
                    return default
            
            return current
        except Exception:
            return default

    @staticmethod
    def safe_set(json_data: dict, path: str, value: Any, separator: str = ".") -> Dict:
        """Safely set a value in nested JSON using dot notation path."""
        try:
            keys = path.split(separator)
            current = json_data
            
            for key in keys[:-1]:
                if key not in current:
                    current[key] = {}
                current = current[key]
            
            current[keys[-1]] = value
            return {"success": True, "msg": "Value successfully set in JSON data", "data": json_data}
        except Exception as e:
            return {"success": False, "msg": f"Error setting value: {str(e)}", "data": None}

    @staticmethod
    def flatten(json_data: dict, separator: str = ".") -> Dict:
        """Flatten a nested JSON object into a single level dictionary."""
        try:
            def _flatten(obj, parent_key="", sep="."):
                items = []
                if isinstance(obj, dict):
                    for k, v in obj.items():
                        new_key = f"{parent_key}{sep}{k}" if parent_key else k
                        if isinstance(v, dict):
                            items.extend(_flatten(v, new_key, sep=sep).items())
                        elif isinstance(v, list):
                            for i, item in enumerate(v):
                                items.extend(_flatten(item, f"{new_key}{sep}{i}", sep=sep).items())
                        else:
                            items.append((new_key, v))
                else:
                    items.append((parent_key, obj))
                return dict(items)
            
            flattened = _flatten(json_data, sep=separator)
            return {"success": True, "msg": "JSON data successfully flattened", "data": flattened}
        except Exception as e:
            return {"success": False, "msg": f"Error flattening JSON: {str(e)}", "data": None}

    @staticmethod
    def unflatten(flat_data: dict, separator: str = ".") -> Dict:
        """Unflatten a flattened dictionary back to nested structure."""
        try:
            result = {}
            for key, value in flat_data.items():
                keys = key.split(separator)
                current = result
                
                for k in keys[:-1]:
                    if k not in current:
                        current[k] = {}
                    current = current[k]
                
                current[keys[-1]] = value
            
            return {"success": True, "msg": "Flattened data successfully unflattened", "data": result}
        except Exception as e:
            return {"success": False, "msg": f"Error unflattening data: {str(e)}", "data": None}

    @staticmethod
    def compare(json1: Union[dict, list], json2: Union[dict, list]) -> Dict:
        """Compare two JSON objects and return differences."""
        try:
            def _compare(obj1, obj2, path=""):
                differences = []
                
                if type(obj1) != type(obj2):
                    differences.append({
                        "path": path or "root",
                        "type": "type_mismatch",
                        "value1": type(obj1).__name__,
                        "value2": type(obj2).__name__
                    })
                    return differences
                
                if isinstance(obj1, dict):
                    all_keys = set(obj1.keys()) | set(obj2.keys())
                    for key in all_keys:
                        current_path = f"{path}.{key}" if path else key
                        if key not in obj1:
                            differences.append({
                                "path": current_path,
                                "type": "missing_in_first",
                                "value": obj2[key]
                            })
                        elif key not in obj2:
                            differences.append({
                                "path": current_path,
                                "type": "missing_in_second",
                                "value": obj1[key]
                            })
                        else:
                            differences.extend(_compare(obj1[key], obj2[key], current_path))
                
                elif isinstance(obj1, list):
                    max_len = max(len(obj1), len(obj2))
                    for i in range(max_len):
                        current_path = f"{path}[{i}]" if path else f"[{i}]"
                        if i >= len(obj1):
                            differences.append({
                                "path": current_path,
                                "type": "missing_in_first",
                                "value": obj2[i]
                            })
                        elif i >= len(obj2):
                            differences.append({
                                "path": current_path,
                                "type": "missing_in_second",
                                "value": obj1[i]
                            })
                        else:
                            differences.extend(_compare(obj1[i], obj2[i], current_path))
                
                else:
                    if obj1 != obj2:
                        differences.append({
                            "path": path or "root",
                            "type": "value_difference",
                            "value1": obj1,
                            "value2": obj2
                        })
                
                return differences
            
            differences = _compare(json1, json2)
            return {
                "success": True,
                "msg": "JSON objects successfully compared", 
                "data": {
                    "are_equal": len(differences) == 0,
                    "differences": differences
                }
            }
        except Exception as e:
            return {"success": False, "msg": f"Error comparing JSON objects: {str(e)}", "data": None}

    @staticmethod
    def filter_keys(json_data: dict, keys_to_keep: List[str], deep: bool = False) -> Dict:
        """Filter JSON object to keep only specified keys."""
        try:
            def _filter_deep(obj, keys):
                if isinstance(obj, dict):
                    filtered = {}
                    for key, value in obj.items():
                        if key in keys:
                            filtered[key] = _filter_deep(value, keys) if deep else value
                        elif deep:
                            filtered[key] = _filter_deep(value, keys)
                    return filtered
                elif isinstance(obj, list):
                    return [_filter_deep(item, keys) for item in obj]
                else:
                    return obj
            
            if deep:
                filtered_data = _filter_deep(json_data, keys_to_keep)
            else:
                filtered_data = {k: v for k, v in json_data.items() if k in keys_to_keep}
            
            return {"success": True, "msg": "JSON data successfully filtered", "data": filtered_data}
        except Exception as e:
            return {"success": False, "msg": f"Error filtering keys: {str(e)}", "data": None}

    @staticmethod
    def remove_null_values(json_data: Union[dict, list], remove_empty: bool = False) -> Dict:
        """Remove null values (and optionally empty values) from JSON data."""
        try:
            def _clean(obj):
                if isinstance(obj, dict):
                    cleaned = {}
                    for key, value in obj.items():
                        cleaned_value = _clean(value)
                        if cleaned_value is not None:
                            if not remove_empty or (
                                cleaned_value != "" and 
                                cleaned_value != [] and 
                                cleaned_value != {}
                            ):
                                cleaned[key] = cleaned_value
                    return cleaned
                elif isinstance(obj, list):
                    cleaned = []
                    for item in obj:
                        cleaned_item = _clean(item)
                        if cleaned_item is not None:
                            if not remove_empty or (
                                cleaned_item != "" and 
                                cleaned_item != [] and 
                                cleaned_item != {}
                            ):
                                cleaned.append(cleaned_item)
                    return cleaned
                else:
                    return obj
            
            cleaned_data = _clean(json_data)
            return {"success": True, "msg": "Null values successfully removed from JSON data", "data": cleaned_data}
        except Exception as e:
            return {"success": False, "msg": f"Error removing null values: {str(e)}", "data": None}

    @staticmethod
    def find_field_from_array(json_data: Union[dict, list, Any], field_name: str, field_value: Any) -> dict:
        """Find all objects in JSON data that contain a specific field name with matching value."""
        try:
            matches = []
            
            def _search(obj):
                if isinstance(obj, dict):
                    # Check if this dict has the field with matching value
                    if field_name in obj and obj[field_name] == field_value:
                        matches.append(obj)
                    # Recursively search nested values
                    for value in obj.values():
                        _search(value)
                elif isinstance(obj, list):
                    # Search each item in the list
                    for item in obj:
                        _search(item)
                # For primitive types, no need to search further
            
            _search(json_data)
            
            return {
                "success": True,
                "msg": f"Found {len(matches)} objects with field '{field_name}' = '{field_value}'",
                "first_match": matches[0] if len(matches) > 0 else None,
                "matches": matches
            }
        except Exception as e:
            return {"success": False, "msg": f"Error searching for field: {str(e)}", "data": None}
