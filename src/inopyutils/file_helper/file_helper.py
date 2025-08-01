import zipfile
import asyncio
from pathlib import Path

class InoFileHelper:
    @staticmethod
    async def unzip(zip_path: Path, output_path: Path) -> dict:
        output_path.mkdir(parents=True, exist_ok=True)
        if not zip_path.is_file():
            return {
                "success": False,
                "msg": f"{zip_path.name} is not a file"
            }

        if not zip_path.suffix == ".zip":
            return {
                "success": False,
                "msg": f"{zip_path.name} is not a zip file"
            }

        def _extract():
            with zipfile.ZipFile(zip_path, "r") as zf:
                zf.extractall(output_path)

        try:
            await asyncio.to_thread(_extract)
            extracted_files = list(output_path.rglob("*"))
            if not extracted_files:
                return {
                    "success": False,
                    "msg": f"No files found after extracting {zip_path.name}"
                }
            return {
                "success": True,
                "output_path": str(output_path),
                "files_extracted": len(extracted_files)
            }

        except zipfile.BadZipFile:
            return {
                "success": False,
                "msg": f"{zip_path.name} is not a valid zip file"
            }
        except Exception as e:
            return {
                "success": False,
                "msg": f"Error extracting {zip_path.name}: {e}"
            }

    @staticmethod
    async def remove_file(file_path: Path) -> dict:
        if not file_path.exists():
            return {
                "success": False,
                "msg": f"{file_path.name} not exist"
            }

        if not file_path.is_file():
            return {
                "success": False,
                "msg": f"{file_path.name} is not a file"
            }

        try:
            await asyncio.to_thread(file_path.unlink)
        except Exception as e:
            return {
                "success": False,
                "msg": f"⚠️ Failed to delete {file_path}: {e}"
            }

        return {
            "success": True,
            "msg": f"File {file_path.name} deleted"
        }