import zipfile
import asyncio
from pathlib import Path

class InoFileHelper:
    @staticmethod
    async def unzip(zip_path: Path, output_path: Path) -> dict:
        output_path.mkdir(parents=True, exist_ok=True)

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
