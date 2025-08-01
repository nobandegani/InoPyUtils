import zipfile
import asyncio
import shutil
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

    @staticmethod
    async def remove_folder(folder_path: Path) -> dict:
        if not folder_path.exists():
            return {
                "success": False,
                "msg": f"{folder_path.name} not exist"
            }

        if not folder_path.is_dir():
            return {
                "success": False,
                "msg": f"{folder_path.name} is not a directory"
            }

        try:
            await asyncio.to_thread(shutil.rmtree, folder_path)
        except Exception as e:
            return {
                "success": False,
                "msg": f"⚠️ Failed to delete {folder_path}: {e}"
            }

        return {
            "success": True,
            "msg": f"File {folder_path.name} deleted"
        }

    @staticmethod
    def copy_files(
            from_path: Path,
            to_path: Path,
            iterate_subfolders: bool = True,
            rename_files: bool = True,
            prefix_name: str = "File"
    ) -> dict:
        to_path.mkdir(parents=True, exist_ok=True)

        log_file = to_path.parent / "copy_log.txt"
        log_lines = []

        if iterate_subfolders:
            files = [f for f in from_path.rglob("*") if f.is_file()]
        else:
            files = [f for f in from_path.iterdir() if f.is_file()]

        error = False
        for idx, file in enumerate(files, start=1):
            if not file.is_file():
                log_lines.append(f"⚠️ {file}: not a file")
                continue

            ext = file.suffix.lower()
            if ext == "":
                log_lines.append(f"⚠️ file with no extension: {file.name}")
                ext = file.name

            if rename_files:
                new_name = f"{prefix_name}_{idx:03}{ext}"
            else:
                if not file.stem.strip():
                    log_lines.append(f"⚠️ Empty or invalid filename detected: {file.name}")
                    new_name = f"unnamed_{idx:03}{ext}"
                else:
                    new_name = file.name

            dest = to_path / new_name
            if dest.exists():
                log_lines.append(f"⚠️ target file trying to copy to is already exist: {dest}")

            print(f"Coping: {file} → {dest}")
            try:
                shutil.copy2(file, dest)
                log_lines.append(f"✅ Copied: {file.resolve()} => {dest.resolve()}")
            except Exception as e:
                log_lines.append(f"❌ Failed to copy {file} → {dest} — {e}")
                error = True

        if log_lines:
            with open(log_file, "w", encoding="utf-8") as log:
                log.write("\n".join(log_lines))

        if error:
            return {
                "success": False,
                "msg": f"Failed to copy files, see log file: {log_file}"
            }

        return {
            "success": True,
            "msg": f"📂 Coping and renaming files completed"
        }