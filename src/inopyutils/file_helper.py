import zipfile
import asyncio, os
import json
import shutil
import re
from pathlib import Path
from .media_helper import InoMediaHelper

class InoFileHelper:
    @staticmethod
    def increment_batch_name(name: str) -> str:
        """
        Given a string ending in digits, increments that number
        and preserves leading zeros. If no trailing digits, returns name unchanged.
        """
        m = re.match(r'^(.*?)(\d+)$', name)
        if not m:
            return name

        prefix, numstr = m.group(1), m.group(2)
        width = len(numstr)
        new_num = int(numstr) + 1
        return f"{prefix}{new_num:0{width}d}"

    @staticmethod
    def get_last_file(path: Path) -> dict:
        """
        Return the most recently modified file in `path`.
        Returns:
            {
              "success": bool,
              "file": str,      # full path to the file (when success=True)
              "modified": float,# timestamp of last modification
              "msg": str
            }
        """
        if not path.exists() or not path.is_dir():
            return {
                "success": False,
                "msg": f"❌ Path not found or not a directory: {path}"
            }

        files = [p for p in path.iterdir() if p.is_file()]
        if not files:
            return {
                "success": False,
                "msg": f"❌ No files found in directory: {path}"
            }

        last_file = max(files, key=lambda p: p.stat().st_mtime)
        mtime = last_file.stat().st_mtime

        return {
            "success": True,
            "file": last_file,
            "modified": mtime,
            "msg": f"✅ Last file is '{last_file.name}' (modified {mtime})"
        }

    @staticmethod
    async def zip(
            to_zip: Path,
            path_to_save: Path,
            zip_file_name: str,
            compression_method: int = 8,
            compression_level: int = 5,
            include_root: bool = True
    ) -> dict:
        """
                Zip a file or folder `to_zip` into `path_to_save/zip_file_name`.

                Args:
                    to_zip: path to file or directory to compress
                    path_to_save: directory where the .zip will be written
                    zip_file_name: name of the .zip file (with .zip extension)
                    compression_method: ZIP_STORED = 0, ZIP_DEFLATED = 8, ZIP_BZIP2 = 12, ZIP_LZMA = 14
                    compression_level: integer 0–9 for zlib compression level, or 5 for default.
                    include_root: if True and `to_zip` is a directory, include the top-level
                                  folder in the archive; if False, only include its contents.

                Returns:
                    {
                      "success": bool,
                      "msg": str,
                      "original_size": int,     # bytes before compression
                      "zipped_size": int        # bytes after compression
                    }
        """

        if not to_zip.exists():
            return {"success": False, "msg": f"❌ Path not found: {to_zip}"}

        path_to_save.mkdir(parents=True, exist_ok=True)
        out_path = path_to_save / zip_file_name

        def _do_zip():
            total_in = 0
            with zipfile.ZipFile(out_path, "w", compression=compression_method, compresslevel=compression_level) as zf:
                if to_zip.is_file():
                    total_in += to_zip.stat().st_size
                    zf.write(to_zip, arcname=to_zip.name)
                else:
                    base = to_zip.parent if include_root else to_zip
                    for file in to_zip.rglob("*"):
                        if not file.is_file():
                            continue
                        total_in += file.stat().st_size
                        arc = file.relative_to(base)
                        zf.write(file, arcname=str(arc))
            return total_in

        try:
            original_size = await asyncio.to_thread(_do_zip)
            zipped_size = out_path.stat().st_size
            return {
                "success": True,
                "msg": (
                    f"✅ Zipped '{to_zip.name}' to '{out_path}': "
                    f"{original_size} → {zipped_size} bytes"
                ),
                "original_size": original_size,
                "zipped_size": zipped_size,
            }
        except RuntimeError as re:
            return {
                "success": False,
                "msg": f"❌ Error zipping '{to_zip}': {re}"
            }
        except Exception as e:
            return {
                "success": False,
                "msg": f"❌ Error zipping '{to_zip}': {e}"
            }

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
    async def move_file(from_path: Path, to_path: Path) -> dict:
        if not from_path.exists():
            return {
                "success": False,
                "msg": f"{from_path.name} not exist"
            }

        if not from_path.is_file():
            return {
                "success": False,
                "msg": f"{from_path.name} is not a file"
            }

        if not to_path.parent.exists():
            to_path.parent.mkdir(parents=True, exist_ok=True)

        try:
            await asyncio.to_thread(shutil.move, str(from_path.resolve()), str(to_path.resolve()))
            return {
                "success": True,
                "msg": f"File {from_path} moved to {to_path}"
            }
        except Exception as e:
            return {
                "success": False,
                "msg": f"⚠️ Failed to move {from_path.name}: {e}"
            }

    @staticmethod
    async def count_files(path: Path, recursive: bool = False) -> dict:
        """
        Count files in `path`. If `recursive=True`, include subfolders.
        """
        if not path.exists() or not path.is_dir():
            return {
                "success": False,
                "msg": f"{path.name} not exist",
                "count": -1
            }

        def _count_nonrec() -> int:
            return sum(1 for p in path.iterdir() if p.is_file())

        def _count_rec() -> int:
            total = 0
            for _, _, files in os.walk(path):
                total += len(files)
            return total

        count = await asyncio.to_thread(_count_rec if recursive else _count_nonrec)

        return {"success": True, "msg": "Counting files successful", "count": count}


    @staticmethod
    async def copy_files(
            from_path: Path,
            to_path: Path,
            iterate_subfolders: bool = True,
            rename_files: bool = True,
            prefix_name: str = "File"
    ) -> dict:
        to_path.mkdir(parents=True, exist_ok=True)

        log_file = to_path.parent / f"{to_path.stem}_copy_log.txt"
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

            try:
                await asyncio.to_thread(shutil.copy2, str(file), str(dest))
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

    @staticmethod
    async def validate_files(
            input_path: Path,
            include_image=True,
            include_video=True,
            image_valid_exts : list[str] | None = None,
            image_convert_exts: list[str] | None = None,
            video_valid_exts : list[str] | None = None,
            video_convert_exts : list[str] | None = None

    ) -> dict:
        if not input_path.exists() or not input_path.is_dir():
            return {"success": False, "msg": f"{input_path!s} is not a directory"}

        image_valid_exts = image_valid_exts or [".png"]
        image_convert_exts = image_convert_exts or [".webp", ".tiff", ".bmp", ".heic", ".jpeg", ".jpg"]
        video_valid_exts = video_valid_exts or [".mp4"]
        video_convert_exts = video_convert_exts or [".avi", ".mov", ".mkv", ".flv"]

        log_file = input_path.parent / f"{input_path.stem}_validate_log.txt"
        log_lines = []

        error = False
        for file in input_path.iterdir():
            if not file.is_file():
                continue

            ext = file.suffix.lower()

            if include_image and ext in image_valid_exts:
                image_resize_res = InoMediaHelper.image_resize_pillow(file, file)
                log_lines.append(
                    image_resize_res
                )

            elif include_image and ext in image_convert_exts:
                new_file = file.with_suffix('.png')
                image_convert_res = InoMediaHelper.image_convert_pillow(file, new_file)
                log_lines.append(
                    image_convert_res
                )
                image_resize_res = InoMediaHelper.image_resize_pillow(new_file, new_file)
                log_lines.append(
                    image_resize_res
                )

            elif include_video and ext in video_valid_exts:
                video_convert_res = await InoMediaHelper.video_convert_ffmpeg(
                    input_path=file,
                    output_path=file,
                    change_res=True,
                    change_fps=True
                )

                log_lines.append(
                    video_convert_res
                )
            elif include_video and ext in video_convert_exts:
                new_file = file.with_suffix('.mp4')
                video_convert_res = await InoMediaHelper.video_convert_ffmpeg(
                    input_path=file,
                    output_path=new_file,
                    change_res=True,
                    change_fps=True
                )

                log_lines.append(
                    video_convert_res
                )
            elif not include_image and ext in image_valid_exts:
                move_file = file.parent / "skipped_images" / file.name
                move_file_res = await InoFileHelper.move_file(file, move_file)
                if not move_file_res["success"]:
                    return move_file_res
                log_lines.append(
                    f"⚠️ Skipped image: {file.name}"
                )
            elif not include_image and ext in image_convert_exts:
                move_file = file.parent / "skipped_images_unsupported" / file.name
                move_file_res = await InoFileHelper.move_file(file, move_file)
                if not move_file_res["success"]:
                    return move_file_res
                log_lines.append(
                    f"⚠️ Skipped unsupported image: {file.name}"
                )
            elif not include_video and ext in video_valid_exts:
                move_file = file.parent / "skipped_videos" / file.name
                move_file_res = await InoFileHelper.move_file(file, move_file)
                if not move_file_res["success"]:
                    return move_file_res
                log_lines.append(
                    f"⚠️ Skipped video: {file.name}"
                )
            elif not include_video and ext in video_convert_exts:
                move_file = file.parent / "skipped_videos_unsupported" / file.name
                move_file_res = await InoFileHelper.move_file(file, move_file)
                if not move_file_res["success"]:
                    return move_file_res
                log_lines.append(
                    f"⚠️ Skipped unsupported video: {file.name}"
                )
            else:
                # -----skip all unsupported files
                move_file = file.parent / "unsupported_files" / file.name
                move_file_res = await InoFileHelper.move_file(file, move_file)
                if not move_file_res["success"]:
                    return move_file_res
                log_lines.append(
                    f"⚠️ Skipped unsupported file: {file.name}"
                )

        if log_lines:
            with log_file.open("w", encoding="utf-8") as f:
                for entry in log_lines:
                    if isinstance(entry, str):
                        entry = {"success": True, "msg": entry}
                    f.write(json.dumps(entry, ensure_ascii=False) + "\n")
        if error:
            return {
                "success": False,
                "msg": f"Failed to validate files, see log file: {log_file}"
            }

        return {
            "success": True,
            "msg": f"📂 Validating files completed"
        }