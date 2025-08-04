import zipfile
import asyncio
import json
import shutil
from pathlib import Path
from ..meida_helper.media_helper import InoMediaHelper

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
    async def copy_files(
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

        log_file = input_path.parent / "validate_log.txt"
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