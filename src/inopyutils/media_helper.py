import asyncio
from pathlib import Path
from typing import Dict, Any, Tuple
from PIL import Image, ImageOps, ExifTags
from PIL.Image import Resampling

from pillow_heif import register_heif_opener

import cv2
import shutil

register_heif_opener()

class InoMediaHelper:
    @staticmethod
    async def video_convert_ffmpeg(
            input_path: Path,
            output_path: Path,
            change_res: bool,
            change_fps: bool,
            max_res: int = 2560,
            max_fps: int = 30
    ) -> dict:
        output_path = output_path.with_suffix('.mp4')
        temp_output = output_path.with_name(output_path.stem + "_converted.mp4")

        args = [
            'ffmpeg', '-y',
            '-loglevel', 'error',
            '-i', str(input_path),
            ]

        if change_fps:
            args += ["-filter:v", f"fps={max_fps}"]

        if change_res:
            # if width>=height, setting width to min(iw,max_res) and keeping AR. else setting height.
            scale = f"scale='if(gte(iw,ih),min(iw,{max_res}),-2)':'if(gte(ih,iw),min(ih,{max_res}),-2)'"
            # preventing upscaling
            scale = f"{scale}:force_original_aspect_ratio=decrease"
            # merging with existing filter if fps already added
            if "-filter:v" in args:
                i = args.index("-filter:v") + 1
                args[i] = args[i] + f", {scale}"
            else:
                args += ["-filter:v", scale]

        args += [
            "-c:v", "libx264",
            "-preset", "medium",
            "-crf", "23",  # 20–24 typical; lower = larger
            "-pix_fmt", "yuv420p",
            "-maxrate", "12M",  # cap spikes (tune to your needs)
            "-bufsize", "24M",  # 2× maxrate is common
            "-movflags", "+faststart",  # better MP4 streaming
        ]

        args += ["-c:a", "aac", "-b:a", "192k"]
        args += ["-f", "mp4", str(temp_output)]

        try:
            proc = await asyncio.create_subprocess_exec(
                *args,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            stdout, stderr = await proc.communicate()

            if proc.returncode != 0:
                return {
                    "success": False,
                    "msg": f"❌ Conversion failed ({input_path.name}): {stderr.decode().strip()}",
                    "original_size": 0,
                    "converted_size": 0,
                }

            original_size = input_path.stat().st_size // 1024
            converted_size = temp_output.stat().st_size // 1024

            if not temp_output.exists():
                return {
                    "success": False,
                    "msg": "Conversion failed, converted file not found",
                    "original_size": 0,
                    "converted_size": 0,
                }

            await asyncio.to_thread(input_path.unlink)
            await asyncio.to_thread(shutil.move, str(temp_output), str(output_path))
            return {
                "success": True,
                "msg": f"✅ Converted {input_path.name} ",
                "original_size": original_size,
                "converted_size": converted_size,
            }
        except Exception as e:
            return {
                "success": False,
                "msg": f"❌ Video conversion error: {e}",
                "original_size": 0,
                "converted_size": 0,
            }


    @staticmethod
    async def image_convert_ffmpeg(input_path: Path, output_path: Path) -> dict:
        args = [
            'ffmpeg', '-y',
            '-loglevel', 'error',
            '-i', str(input_path),
            str(output_path)
        ]
        try:
            proc = await asyncio.create_subprocess_exec(
                *args,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            stdout, stderr = await proc.communicate()

            if proc.returncode != 0:
                return {
                    "success": False,
                    "msg": f"❌ Conversion failed ({input_path.name}): {stderr.decode().strip()}",
                }

            await asyncio.to_thread(input_path.unlink)
            return {
                "success": True,
                "msg": f"✅ Converted {input_path.name} ",
            }
        except Exception as e:
            return {
                "success": False,
                "msg": f"❌ Image conversion error: {e}",
            }


    @staticmethod
    async def image_validate_pillow(
            input_path: Path,
            output_path: Path | None = None,
            max_res: int = 3200,
            jpg_quality: int = 92
    ) -> Dict[str, Any]:
        """
        - Fix EXIF rotation
        - Resize only if larger than max_res
        - Save as JPEG if not already JPEG (overwrites if already JPEG)
        - Preserve EXIF + ICC profile where possible
        """
        _ORIENTATION_TAG = {v: k for k, v in ExifTags.TAGS.items()}.get("Orientation")

        def _work() -> Dict[str, Any]:
            try:
                # Ensure output has .jpg extension when provided
                if output_path is not None:
                    final_out = Path(output_path)
                    if final_out.suffix.lower() != ".jpg":
                        final_out = final_out.with_suffix(".jpg")
                else:
                    final_out = None  # decide after opening

                pending_rename = False
                with Image.open(input_path) as img:
                    input_format = (img.format or "").upper()
                    is_jpeg_in = input_format == "JPEG"

                    # Decide final_out now if not provided
                    if final_out is None:
                        # Always target .jpg extension as requested
                        final_out = input_path.with_suffix(".jpg")

                    # Prepare output folder
                    final_out.parent.mkdir(parents=True, exist_ok=True)

                    # Capture original metadata early
                    orig_exif = img.getexif()
                    orig_icc = img.info.get("icc_profile")
                    orig_orientation = (
                        orig_exif.get(_ORIENTATION_TAG, 1)
                        if (orig_exif and _ORIENTATION_TAG is not None)
                        else 1
                    )

                    # Fix orientation in pixels
                    img = ImageOps.exif_transpose(img)
                    orientation_changed = orig_orientation != 1

                    # Compute resizing
                    old_size: Tuple[int, int] = (img.width, img.height)
                    need_resize = img.width > max_res or img.height > max_res
                    if need_resize:
                        scale = min(max_res / img.width, max_res / img.height)
                        new_size = (max(1, int(img.width * scale)), max(1, int(img.height * scale)))
                        img = img.resize(new_size, resample=Resampling.LANCZOS)
                    else:
                        new_size = old_size

                    # If input already JPEG and no pixel changes are required
                    if (
                        is_jpeg_in and not need_resize and not orientation_changed
                    ):
                        # If target equals source path -> nothing to do
                        if final_out.resolve() == input_path.resolve():
                            return {
                                "success": True,
                                "msg": f"✅ No changes needed: {input_path.name}",
                                "resized": False,
                                "converted_to_jpeg": False,
                                "old_size": old_size,
                                "new_size": new_size,
                                "output": str(final_out),
                            }
                        # Else, only extension/path change is needed -> defer filesystem rename
                        pending_rename = True

                    if not pending_rename:
                        # Handle transparency & modes
                        if img.mode == "P":
                            if "transparency" in img.info:
                                img = img.convert("RGBA")
                            else:
                                img = img.convert("RGB")
                        if img.mode in ("RGBA", "LA"):
                            alpha = img.getchannel("A")
                            background = Image.new("RGB", img.size, (255, 255, 255))
                            img = Image.composite(img.convert("RGB"), background, alpha)
                        elif img.mode not in ("RGB", "L"):
                            img = img.convert("RGB")

                        # JPEG save settings
                        save_kwargs: Dict[str, Any] = {
                            "format": "JPEG",
                            "quality": max(1, min(95, int(jpg_quality))),
                            "optimize": True,
                            "progressive": True,
                        }

                        if orig_icc:
                            save_kwargs["icc_profile"] = orig_icc

                        if orig_exif and _ORIENTATION_TAG is not None:
                            try:
                                orig_exif[_ORIENTATION_TAG] = 1
                                save_kwargs["exif"] = orig_exif.tobytes()
                            except Exception:
                                pass

                        img.save(final_out, **save_kwargs)

                # If only rename is pending (JPEG→JPEG with extension change), move the file and return
                if pending_rename:
                    try:
                        final_out.parent.mkdir(parents=True, exist_ok=True)
                        shutil.move(str(input_path), str(final_out))
                    except Exception as e:
                        return {
                            "success": False,
                            "msg": f"❌ Image rename failed: {input_path.name} — {e}",
                            "resized": False,
                            "converted_to_jpeg": False,
                            "old_size": old_size,
                            "new_size": new_size,
                            "output": None,
                        }
                    return {
                        "success": True,
                        "msg": f"✅ Renamed to .jpg: {final_out.name}",
                        "resized": False,
                        "converted_to_jpeg": False,
                        "old_size": old_size,
                        "new_size": new_size,
                        "output": str(final_out),
                    }

                # If we converted from non-JPEG to JPEG and wrote to a new file, delete original
                converted = not is_jpeg_in
                if converted and input_path.exists():
                    try:
                        input_path.unlink()
                    except Exception as e:
                        return {
                            "success": False,
                            "msg": f"❌ Image validation failed: {input_path.name} — {e}",
                            "resized": None,
                            "converted_to_jpeg": None,
                            "old_size": None,
                            "new_size": None,
                            "output": None,
                        }

                return {
                    "success": True,
                    "msg": f"✅ Validated {input_path.name}",
                    "resized": need_resize,
                    "converted_to_jpeg": converted,
                    "old_size": old_size,
                    "new_size": new_size,
                    "output": str(final_out),
                }

            except Exception as e:
                return {
                    "success": False,
                    "msg": f"❌ Image validation failed: {input_path.name} — {e}",
                    "resized": None,
                    "converted_to_jpeg": None,
                    "old_size": None,
                    "new_size": None,
                    "output": None,
                }

        return await asyncio.to_thread(_work)

    @staticmethod
    def validate_video_res_fps(input_path: Path, max_res: int = 2560, max_fps: int = 30) -> dict:
        cap = cv2.VideoCapture(str(input_path))
        if not cap.isOpened():
            return {
                "Result": False,
                "Message": f"OpenCV failed to open {input_path.name}",
            }

        width = int(cap.get(cv2.CAP_PROP_FRAME_WIDTH))
        height = int(cap.get(cv2.CAP_PROP_FRAME_HEIGHT))
        fps = cap.get(cv2.CAP_PROP_FPS)
        cap.release()

        is_res_too_high = width > max_res or height > max_res
        is_fps_too_high = fps > max_fps
        if is_res_too_high:
            return {
                "Result": True,
                "Message": f"Video res is too high: {input_path.name} -> {width}x{height}",
            }
        elif is_fps_too_high:
            return {
                "Result": True,
                "Message": f"Video fps is too high: {input_path.name} -> {fps}",
            }
        else:
            return {
                "Result": False,
                "Message": f"Video {input_path.name} have a valid res and fps",
            }

    @staticmethod
    def get_video_fps(input_path: Path) -> float:
        cap = cv2.VideoCapture(str(input_path))
        fps = cap.get(cv2.CAP_PROP_FPS)
        cap.release()
        return fps
