import asyncio
from pathlib import Path
from typing import Dict, Any, Tuple
from PIL import Image, ImageOps, ExifTags
from PIL.Image import Resampling

from pillow_heif import register_heif_opener

import shutil

from .util_helper import ino_err, ino_is_err, ino_ok

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
            if output_path is not None:
                final_out = Path(output_path)
                if final_out.suffix.lower() != ".jpg":
                    final_out = final_out.with_suffix(".jpg")
            else:
                final_out = input_path.with_suffix(".jpg")

            final_out.parent.mkdir(parents=True, exist_ok=True)

            is_jpg_in = input_path.suffix.lower() == ".jpg"

            with Image.open(input_path) as img:
                orig_exif = img.getexif()
                orig_icc = img.info.get("icc_profile")
                orig_orientation = (
                    orig_exif.get(_ORIENTATION_TAG, 1)
                    if (orig_exif and _ORIENTATION_TAG is not None)
                    else 1
                )

                img = ImageOps.exif_transpose(img)
                orientation_changed = orig_orientation != 1

                old_size: Tuple[int, int] = (img.width, img.height)
                need_resize = img.width > max_res or img.height > max_res
                if need_resize:
                    scale = min(max_res / img.width, max_res / img.height)
                    new_size = (max(1, int(img.width * scale)), max(1, int(img.height * scale)))
                    img = img.resize(new_size, resample=Resampling.LANCZOS)
                else:
                    new_size = old_size

                if is_jpg_in and not need_resize and not orientation_changed:
                    if final_out.resolve() == input_path.resolve():
                        return ino_ok(
                            f"✅ No changes needed: {input_path.name}",
                            resized=False,
                            converted=False,
                            old_size=old_size,
                            new_size=new_size,
                            output=str(final_out),
                        )
                    else:
                        shutil.move(str(input_path), str(final_out))
                        return ino_ok(
                            f"✅ No changes needed: {input_path.name}",
                            resized=False,
                            converted=False,
                            old_size=old_size,
                            new_size=new_size,
                            output=str(final_out),
                        )

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

            input_path.unlink()

            return ino_ok(
                f"✅ Validated {input_path.name}",
                resized=need_resize,
                converted=True,
                old_size=old_size,
                new_size=new_size,
                output=str(final_out),
            )

        try:
            img_validate = await asyncio.to_thread(_work)
            return img_validate
        except Exception as e:
            return ino_err(
                f"❌ Validation failed: {e}",
                resized=False,
                converted=False,
                old_size=0,
                new_size=0,
                output="",
            )

    @staticmethod
    def validate_video_res_fps(input_path: Path, max_res: int = 2560, max_fps: int = 30) -> dict:
        return ino_err("validate fps deprecated, ")

    @staticmethod
    def get_video_fps(input_path: Path) -> float:
        return 0
