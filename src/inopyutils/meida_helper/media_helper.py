import asyncio
import sys
from pathlib import Path

from pillow_heif import register_heif_opener
from PIL import Image

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
            args += ['-r', str(max_fps)]

        if change_res:
            args += ['-vf', f"scale='if(gt(iw,ih),min(iw,{max_res}),-2)':'if(gt(ih,iw),min(ih,{max_res}),-2)'"]

        args += [
            '-preset', 'medium',
            '-crf', '23',
            '-c:v', 'libx264',
            '-c:a', 'aac',
            '-b:a', '192k'
        ]

        args += ['-f', 'mp4']
        args += [str(temp_output)]

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
    async def image_convert_ffmpeg(input_path: Path, output_path: Path):
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
    def image_convert_pillow(input_path: Path, output_path: Path) -> dict:
        try:
            img = Image.open(input_path)
            img.save(output_path, format="PNG")
            img.close()

            input_path.unlink()

            return {
                "success": True,
                "msg": f"✅ Converted {input_path.name}",
            }
        except Exception as e:
            return {
                "success": False,
                "msg": f"❌ Image conversion failed: {input_path.name} — {e}",
            }

    @staticmethod
    def image_resize_pillow(input_path: Path, output_path: Path, max_res: int = 3200) -> dict:
        try:
            img = Image.open(input_path)

            if img.width > max_res or img.height > max_res:
                temp_output = output_path.with_name(output_path.stem + "_converted.png")
                scale = min(max_res / img.width, max_res / img.height)
                old_size = (int(img.width), int(img.height))
                new_size = (int(img.width * scale), int(img.height * scale))

                img = img.resize(new_size, Image.LANCZOS)
                img.save(temp_output, format="PNG")
                img.close()

                shutil.move(temp_output, output_path)

                return {
                    "success": True,
                    "msg": f"✅ Converted {input_path.name}",
                    "old_size": old_size,
                    "new_size": new_size,
                }
            else:
                return {
                    "success": True,
                    "msg": f"🖼️ Resize image skipped: {input_path.name}: {img.width}x{img.height}",
                    "old_size": None,
                    "new_size": None,
                }
        except Exception as e:
            return {
                "success": False,
                "msg": f"❌ Image resize failed: {input_path.name} — {e}",
                "old_size": None,
                "new_size": None,
            }

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
