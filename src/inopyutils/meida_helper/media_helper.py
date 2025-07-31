import subprocess
from pathlib import Path

from pillow_heif import register_heif_opener
from PIL import Image

import cv2
import shutil

register_heif_opener()

class InoMediaHelper:
    @staticmethod
    def video_convert_ffmpeg(input_path: Path, output_path: Path, remove_input: bool, force_max_fps: bool, max_res: int = 2560, max_fps: int = 30) -> dict:
        try:
            output_path = output_path.with_suffix('.mp4')
            temp_output = output_path.with_name(output_path.stem + "_converted.mp4")

            args = [
                'ffmpeg', '-y',
                '-loglevel', 'error',
                '-i', str(input_path),
                ]

            try:
                if force_max_fps:
                    args += ['-r', str(max_fps)]
                else:
                    fps = InoMediaHelper.get_video_fps(input_path)
                    if fps > max_fps:
                        args += ['-r', str(max_fps)]
            except Exception as e:
                print(f"⚠️ Could not determine FPS: {e} for {input_path}")

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

            original_size = input_path.stat().st_size // 1024
            subprocess.run(args, capture_output=True, text=True, check=True)
            converted_size = temp_output.stat().st_size // 1024
            print(f"📦 Size reduced from {original_size} KB to {converted_size} KB")

            if temp_output.exists():
                if remove_input:
                    input_path.unlink()
                shutil.move(temp_output, output_path)
                result = f"🎥 Converted video: {input_path.name} → {temp_output.name}"
            else:
                print(f"❌ Temp output not created: {temp_output}")
                return f"❌ Conversion failed, no output file created for: {input_path.name}"

            print(result)
            return result
        except subprocess.CalledProcessError as e:
            result = f"❌ Video conversion failed: {input_path.name} — {e}"
            print(result)
            return result


    @staticmethod
    def image_convert_ffmpeg(input_path: Path, output_path: Path):
        try:
            subprocess.run([
                'ffmpeg', '-y',
                '-loglevel', 'error',
                '-i', str(input_path),
                str(output_path)
                ], capture_output=True, text=True, check=True
            )
            result = f"🖼️ Converted image: {input_path.name} → {output_path.name}"
            print(result)
            input_path.unlink()
            return result
        except subprocess.CalledProcessError as e:
            result = f"❌ Image conversion failed: {input_path.name} — {e}"
            print(result)
            return result

    @staticmethod
    def image_convert_pillow(input_path: Path, output_path: Path) -> str:
        try:
            #temp_output = output_path.with_name(output_path.stem + "_converted.png")
            temp_output = output_path

            img = Image.open(input_path)
            img.save(temp_output, format="PNG")
            img.close()

            result = f"🖼️ Converted image: {input_path.name} → {output_path.name}"
            print(result)

            input_path.unlink()

            return result
        except Exception as e:
            result = f"❌ Image conversion failed: {input_path.name} — {e}"
            print(result)
            return result

    def image_resize_pillow(input_path: Path, output_path: Path, max_res: int = 3200) -> str:
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

                result = f"🖼️ Resized image: {input_path.name}: {old_size[0]}x{old_size[1]} -> {new_size[0]}x{new_size[1]}"
                print(result)

                return result
            else:
                result = f"🖼️ Resize image skipped: {input_path.name}: {img.width}x{img.height}"
                print(result)
                return result
        except Exception as e:
            result = f"❌ Image resize failed: {input_path.name} — {e}"
            print(result)
            return result

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
