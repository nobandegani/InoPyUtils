import asyncio
from dataclasses import dataclass
from fractions import Fraction
from pathlib import Path
from typing import Dict, Any, Tuple, Optional, Union
from PIL import Image, ImageOps, ExifTags
from PIL.Image import Resampling

from pillow_heif import register_heif_opener

import cv2
import shutil

register_heif_opener()


@dataclass
class PhotoMetadata:
    # Camera category can be stored in ImageDescription
    camera_category: Optional[str] = None
    camera_maker: Optional[str] = None
    camera_model: Optional[str] = None

    # Core exposure fields
    f_stop: Optional[Union[str, int, float]] = None            # EXIF FNumber
    exposure_time: Optional[Union[str, int, float]] = None     # EXIF ExposureTime
    iso_speed: Optional[Union[int, str]] = None                # EXIF ISOSpeedRatings (int)
    exposure_bias: Optional[Union[str, int, float]] = None     # EXIF ExposureBiasValue
    focal_length: Optional[Union[str, int, float]] = None      # EXIF FocalLength
    max_aperture: Optional[Union[str, int, float]] = None      # EXIF MaxApertureValue
    metering_mode: Optional[Union[int, str]] = None            # EXIF MeteringMode (enum int)
    subject_distance: Optional[Union[str, int, float]] = None  # EXIF SubjectDistance
    flash_mode: Optional[Union[int, str]] = None               # EXIF Flash (bitmask/int)
    flash_energy: Optional[Union[str, int, float]] = None      # EXIF FlashEnergy
    focal_length_35mm: Optional[Union[int, str]] = None        # EXIF FocalLengthIn35mmFilm

    # Advanced
    lens_maker: Optional[str] = None
    lens_model: Optional[str] = None
    flash_maker: Optional[str] = None  # Not standard; will be placed in UserComment
    flash_model: Optional[str] = None  # Not standard; will be placed in UserComment
    camera_serial_number: Optional[str] = None                 # EXIF BodySerialNumber
    contrast: Optional[Union[int, str]] = None                 # EXIF Contrast (enum int)
    brightness: Optional[Union[str, int, float]] = None        # EXIF BrightnessValue
    light_source: Optional[Union[int, str]] = None             # EXIF LightSource (enum int)
    exposure_program: Optional[Union[int, str]] = None         # EXIF ExposureProgram (enum int)
    saturation: Optional[Union[int, str]] = None               # EXIF Saturation (enum int)
    sharpness: Optional[Union[int, str]] = None                # EXIF Sharpness (enum int)
    white_balance: Optional[Union[int, str]] = None            # EXIF WhiteBalance (enum int)
    photometric_interpretation: Optional[Union[int, str]] = None  # EXIF PhotometricInterpretation
    digital_zoom: Optional[Union[str, int, float]] = None      # EXIF DigitalZoomRatio
    exif_version: Optional[Union[str, bytes, int]] = None      # EXIF ExifVersion e.g., b"0231"

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
            jpg_quality: int = 92,
            metadata: Optional["PhotoMetadata"] = None,
            remove_metadata: bool = False,
            overwrite_existing: bool = True,
    ) -> Dict[str, Any]:
        """
        - Fix EXIF rotation
        - Resize only if larger than max_res
        - Save as JPEG if not already JPEG (overwrites if already JPEG)
        - Preserve EXIF + ICC profile where possible
        """
        _ORIENTATION_TAG = {v: k for k, v in ExifTags.TAGS.items()}.get("Orientation")

        # Build a reverse lookup once
        _TAG_BY_NAME: Dict[str, int] = {v: k for k, v in ExifTags.TAGS.items()}

        def _to_rational(value: Union[str, int, float, Fraction]) -> Tuple[int, int]:
            if isinstance(value, Fraction):
                return (value.numerator, value.denominator)
            if isinstance(value, (int,)):
                return (int(value), 1)
            if isinstance(value, float):
                f = Fraction(value).limit_denominator(10000)
                return (f.numerator, f.denominator)
            if isinstance(value, str):
                s = value.strip().lower()
                # "1/125"
                if "/" in s:
                    try:
                        num, den = s.split("/", 1)
                        return (int(num.strip()), int(den.strip()))
                    except Exception:
                        pass
                # "f/2.8" or "2.8"
                s = s.replace("f/", "").replace("mm", "").strip()
                try:
                    f = Fraction(s).limit_denominator(10000)
                    return (f.numerator, f.denominator)
                except Exception:
                    # last resort
                    try:
                        f = Fraction(float(s)).limit_denominator(10000)
                        return (f.numerator, f.denominator)
                    except Exception:
                        return (0, 1)
            # Unknown type
            return (0, 1)

        def _apply_metadata(exif: Image.Exif, meta: "PhotoMetadata", overwrite: bool) -> Tuple[Image.Exif, Dict[str, Any], Dict[str, str]]:
            applied: Dict[str, Any] = {}
            skipped: Dict[str, str] = {}

            def set_tag(tag_name: str, value: Any, transformer=None):
                if value is None:
                    return
                tag_id = _TAG_BY_NAME.get(tag_name)
                if tag_id is None:
                    skipped[tag_name] = "No EXIF tag available"
                    return
                if not overwrite and exif.get(tag_id) not in (None, b"", 0, 0.0):
                    skipped[tag_name] = "Existing value retained"
                    return
                try:
                    v = transformer(value) if transformer else value
                    exif[tag_id] = v
                    applied[tag_name] = v
                except Exception as e:
                    skipped[tag_name] = f"Failed to set: {e}"

            # Camera info
            set_tag("Make", meta.camera_maker)
            set_tag("Model", meta.camera_model)
            # Use ImageDescription for category if provided
            set_tag("ImageDescription", meta.camera_category)

            # Core exposure
            set_tag("FNumber", meta.f_stop, _to_rational)
            set_tag("ExposureTime", meta.exposure_time, _to_rational)
            set_tag("ISOSpeedRatings", meta.iso_speed, lambda v: int(v) if v is not None else v)
            set_tag("ExposureBiasValue", meta.exposure_bias, _to_rational)
            set_tag("MaxApertureValue", meta.max_aperture, _to_rational)
            set_tag("FocalLength", meta.focal_length, _to_rational)
            set_tag("SubjectDistance", meta.subject_distance, _to_rational)
            set_tag("MeteringMode", meta.metering_mode, lambda v: int(v))
            set_tag("ExposureProgram", meta.exposure_program, lambda v: int(v))
            set_tag("LightSource", meta.light_source, lambda v: int(v))
            set_tag("Flash", meta.flash_mode, lambda v: int(v))
            set_tag("FlashEnergy", meta.flash_energy, _to_rational)
            set_tag("FocalLengthIn35mmFilm", meta.focal_length_35mm, lambda v: int(v))

            # Advanced
            set_tag("LensMake", meta.lens_maker)
            set_tag("LensModel", meta.lens_model)
            set_tag("BodySerialNumber", meta.camera_serial_number)
            set_tag("Contrast", meta.contrast, lambda v: int(v))
            set_tag("BrightnessValue", meta.brightness, _to_rational)
            set_tag("Saturation", meta.saturation, lambda v: int(v))
            set_tag("Sharpness", meta.sharpness, lambda v: int(v))
            set_tag("WhiteBalance", meta.white_balance, lambda v: int(v))
            set_tag("PhotometricInterpretation", meta.photometric_interpretation, lambda v: int(v))
            set_tag("DigitalZoomRatio", meta.digital_zoom, _to_rational)
            # ExifVersion expects 4-byte string like b"0231"
            def _exif_version_transform(v: Any):
                if isinstance(v, (bytes, bytearray)):
                    return bytes(v)
                s = str(v).strip().replace(".", "")
                s = (s + "0000")[:4]
                return s.encode("ascii", errors="ignore")

            set_tag("ExifVersion", meta.exif_version, _exif_version_transform)

            # Flash maker/model not standard in EXIF; record into UserComment if provided
            if meta.flash_maker or meta.flash_model:
                tag_id = _TAG_BY_NAME.get("UserComment")
                comment = f"FlashMaker={meta.flash_maker or ''}; FlashModel={meta.flash_model or ''}".strip()
                try:
                    if tag_id is not None:
                        if overwrite or not exif.get(tag_id):
                            exif[tag_id] = comment.encode("utf-8", errors="ignore")
                            applied["UserComment"] = comment
                        else:
                            skipped["UserComment"] = "Existing value retained"
                    else:
                        skipped["Flash maker/model"] = "No standard EXIF tag; skipped"
                except Exception as e:
                    skipped["UserComment"] = f"Failed to set: {e}"

            return exif, applied, skipped

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
                        if final_out.resolve() == input_path.resolve() and not remove_metadata and metadata is None:
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
                        # But do not rename if we need to strip or write metadata; force re-encode then
                        pending_rename = not (remove_metadata or metadata is not None)

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

                        # ICC handling
                        if orig_icc and not remove_metadata:
                            save_kwargs["icc_profile"] = orig_icc

                        # EXIF handling
                        exif_to_write = None
                        if not remove_metadata:
                            # start from existing EXIF if available
                            exif_to_write = orig_exif if orig_exif else Image.Exif()
                            # Ensure Orientation is reset to 1 after pixel transpose
                            if _ORIENTATION_TAG is not None:
                                try:
                                    exif_to_write[_ORIENTATION_TAG] = 1
                                except Exception:
                                    pass
                            # Apply requested metadata
                            if metadata is not None:
                                exif_to_write, applied_meta, skipped_meta = _apply_metadata(exif_to_write, metadata, overwrite_existing)
                            else:
                                applied_meta, skipped_meta = {}, {}
                            try:
                                save_kwargs["exif"] = exif_to_write.tobytes()
                            except Exception:
                                # If EXIF serialization fails, drop EXIF
                                exif_to_write = None
                        else:
                            applied_meta, skipped_meta = {}, {}

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
                    "metadata_applied": applied_meta if not pending_rename else {},
                    "metadata_skipped": skipped_meta if not pending_rename else {},
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
