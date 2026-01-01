import asyncio
from pathlib import Path
from typing import Iterable, List, Optional

try:
    from PIL import Image, ImageOps, ImageFilter
except Exception:  # pragma: no cover - import error surfaced at runtime
    Image = None
    ImageOps = None
    ImageFilter = None

from .util_helper import ino_is_err, ino_err, ino_ok

class InoThumbnailHelper:
    """Helper for generating square thumbnails at multiple sizes.
    """

    @staticmethod
    def image_generate_square_thumbnails(
        image_path: Path,
        output_dir: Optional[Path] = None,
        sizes: Iterable[int] = (256, 512, 1024),
        quality: int = 50,
        crop: bool = False,
    ) -> dict:
        """Create 1:1 thumbnails by cropping or padding to square, then resizing.

        - Keeps original base filename, adds prefix: t_{size}_ and ALWAYS saves as .jpg
        - Uses Pillow for processing
        """

        if Image is None or ImageOps is None:
            return ino_err("Pillow (PIL) is required to use InoThumbnailHelper")

        image_path = Path(image_path)
        if not image_path.is_file():
            return ino_err(f"Input image not found: {image_path}")

        output_dir = Path(output_dir) if output_dir is not None else image_path.parent
        output_dir.mkdir(parents=True, exist_ok=True)

        try:
            quality = int(quality)
        except Exception as e:
            return ino_err(f"Invalid quality value: {quality}")
        if not (1 <= quality <= 95):
            return ino_err(f"Quality must be between 1 and 95, got {quality}")

        norm_sizes: List[int] = []
        for s in sizes:
            try:
                v = int(s)
            except Exception as e:
                return ino_err(f"Invalid size value: {s}")
            if v <= 0:
                return ino_err(f"Size must be positive, got {s}")
            if v not in norm_sizes:
                norm_sizes.append(v)

        name = image_path.stem

        # Convert to RGB for formats that don't support modes well (e.g., JPEG)
        # We'll decide per-extension when saving.

        # Prepare a square image either by center-cropping (crop=True)
        # or padding to square with a blurred background (crop=False)

        original_image = Image.open(str(image_path))

        original_image = ImageOps.exif_transpose(original_image)

        output_paths: List[str] = []

        try:
            width, height = original_image.size
            if crop:
                side = min(width, height)
                left = (width - side) // 2
                top = (height - side) // 2
                right = left + side
                bottom = top + side
                square = original_image.crop((left, top, right, bottom))
            else:
                side = max(width, height)
                # Ensure RGB for consistent padding background
                im_rgb = original_image if original_image.mode == "RGB" else original_image.convert("RGB")

                # Build a blurred background that fills the square without distortion:
                # 1) Scale the image so the smaller side equals `side` (cover)
                # 2) Center-crop to a square of (side x side)
                resample_bg = getattr(Image, "Resampling", Image).LANCZOS
                scale = side / float(min(width, height)) if min(width, height) else 1.0
                bg_w = max(1, int(round(width * scale)))
                bg_h = max(1, int(round(height * scale)))
                bg = im_rgb.resize((bg_w, bg_h), resample=resample_bg)
                bg_left = max(0, (bg_w - side) // 2)
                bg_top = max(0, (bg_h - side) // 2)
                bg = bg.crop((bg_left, bg_top, bg_left + side, bg_top + side))

                # Apply Gaussian blur to create the background
                if ImageFilter is not None:
                    radius = max(2, int(side * 0.02))  # proportional blur radius
                    bg = bg.filter(ImageFilter.GaussianBlur(radius=radius))

                # Paste the original image centered on the blurred background
                paste_left = (side - width) // 2
                paste_top = (side - height) // 2
                bg.paste(im_rgb, (paste_left, paste_top))
                square = bg

            # Resampling selection compatible across Pillow versions
            resample = getattr(Image, "Resampling", Image).LANCZOS

            for size in norm_sizes:
                resized = square.resize((size, size), resample=resample)

                # Always save as JPEG with .jpg extension
                out_filename = f"{name}_ino_t_{size}_.jpg"
                out_path = output_dir / out_filename

                # Ensure RGB for JPEG output
                if resized.mode not in ("RGB", "L"):
                    resized = resized.convert("RGB")

                # Strip metadata by creating a fresh image and pasting the pixel data
                if resized.mode != "RGB":
                    rgb_img = resized.convert("RGB")
                else:
                    rgb_img = resized
                clean_img = Image.new("RGB", rgb_img.size)
                clean_img.paste(rgb_img)

                # Explicitly save as JPEG with provided quality, no EXIF/ICC passed
                clean_img.save(
                    str(out_path),
                    format="JPEG",
                    quality=quality,
                    optimize=True,
                    progressive=True,
                )
                output_paths.append(str(out_path))
                clean_img.close()
        except Exception as e:
            return ino_err(f"Error generating thumbnails: {str(e)}")
        finally:
            original_image.close()
            return ino_ok("Thumbnail generated", output_paths=output_paths)

    @staticmethod
    async def image_generate_square_thumbnails_async(
        image_path: Path,
        output_dir: Optional[Path] = None,
        sizes: Iterable[int] = (256, 512, 1024),
        quality: int = 90,
        crop: bool = False,
    ) -> dict:
        """Async wrapper for `image_generate_square_thumbnails`.

        Runs the CPU-bound Pillow processing in a background thread to avoid
        blocking the event loop. API mirrors the synchronous method and returns
        the same list of generated file paths.
        """
        return await asyncio.to_thread(
            InoThumbnailHelper.image_generate_square_thumbnails,
            image_path,
            output_dir,
            sizes,
            quality,
            crop,
        )