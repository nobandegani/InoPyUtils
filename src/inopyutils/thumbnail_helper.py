import asyncio
from pathlib import Path
from typing import Iterable, List, Optional

try:
    # Pillow is required
    from PIL import Image, ImageOps
except Exception:  # pragma: no cover - import error surfaced at runtime
    Image = None
    ImageOps = None


class InoThumbnailHelper:
    """Helper for generating square thumbnails at multiple sizes.

    Usage example:
        InoThumbnailHelper.generate_square_thumbnails(
            image_path="/path/to/image_001.jpg",
            output_dir="/path/to/output",
            sizes=(256, 512, 1024),
        )
    This will create files like:
        t_256_image_001.jpg, t_512_image_001.jpg, t_1024_image_001.jpg
    Note: Thumbnails are ALWAYS saved as JPEG (.jpg) regardless of input format.
    """

    @staticmethod
    def image_generate_square_thumbnails(
        image_path: Path,
        output_dir: Optional[Path] = None,
        sizes: Iterable[int] = (256, 512, 1024),
        quality: int = 50,
    ) -> List[str]:
        """Create 1:1 thumbnails by center-cropping and resizing.

        - Keeps original base filename, adds prefix: t_{size}_ and ALWAYS saves as .jpg
        - Uses Pillow for processing

        Args:
            image_path: Path to input image (Path)
            output_dir: Directory where thumbnails will be saved (Path). If None, uses the
                same directory as image_path. The directory will be created if absent.
            sizes: Iterable of square edge sizes to generate
            quality: JPEG quality (1-95). Higher is better quality but larger size. Metadata
                is stripped from the output files.

        Returns:
            List of full paths to the generated thumbnails
        """

        if Image is None or ImageOps is None:
            raise ImportError("Pillow (PIL) is required to use InoThumbnailHelper")

        # Normalize paths
        image_path = Path(image_path)
        if not image_path.is_file():
            raise FileNotFoundError(f"Input image not found: {image_path}")

        # Determine output directory
        output_dir = Path(output_dir) if output_dir is not None else image_path.parent
        # Ensure output directory exists
        output_dir.mkdir(parents=True, exist_ok=True)

        # Validate quality
        try:
            quality = int(quality)
        except Exception as e:
            raise ValueError(f"Invalid quality value: {quality}") from e
        if not (1 <= quality <= 95):
            raise ValueError(f"quality must be between 1 and 95, got {quality}")

        # Validate and normalize sizes
        norm_sizes: List[int] = []
        for s in sizes:
            try:
                v = int(s)
            except Exception as e:
                raise ValueError(f"Invalid size value: {s}") from e
            if v <= 0:
                raise ValueError(f"Thumbnail size must be > 0, got {v}")
            if v not in norm_sizes:
                norm_sizes.append(v)

        name = image_path.stem

        # Open image and correct orientation using EXIF
        with Image.open(str(image_path)) as im:
            im = ImageOps.exif_transpose(im)

            # Convert to RGB for formats that don't support modes well (e.g., JPEG)
            # We'll decide per-extension when saving.

            # Center-crop to square (1:1 aspect ratio)
            width, height = im.size
            side = min(width, height)
            left = (width - side) // 2
            top = (height - side) // 2
            right = left + side
            bottom = top + side
            square = im.crop((left, top, right, bottom))

            # Resampling selection compatible across Pillow versions
            resample = getattr(Image, "Resampling", Image).LANCZOS

            output_paths: List[str] = []
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

        return output_paths

    @staticmethod
    async def image_generate_square_thumbnails_async(
        image_path: Path,
        output_dir: Optional[Path] = None,
        sizes: Iterable[int] = (256, 512, 1024),
        quality: int = 90,
    ) -> List[str]:
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
        )