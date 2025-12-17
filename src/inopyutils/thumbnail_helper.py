import os
from typing import Iterable, List

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
    def generate_square_thumbnails(
        image_path: str,
        output_dir: str,
        sizes: Iterable[int] = (256, 512, 1024),
    ) -> List[str]:
        """Create 1:1 thumbnails by center-cropping and resizing.

        - Keeps original base filename, adds prefix: t_{size}_ and ALWAYS saves as .jpg
        - Uses Pillow for processing

        Args:
            image_path: Full path to input image
            output_dir: Directory where thumbnails will be saved (created if absent)
            sizes: Iterable of square edge sizes to generate

        Returns:
            List of full paths to the generated thumbnails
        """

        if Image is None or ImageOps is None:
            raise ImportError("Pillow (PIL) is required to use InoThumbnailHelper")

        if not image_path or not os.path.isfile(image_path):
            raise FileNotFoundError(f"Input image not found: {image_path}")

        if not output_dir:
            raise ValueError("output_dir must be provided")

        # Normalize and ensure output directory exists
        output_dir = os.path.abspath(output_dir)
        os.makedirs(output_dir, exist_ok=True)

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

        base_name = os.path.basename(image_path)
        name, _ext = os.path.splitext(base_name)

        # Open image and correct orientation using EXIF
        with Image.open(image_path) as im:
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
                out_filename = f"t_{size}_{name}.jpg"
                out_path = os.path.join(output_dir, out_filename)

                # Ensure RGB for JPEG output
                if resized.mode not in ("RGB", "L"):
                    resized = resized.convert("RGB")

                # Explicitly save as JPEG
                resized.save(
                    out_path,
                    format="JPEG",
                    quality=90,
                    optimize=True,
                    progressive=True,
                )
                output_paths.append(out_path)

        return output_paths