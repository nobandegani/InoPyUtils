import asyncio
from pathlib import Path

from src.inopyutils import InoThumbnailHelper

async def main():
    await InoThumbnailHelper.image_generate_square_thumbnails_async(
        image_path=Path(r""),
        output_dir=Path(r""),
        sizes=[256, 512, 1024],
        crop=False
    )

if __name__ == "__main__":
    asyncio.run(main())