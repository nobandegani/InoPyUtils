import asyncio
from pathlib import Path

from src.inopyutils import InoMediaHelper

async def main():
    """
    convert = await InoMediaHelper.image_validate_pillow(
        input_path=Path(r"assets\IMG_9737.jpeg"),
        output_path=Path(r"converted\IMG_9737.jpeg"),
        #metadata=PhotoMetadata("iphone"),
        #remove_metadata=False,
        #overwrite_existing=True
    )
    print(convert)
    """
    convert = await InoMediaHelper.video_extract_frame(
        input_path=Path(r"assets\Wanimate_00008-audio.mp4")
    )
    print(convert)

if __name__ == "__main__":
    asyncio.run(main())