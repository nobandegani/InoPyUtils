import asyncio
from pathlib import Path

from src.inopyutils import InoMediaHelper

async def main():

    await InoMediaHelper.image_validate_pillow(
        input_path=Path(r""),
        output_path=Path(r""),
        #metadata=PhotoMetadata("iphone"),
        #remove_metadata=False,
        #overwrite_existing=True
    )

if __name__ == "__main__":
    asyncio.run(main())