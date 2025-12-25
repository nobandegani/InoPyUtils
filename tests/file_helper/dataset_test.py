import asyncio
from pathlib import Path

from src.inopyutils import InoMediaHelper, InoFileHelper


async def main():

    copy = await InoFileHelper.copy_files(
        from_path=Path(r"assets"),
        to_path=Path(r"convert")
    )
    print(copy)
    convert = await InoFileHelper.validate_files(
        include_image=True,
        include_video=False,
        input_path=Path(r"convert")
    )
    print(convert)

if __name__ == "__main__":
    asyncio.run(main())