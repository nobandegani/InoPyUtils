import asyncio
from pathlib import Path

from src.inopyutils.civitai_helper import InoCivitHelper

async def main():
    civit = InoCivitHelper(token="")

    print("Ino Civit helper test started")

    #get_model = await civit.get_model(969431)
    #print(get_model["model"])

    #get_model_version = await civit.get_model_version(1085456)
    #print(get_model_version["files"])

    download_model = await civit.download_model(
        model_path=Path("assets"),
        model_id=0,
        model_version=2477552,
        file_id=0,
        download_connections=4
    )
    print(download_model)

    download_model = await civit.download_model(
        model_path=Path("assets"),
        model_id=0,
        model_version=2477555,
        file_id=0,
        download_connections=4
    )
    print(download_model)

    await civit.close()

if __name__ == "__main__":
    asyncio.run(main())