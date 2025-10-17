import asyncio

from src.inopyutils import InoAudioHelper

file = "audio.ogg"


async def main():
    """Main test function"""
    print("AudioHelper Compatibility Test Suite")

    audio_raw = await InoAudioHelper.audio_to_raw_pcm(
        audio=,
        rate=16000,
        channel=1,
        to_format="s16le"
    )

if __name__ == "__main__":
    asyncio.run(main())