import asyncio
import os
import shutil
import tempfile
import wave
import sys
import subprocess

from src.inopyutils import InoAudioHelper

AUDIO_PATH = os.path.join(os.path.dirname(__file__), "audio.ogg")


async def play_pcm_ffplay(pcm_bytes: bytes, rate: int = 16000, channel: int = 1, fmt: str = "s16le") -> bool:
    """Play raw PCM using ffplay if available. Returns True if attempted."""
    ffplay = shutil.which("ffplay")
    if not ffplay:
        return False

    # Use asyncio subprocess to avoid blocking the event loop
    proc = await asyncio.create_subprocess_exec(
        ffplay,
        "-autoexit",
        "-nodisp",
        "-loglevel", "error",
        "-f", fmt,
        "-ar", str(rate),
        "-ac", str(channel),
        "-i", "pipe:0",
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    await proc.communicate(input=pcm_bytes)
    return True


def play_pcm_winsound_fallback(pcm_bytes: bytes, rate: int = 16000, channel: int = 1):
    """Fallback: wrap PCM into a temporary WAV and play using winsound on Windows."""
    try:
        import winsound  # only available on Windows
    except Exception:
        return False

    # Create a temporary WAV file
    with tempfile.NamedTemporaryFile(delete=False, suffix=".wav") as tmp:
        tmp_path = tmp.name
    try:
        with wave.open(tmp_path, 'wb') as wf:
            wf.setnchannels(channel)
            wf.setsampwidth(2)  # s16le -> 2 bytes per sample
            wf.setframerate(rate)
            wf.writeframes(pcm_bytes)
        winsound.PlaySound(tmp_path, winsound.SND_FILENAME)
        return True
    except Exception:
        return False
    finally:
        try:
            os.remove(tmp_path)
        except Exception:
            pass


async def play_file_ffplay(file_path: str) -> bool:
    """Play a source audio file directly using ffplay if available."""
    ffplay = shutil.which("ffplay")
    if not ffplay:
        return False
    proc = await asyncio.create_subprocess_exec(
        ffplay,
        "-autoexit",
        "-nodisp",
        "-loglevel", "error",
        file_path,
        stdin=asyncio.subprocess.DEVNULL,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    await proc.communicate()
    return True


def play_file_via_winsound_transcode(file_path: str, rate: int = 16000, channel: int = 1) -> bool:
    """Fallback for original file: transcode to a temp WAV via ffmpeg and play with winsound (Windows)."""
    try:
        import winsound
    except Exception:
        return False

    ffmpeg = shutil.which("ffmpeg")
    if not ffmpeg:
        return False

    with tempfile.NamedTemporaryFile(delete=False, suffix=".wav") as tmp:
        tmp_path = tmp.name
    try:
        # Transcode the source file to WAV on disk
        args = [
            ffmpeg, "-y",
            "-i", file_path,
            "-ar", str(rate),
            "-ac", str(channel),
            tmp_path,
        ]
        proc = subprocess.run(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        if proc.returncode != 0:
            return False
        winsound.PlaySound(tmp_path, winsound.SND_FILENAME)
        return True
    except Exception:
        return False
    finally:
        try:
            os.remove(tmp_path)
        except Exception:
            pass


async def main():
    """Main manual test function: preflight play original -> load bytes -> raw PCM -> play."""
    print("AudioHelper Compatibility Test Suite")
    if not os.path.exists(AUDIO_PATH):
        print(f"Audio file not found: {AUDIO_PATH}")
        sys.exit(1)

    # Preflight: try to play the original audio BEFORE using the helper function
    print("\n[Preflight] Attempting to play the original audio file directly...")
    preflight_played = await play_file_ffplay(AUDIO_PATH)
    if not preflight_played:
        preflight_played = play_file_via_winsound_transcode(AUDIO_PATH, rate=16000, channel=1)

    if preflight_played:
        print("[Preflight] Playback attempted for original audio. If you heard audio, your playback path works.")
    else:
        print("[Preflight] Could not attempt direct playback. Ensure ffplay (from FFmpeg) is in PATH or you are on Windows with winsound available.")

    # Load the file and convert to raw PCM via helper
    with open(AUDIO_PATH, "rb") as f:
        src_bytes = f.read()

    result = await InoAudioHelper.audio_to_raw_pcm(
        audio=src_bytes,
        rate=16000,
        channel=1,
        to_format="s16le",
    )

    print(f"\n[Decode] Success: {result['success']}")
    if not result["success"]:
        print("[Decode] ffmpeg stderr:\n" + result["error_code"][:500])
        return

    pcm = result["data"]
    print(f"[Decode] Decoded PCM bytes: {len(pcm):,}")

    # Try to play via ffplay; fallback to winsound on Windows
    print("\n[Post-decode] Attempting to play decoded PCM...")
    played = await play_pcm_ffplay(pcm, rate=16000, channel=1, fmt="s16le")
    if not played:
        played = play_pcm_winsound_fallback(pcm, rate=16000, channel=1)

    if played:
        print("[Post-decode] Playback attempted. If you heard audio, decoding works.")
    else:
        print("[Post-decode] Playback not available (no ffplay/winsound). Raw PCM decoding still verified.")


if __name__ == "__main__":
    asyncio.run(main())