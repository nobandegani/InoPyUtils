import asyncio
import os
import shutil
import tempfile
import wave
import sys
import subprocess
from array import array
import math

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


def pcm_int16_stats(pcm_bytes: bytes, channel: int = 1) -> dict:
    """Compute simple stats for s16le PCM. Returns dict with duration, min, max, peak, rms."""
    if not pcm_bytes:
        return {"samples": 0, "duration_s": 0.0, "min": 0, "max": 0, "peak": 0, "rms": 0.0}
    buf = array('h')
    buf.frombytes(pcm_bytes)
    # array('h') uses native endianness; on little-endian systems this matches s16le
    # If running on big-endian, swap bytes
    if array('h').itemsize == 2 and sys.byteorder != 'little':
        buf.byteswap()
    n = len(buf)
    if n == 0:
        return {"samples": 0, "duration_s": 0.0, "min": 0, "max": 0, "peak": 0, "rms": 0.0}
    mn = min(buf)
    mx = max(buf)
    peak = max(abs(mn), abs(mx))
    # RMS
    ssum = 0
    for v in buf:
        ssum += v * v
    rms = math.sqrt(ssum / n)
    # Estimate duration from bytes for 16kHz mono s16: 2 bytes per sample per channel
    samples_per_channel = n // channel
    duration_s = samples_per_channel / 16000.0
    return {"samples": n, "duration_s": duration_s, "min": mn, "max": mx, "peak": peak, "rms": rms}


def write_wav_and_play(pcm_bytes: bytes, rate: int = 16000, channel: int = 1) -> bool:
    """Write PCM to a temp WAV, then attempt playback via ffplay, then winsound."""
    tmp_path = None
    try:
        with tempfile.NamedTemporaryFile(delete=False, suffix=".wav") as tmp:
            tmp_path = tmp.name
        with wave.open(tmp_path, 'wb') as wf:
            wf.setnchannels(channel)
            wf.setsampwidth(2)
            wf.setframerate(rate)
            wf.writeframes(pcm_bytes)
        # Try ffplay on the WAV
        try:
            # Reuse the existing async function by running a short event loop if needed is complex here.
            # Use synchronous subprocess for simplicity.
            ffplay = shutil.which("ffplay")
            if ffplay:
                proc = subprocess.run([ffplay, "-autoexit", "-nodisp", "-loglevel", "error", tmp_path])
                if proc.returncode == 0:
                    return True
        except Exception:
            pass
        # Fallback to winsound
        try:
            import winsound
            winsound.PlaySound(tmp_path, winsound.SND_FILENAME)
            return True
        except Exception:
            return False
    finally:
        if tmp_path:
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

    # Stats to determine if PCM is silent or low-level
    stats = pcm_int16_stats(pcm, channel=1)
    print(f"[Decode] Stats: samples={stats['samples']:,}, duration≈{stats['duration_s']:.2f}s, min={stats['min']}, max={stats['max']}, peak={stats['peak']}, rms={stats['rms']:.1f}")
    if stats['peak'] == 0 or stats['rms'] < 50:
        print("[Warn] PCM appears near-silent. This suggests an upstream decode issue or a very quiet source.")

    # Try to play via ffplay; fallback to winsound on Windows
    print("\n[Post-decode] Attempting to play decoded PCM via stdin -> ffplay...")
    played = await play_pcm_ffplay(pcm, rate=16000, channel=1, fmt="s16le")

    # Alternate path: write WAV and play, to isolate stdin issues
    print("[Post-decode] Also attempting alternate playback: write WAV -> play...")
    alt_played = write_wav_and_play(pcm, rate=16000, channel=1)

    if played or alt_played:
        print("[Post-decode] Playback attempted (stdin and/or WAV path). If you heard audio, decoding works.")
    else:
        print("[Post-decode] Playback not available (no ffplay/winsound). Raw PCM decoding still verified.")

    # Optional: round-trip encode the PCM to OGG and try to play it to confirm validity end-to-end
    try:
        rt = await InoAudioHelper.transcode_raw_pcm(pcm, output="ogg", codec="libopus", to_format="s16le", rate=16000, channel=1)
        if rt.get('success') and rt.get('data'):
            with tempfile.NamedTemporaryFile(delete=False, suffix=".ogg") as ogg_tmp:
                ogg_path = ogg_tmp.name
                ogg_tmp.write(rt['data'])
            try:
                print("\n[Round-trip] Attempting to play re-encoded OGG via ffplay...")
                await play_file_ffplay(ogg_path)
            finally:
                try:
                    os.remove(ogg_path)
                except Exception:
                    pass
        else:
            print("[Round-trip] Transcode back to OGG failed; skipping playback.")
    except Exception as e:
        print(f"[Round-trip] Skipped due to error: {e}")


if __name__ == "__main__":
    asyncio.run(main())