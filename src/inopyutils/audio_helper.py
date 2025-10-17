import re
import asyncio

class InoAudioHelper:
    @staticmethod
    async def transcode_raw_pcm(
            pcm_bytes: bytes,
            output:str = "ogg",
            codec: str = "libopus",
            to_format: str = "s16le",
            application: str = "voip",
            rate: int = 16000,
            channel: int = 1,
            gain_db: float | None = None,
            limit_after_gain: bool = True,
            limit_ceiling: float = 0.98
    ) -> dict:
        args = [
            "ffmpeg",
            "-f", to_format,
            "-ar", str(rate),
            "-ac", str(channel),
            "-i", "pipe:0",
        ]

        afilters: list[str] = []
        if gain_db is not None:
            afilters.append(f"volume={gain_db}dB")
            if limit_after_gain:
                afilters.append(f"alimiter=limit={limit_ceiling}")

        if afilters:
            args += ["-filter:a", ",".join(afilters)]

        args += [
            "-c:a", codec,
            "-b:a", "24k",
            "-vbr", "on",
            "-application", application,
            "-sample_fmt", "s16",
            "-f", output,
            "pipe:1",
        ]

        process = await asyncio.create_subprocess_exec(
            *args,
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )

        out, err = await process.communicate(input=pcm_bytes)
        if process.returncode != 0:
            return {
                "success": False,
                "msg": "ffmpeg failed",
                "error_code": err.decode(),
                "data": b""
            }

        return {
            "success": True,
            "msg": "Transcode successful",
            "error_code": err.decode(),
            "data": out
        }

    @staticmethod
    async def audio_to_raw_pcm(
            audio: bytes,
            to_format: str = "s16le",
            rate: int = 16000,
            channel: int = 1,
    ) -> dict:
        """
        Convert arbitrary encoded audio bytes to raw PCM stream via ffmpeg.

        Parameters:
            audio: Input audio bytes (e.g., mp3, wav, ogg, webm, etc.).
            to_format: Raw PCM sample format for the output (e.g., "s16le", "f32le").
            rate: Target sample rate (Hz).
            channel: Number of channels (1=mono, 2=stereo).

        Returns:
            dict with keys:
                success: bool
                msg: str
                error_code: str (ffmpeg stderr)
                data: bytes (raw PCM)
        """
        # Build ffmpeg command to read from stdin and output raw PCM to stdout
        # We avoid forcing input format, letting ffmpeg auto-detect from stream headers.
        args = [
            "ffmpeg",
            "-hide_banner",
            "-nostdin",
            "-i", "pipe:0",
            "-vn",  # drop any video streams if present
            "-ar", str(rate),
            "-ac", str(channel),
            "-f", to_format,
            "pipe:1",
        ]

        process = await asyncio.create_subprocess_exec(
            *args,
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )

        out, err = await process.communicate(input=audio)
        if process.returncode != 0:
            return {
                "success": False,
                "msg": "ffmpeg failed",
                "error_code": err.decode(errors="ignore"),
                "data": b"",
            }

        return {
            "success": True,
            "msg": "Decode to raw PCM successful",
            "error_code": err.decode(errors="ignore"),
            "data": out,
        }

    @staticmethod
    async def chunks_raw_pcm(
            audio: bytes,
            chunk_size: int = 1024
    ) -> dict:
        """
        Split a raw PCM byte stream into fixed-size chunks.

        Parameters:
            audio: Raw PCM bytes.
            chunk_size: Size of each chunk in bytes.

        Returns:
            dict with keys:
                success: bool
                msg: str
                count: int (number of chunks)
                chunks: list[bytes] (chunks of raw PCM)
        """
        # Validate inputs
        if not isinstance(audio, (bytes, bytearray)):
            return {
                "success": False,
                "msg": "audio must be bytes or bytearray",
                "count": 0,
                "chunks": [],
            }
        if not isinstance(chunk_size, int) or chunk_size <= 0:
            return {
                "success": False,
                "msg": "chunk_size must be a positive integer",
                "count": 0,
                "chunks": [],
            }

        data = bytes(audio)
        chunks = [data[i:i + chunk_size] for i in range(0, len(data), chunk_size)] if data else []

        return {
            "success": True,
            "msg": "Raw PCM chunked successfully",
            "count": len(chunks),
            "chunks": chunks,
        }

    @staticmethod
    def get_audio_duration_from_text (text: str, wpm: float = 160.0) -> float:
        cleaned = re.sub(r'\s+', ' ', text).strip()
        if not cleaned:
            return 0.0

        words = len([t for t in cleaned.split(' ') if re.search(r'\w', t)])
        minutes = words / max(wpm, 1e-6)
        return minutes * 60.0

    @staticmethod
    def get_empty_audio_pcm_bytes (
            duration: int = 1,
            to_format: str = "s16le",
            rate: int = 16000,
            channel: int = 1,
    ) -> bytes:
        """
        Generate a silent raw PCM byte buffer of the requested duration.

        Parameters:
            duration: Duration in seconds (integer seconds are typical; values <= 0 yield empty bytes).
            to_format: PCM sample format string (e.g., "s16le", "f32le"). Only raw PCM formats are supported.
            rate: Sample rate (Hz), e.g., 16000.
            channel: Number of channels, e.g., 1 for mono, 2 for stereo.

        Returns:
            bytes: Raw PCM bytes representing silence for the given parameters.
        """
        # Basic validation and coercion
        try:
            dur_s = float(duration)
        except Exception:
            return b""
        if dur_s <= 0:
            return b""
        if not isinstance(rate, int) or rate <= 0:
            return b""
        if not isinstance(channel, int) or channel <= 0:
            return b""

        fmt = (to_format or "s16le").lower()

        # Map common raw PCM formats to bytes-per-sample and the silence byte value/pattern.
        # For signed PCM and IEEE float, silence is all zero bytes regardless of endianness.
        # For unsigned 8-bit PCM, silence is 0x80.
        bytes_per_sample = None
        silence_byte = 0x00

        if fmt in ("s8",):
            bytes_per_sample = 1
            silence_byte = 0x00
        elif fmt in ("u8",):
            bytes_per_sample = 1
            silence_byte = 0x80
        elif fmt in ("s16le", "s16be", "s16"):  # treat generic s16 as 2 bytes
            bytes_per_sample = 2
        elif fmt in ("s24le", "s24be", "s24"):
            bytes_per_sample = 3
        elif fmt in ("s32le", "s32be", "s32"):
            bytes_per_sample = 4
        elif fmt in ("f32le", "f32be", "f32"):
            bytes_per_sample = 4
        elif fmt in ("f64le", "f64be", "f64"):
            bytes_per_sample = 8
        else:
            # Unsupported/unknown format
            return b""

        frames = int(round(dur_s * rate))
        total_bytes = frames * channel * bytes_per_sample

        if total_bytes <= 0:
            return b""

        # For 1-byte formats, we may need a non-zero bias for silence (u8 -> 0x80).
        if bytes_per_sample == 1 and silence_byte != 0x00:
            return bytes([silence_byte]) * total_bytes

        # For multi-byte formats and signed/float 1-byte formats, silence is zeros.
        return bytes(total_bytes)
