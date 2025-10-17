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