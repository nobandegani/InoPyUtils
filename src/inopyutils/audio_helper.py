import asyncio

class InoAudioHelper:
    @staticmethod
    async def transcode_raw_pcm(
            pcm_bytes: bytes,
            output:str = "ogg",
            codec: str = "libopus",
            application: str = "voip",
            rate: int = 16000,
            channel: int = 1,
            gain_db: float | None = None
    ) -> dict:
        args = [
            "ffmpeg",
            "-f", "s16le",
            "-ar", str(rate),
            "-ac", str(channel),
            "-i", "pipe:0",
        ]

        if gain_db is not None:
            args += ["-filter:a", f"volume={gain_db}dB"]

        args += [
            "-c:a", codec,
            "-b:a", "24k",
            "-vbr", "on",
            "-application", application,
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
