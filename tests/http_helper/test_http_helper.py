import asyncio
import aiohttp
from pathlib import Path

from src.inopyutils import InoFileHelper
from src.inopyutils.http_helper import InoHttpHelper

async def main():
    """Main test function configured for very large file downloads"""
    print("HTTP Client Test Suite – Large File Download")

    headers = {
        "Authorization": "Bearer "
    }

    # Configure the HTTP client for massive downloads (e.g., 40GB+)
    http_client = InoHttpHelper(
        timeout_total=None,
        timeout_sock_read=None,
        timeout_connect=30.0,
        timeout_sock_connect=30.0,
        retries=5,
        backoff_factor=1.0,
    )

    # Optional: simple progress reporter (prints bytes and percentage when total known)
    def progress(downloaded: int, total: int | None) -> None:
        if total:
            pct = downloaded * 100 / total
            # Print occasionally to avoid excessive console spam
            if downloaded == 0 or downloaded % (50 * 1024 * 1024) < 8 * 1024 * 1024:  # ~every 50MB
                print(f"Downloaded: {downloaded}/{total} bytes ({pct:.2f}%)")
        else:
            if downloaded % (50 * 1024 * 1024) < 8 * 1024 * 1024:
                print(f"Downloaded: {downloaded} bytes")

    # Use a directory for dest_path; filename can be auto-derived from headers/URL
    download_file = await http_client.download(
        url="https://civitai.com/api/download/models/1413133",
        headers=headers,
        dest_path=r"E:\NIL\tests",  # directory to save into
        chunk_size=32 * 1024 * 1024,  # 8 MB chunks for better throughput
        resume=True,                 # enable resume to tolerate interruptions
        overwrite=False,             # avoid clobbering existing files
        progress=progress,           # optional progress reporting
        allow_redirects=True,
        mkdirs=True,
        verify_size=True,
    )
    print(f"Download file result: {download_file}")

    await http_client.close()
if __name__ == "__main__":
    asyncio.run(main())