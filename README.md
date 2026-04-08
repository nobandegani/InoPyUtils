# InoPyUtils

[![Python Version](https://img.shields.io/badge/python-3.9%2B-blue)](https://python.org)
[![Version](https://img.shields.io/badge/version-1.8.3-green)](https://pypi.org/project/inopyutils/)
[![License](https://img.shields.io/badge/license-MPL--2.0-orange)](LICENSE)
[![Development Status](https://img.shields.io/badge/status-beta-yellow)](https://pypi.org/project/inopyutils/)

A comprehensive Python utility library designed for modern async workflows, featuring S3-compatible storage, HTTP client, JSON/CSV processing, media and audio handling, file/config management, structured logging, MongoDB access, CivitAI download helpers, and common utility primitives.

---

## Important Notice

> **Active Development** — This library is under active development and evolving rapidly. APIs may change without prior notice.
>
> **Beta Status** — Currently in beta. While functional, thorough testing is recommended before production use.
>
> **Contributions Welcome** — Feedback, issue reports, and pull requests are encouraged.

---

## Installation

### PyPI (Recommended)
```bash
pip install inopyutils
```

### Development
```bash
git clone https://github.com/nobandegani/InoPyUtils.git
cd InoPyUtils
pip install -e .
```

### Requirements
- **Python** 3.9+
- **FFmpeg** (optional) — required for `InoMediaHelper` video conversion and `InoAudioHelper` audio transcoding

---

## Helpers

### S3-Compatible Storage (`InoS3Helper`)

Async S3 client supporting **AWS S3**, **Backblaze B2**, **DigitalOcean Spaces**, **Wasabi**, **MinIO**, and other S3-compatible services. Features retry with exponential backoff, folder upload/download/sync, presigned URLs, and file verification.

```python
import asyncio
from inopyutils import InoS3Helper

async def main():
    async with InoS3Helper(
        aws_access_key_id="your_key_id",
        aws_secret_access_key="your_secret_key",
        endpoint_url="https://s3.us-west-004.backblazeb2.com",
        region_name="us-west-004",
        bucket_name="your-bucket",
        retries=5,
    ) as s3:
        # Upload & download (overwrite=False by default, skips if file matches)
        await s3.upload_file("local.txt", "remote/path/file.txt")
        await s3.download_file("remote/path/file.txt", "downloaded.txt")
        await s3.download_file("remote/path/file.txt", "downloaded.txt", overwrite=True)  # force re-download

        # Check existence, list objects
        exists = await s3.object_exists("remote/path/file.txt")
        objects = await s3.list_objects(prefix="remote/path/")

        # Folder operations (overwrite=False by default, skips unchanged files)
        await s3.upload_folder(s3_folder_key="remote/folder/", local_folder_path="local_folder/")
        await s3.download_folder(s3_folder_key="remote/folder/", local_folder_path="local_download/")
        await s3.sync_folder(s3_key="remote/folder/", local_folder_path="local_sync/")

        # Verify files match between local and S3
        await s3.verify_file("local.txt", "remote/path/file.txt", use_md5=True)
        await s3.verify_folder_sync(s3_folder_key="remote/folder/", local_folder_path="local_folder/")

        # Quick text/bytes
        await s3.put_text("hello", "remote/hello.txt")
        res = await s3.get_text("remote/hello.txt")
        print(res["text"])

        # Presigned download link
        link = await s3.get_download_link("remote/path/file.txt", expires_in=3600)
        print(link["url"])

asyncio.run(main())
```

---

### HTTP Client (`InoHttpHelper`)

Async HTTP client built on aiohttp with configurable timeouts, retries with exponential backoff, base URL support, auth, and file downloads with resume and multi-connection support.

```python
import asyncio
from inopyutils import InoHttpHelper

async def main():
    async with InoHttpHelper(
        base_url="https://api.example.com",
        timeout_total=30.0,
        retries=3,
        backoff_factor=0.7,
        default_headers={"User-Agent": "InoPyUtils/1.7.7"},
    ) as client:
        # GET JSON
        resp = await client.get("/users/42", json=True)

        # POST JSON
        resp = await client.post(
            "/items",
            json={"name": "Widget", "price": 9.99},
            json_response=True,
        )

        # Download file with resume support
        resp = await client.download(
            "https://example.com/file.zip",
            dest_path="downloads/",
            resume=True,
            connection=4,  # multi-connection parallel download
        )

asyncio.run(main())
```

All verb methods (`get`, `post`, `put`, `delete`, `patch`) return a dict with: `success`, `msg`, `status_code`, `headers`, `data`, `url`, `method`, `attempts`.

---

### JSON Processing (`InoJsonHelper`)

JSON manipulation toolkit with sync and async file I/O, deep merge, flatten/unflatten, dot-path access, comparison, filtering, and search.

```python
import asyncio
from inopyutils import InoJsonHelper

# String/Dict conversion
result = InoJsonHelper.string_to_dict('{"key": "value"}')
data = result["data"] if result["success"] else {}

# Deep merge
merged = InoJsonHelper.deep_merge({"a": 1, "nested": {"x": 10}}, {"b": 2, "nested": {"y": 20}})

# Flatten / unflatten
flat = InoJsonHelper.flatten({"a": {"b": {"c": 1}}})      # {"a.b.c": 1}
nested = InoJsonHelper.unflatten({"a.b.c": 1})             # {"a": {"b": {"c": 1}}}

# Safe dot-path access
value = InoJsonHelper.safe_get(data, "user.profile.name", default="Unknown")
InoJsonHelper.safe_set(data, "user.profile.age", 25)

# Compare two structures
diff = InoJsonHelper.compare({"a": 1}, {"a": 2})

# Async file I/O
async def save_and_load():
    await InoJsonHelper.save_json_as_json_async({"config": "data"}, "config.json")
    loaded = await InoJsonHelper.read_json_from_file_async("config.json")
    print(loaded["data"])

asyncio.run(save_and_load())
```

---

### CSV Utilities (`InoCsvHelper`)

Async CSV file read/write with in-memory helpers for headers, rows, columns, and sorting. Data is always `list[dict]`.

```python
import asyncio
from inopyutils import InoCsvHelper

rows = [{"id": 2, "name": "Bob"}, {"id": 1, "name": "Alice"}]

async def main():
    await InoCsvHelper.save_csv_to_file_async(rows, "people.csv")
    res = await InoCsvHelper.read_csv_from_file_async("people.csv")
    print(res["data"]["headers"], len(res["data"]["rows"]))

    # In-memory utilities
    headers = InoCsvHelper.get_headers(rows)
    first = InoCsvHelper.get_row(rows, 0)
    ids = InoCsvHelper.get_column(rows, "id")
    sorted_rows = InoCsvHelper.sort_rows(rows, by=["name", "id"])

asyncio.run(main())
```

---

### File Management (`InoFileHelper`)

File and folder operations: zip/unzip, copy with rename, move, remove, count, media validation, string-to-file, SHA-256 hashing.

```python
import asyncio
from inopyutils import InoFileHelper
from pathlib import Path

async def main():
    # ZIP compression
    await InoFileHelper.zip(
        to_zip=Path("source_folder"),
        path_to_save=Path("archives"),
        zip_file_name="backup.zip",
        compression_level=6,
    )

    # Copy and rename files
    await InoFileHelper.copy_files(
        from_path=Path("source"),
        to_path=Path("processed"),
        rename_files=True,
        prefix_name="File",
    )

    # File utilities
    count = await InoFileHelper.count_files(Path("folder"), recursive=True)
    last = InoFileHelper.get_last_file(Path("folder"))
    next_name = InoFileHelper.increment_batch_name("Batch_001")  # "Batch_002"

    # SHA-256 hash
    sha = await InoFileHelper.get_file_hash_sha_256(Path("file.bin"))
    print(sha["sha"])

asyncio.run(main())
```

---

### Media Processing (`InoMediaHelper`)

Image validation/conversion via Pillow (HEIF/HEIC supported). Video conversion via FFmpeg with resolution and FPS capping.

```python
import asyncio
from inopyutils import InoMediaHelper
from pathlib import Path

async def main():
    # Image: fix EXIF rotation, resize, convert to JPEG
    res = await InoMediaHelper.image_validate_pillow(
        input_path=Path("photo.heic"),
        output_path=Path("converted.jpg"),
        max_res=2048,
        jpg_quality=85,
    )

    # Video: convert to MP4, cap resolution and FPS
    res = await InoMediaHelper.video_convert_ffmpeg(
        input_path=Path("input.mov"),
        output_path=Path("optimized.mp4"),
        change_res=True, max_res=1920,
        change_fps=True, max_fps=30,
    )

    # Extract a frame from video
    res = await InoMediaHelper.video_extract_frame(
        input_path=Path("video.mp4"),
        output_path=Path("frame.jpg"),
    )

asyncio.run(main())
```

---

### Audio Processing (`InoAudioHelper`)

Audio utilities for raw PCM: transcode to OGG/Opus or WAV, decode any format to PCM, chunk for streaming, estimate duration, generate silence. Requires FFmpeg.

```python
import asyncio
from inopyutils import InoAudioHelper

async def main():
    with open("audio.ogg", "rb") as f:
        ogg_bytes = f.read()

    # Decode to raw PCM
    dec = await InoAudioHelper.audio_to_raw_pcm(ogg_bytes, rate=16000, channel=1)
    pcm = dec["data"]

    # Transcode PCM to OGG/Opus
    enc = await InoAudioHelper.transcode_raw_pcm(
        pcm, output="ogg", codec="libopus", rate=16000, channel=1,
    )

    # Chunk for streaming
    chunks = await InoAudioHelper.chunks_raw_pcm(pcm, chunk_size=3200)

    # Utilities (no FFmpeg needed)
    seconds = InoAudioHelper.get_audio_duration_from_text("Hello world", wpm=160.0)
    silence = InoAudioHelper.get_empty_audio_pcm_bytes(duration=2, rate=16000, channel=1)

asyncio.run(main())
```

---

### Thumbnail Generation (`InoThumbnailHelper`)

Generate square JPEG thumbnails at multiple sizes. Center-crop or pad with blurred background. Strips EXIF metadata.

```python
import asyncio
from pathlib import Path
from inopyutils import InoThumbnailHelper

# Sync
res = InoThumbnailHelper.image_generate_square_thumbnails(
    image_path=Path("photo.jpg"),
    output_dir=Path("thumbnails/"),
    sizes=(256, 512, 1024),
    quality=85,
    crop=False,  # True: center-crop, False: blurred background padding
)

# Async
async def main():
    res = await InoThumbnailHelper.image_generate_square_thumbnails_async(
        image_path=Path("photo.heic"),
        output_dir=Path("thumbnails/"),
        sizes=(256, 768),
        crop=True,
    )

asyncio.run(main())
```

---

### MongoDB Helper (`InoMongoHelper`)

Async MongoDB helper wrapping Motor. Initialize once, use everywhere. Auto-converts `_id` between str and ObjectId.

```python
import asyncio
from inopyutils import InoMongoHelper

mongo = InoMongoHelper()

async def main():
    await mongo.connect(
        uri="mongodb://localhost:27017",
        db_name="mydb",
        check_connection=True,
    )

    # CRUD
    res = await mongo.insert_one("users", {"name": "Ann"})
    user = await mongo.find_one("users", {"_id": res["inserted_id"]})
    await mongo.update_one("users", {"_id": res["inserted_id"]}, {"$set": {"name": "Anna"}})
    await mongo.delete_one("users", {"_id": res["inserted_id"]})

    await mongo.close()

asyncio.run(main())
```

---

### CivitAI Integration (`InoCivitHelper`)

Async helper for fetching CivitAI model metadata and downloading model files with SHA-256 verification and multi-connection resume support.

```python
import asyncio
from pathlib import Path
from inopyutils import InoCivitHelper

async def main():
    civit = InoCivitHelper(token="your_civitai_token")
    try:
        res = await civit.download_model(
            model_path=Path("./models"),
            model_id=123,
            model_version=456,
            file_id=0,
            chunk_size=8,
            download_connections=6,
        )
        print(res)
    finally:
        await civit.close()

asyncio.run(main())
```

Token can also be set via `CIVITAI_TOKEN` environment variable.

---

### OpenAI-Compatible Chat (`InoOpenAIHelper`)

Async chat completions client using the OpenAI SDK. Works with any OpenAI-compatible endpoint (RunPod, vLLM, Ollama, OpenAI, etc.). Supports vision/image input.

```python
import asyncio
from inopyutils import InoOpenAIHelper

async def main():
    # Text chat
    res = await InoOpenAIHelper.chat_completions(
        api_key="your_api_key",
        base_url="https://api.runpod.ai/v2/YOUR_ENDPOINT/openai/v1",
        model="qwen3-vl-32b",
        user_prompt="What is quantum computing?",
        system_prompt="You are a helpful assistant. Reply concisely.",
        temperature=0.7,
        max_tokens=256,
    )
    if res["success"]:
        print(res["response"])

    # Vision with image
    res = await InoOpenAIHelper.chat_completions(
        api_key="your_api_key",
        base_url="https://api.runpod.ai/v2/YOUR_ENDPOINT/openai/v1",
        model="qwen3-vl-32b",
        user_prompt="Describe this image.",
        image="https://example.com/photo.jpg",  # URL or data:image/jpeg;base64,...
    )

asyncio.run(main())
```

---

### RunPod Serverless vLLM (`InoRunpodHelper`)

Async helper for RunPod serverless vLLM endpoints via the runsync API. Uses the OpenAI-compatible route for proper vision/multimodal support.

```python
import asyncio
from inopyutils import InoRunpodHelper

async def main():
    # Text chat
    res = await InoRunpodHelper.serverless_vllm_runsync(
        url="https://api.runpod.ai/v2/YOUR_ENDPOINT/runsync",
        api_key="your_runpod_api_key",
        model="qwen3-vl-32b",
        user_prompt="List 3 benefits of drinking water.",
        system_prompt="You are a helpful assistant.",
        temperature=0.3,
        max_tokens=256,
    )
    if res["success"]:
        print(res["response"])
        print(f"Execution time: {res['execution_time']}ms")

    # Vision with image
    res = await InoRunpodHelper.serverless_vllm_runsync(
        url="https://api.runpod.ai/v2/YOUR_ENDPOINT/runsync",
        api_key="your_runpod_api_key",
        model="qwen3-vl-32b",
        user_prompt="What do you see in this image?",
        image="data:image/jpeg;base64,/9j/4AAQ...",
    )

asyncio.run(main())
```

---

### Configuration Management (`InoConfigHelper`)

INI config file manager with type-safe access, fallbacks, and sync/async save.

```python
import asyncio
from inopyutils import InoConfigHelper

config = InoConfigHelper("config/app.ini")

# Read with type safety
url = config.get("database", "url", fallback="sqlite:///default.db")
debug = config.get_bool("app", "debug", fallback=False)

# Write (sync)
config.set("api", "endpoint", "https://api.prod.com")

# Write (async)
async def main():
    await config.set_async("features", "cache_enabled", True)
    await config.save_async()

asyncio.run(main())
```

---

### Structured Logging (`InoLogHelper`)

Async JSONL logger with automatic file rotation by size. Log levels: DEBUG, INFO, WARNING, ERROR, CRITICAL.

```python
import asyncio
from inopyutils import InoLogHelper, LogType
from pathlib import Path

async def main():
    logger = await InoLogHelper.create(Path("logs"), "MyApp")

    await logger.info(
        msg="User login",
        log_data={"user_id": 123, "ip": "192.168.1.1"},
        source="auth",
    )

    await logger.error(
        msg="Request failed",
        log_data={"status": 500, "endpoint": "/api/users"},
        source="api",
    )

asyncio.run(main())
```

Output files: `logs/MyApp_00001.inolog` (rotates at 10MB by default).

---

### Photo Metadata (`InoPhotoMetadata`)

Dataclass for EXIF-like photo metadata with pre-filled profiles (`iphone`, `samsung`).

```python
from inopyutils import InoPhotoMetadata

meta = InoPhotoMetadata(profile="iphone")
meta.iso_speed = 100
meta.gps_latitude = 37.7749
meta.gps_longitude = -122.4194
```

---

### Utility Helpers (`ino_ok`, `ino_err`, `ino_is_err`, `InoUtilHelper`)

Result envelope primitives used by all helpers, plus common ID/hash utilities.

```python
from inopyutils import ino_ok, ino_err, ino_is_err, InoUtilHelper

res = ino_ok("done", value=42)
print(res["success"], res["value"])  # True 42

if ino_is_err(ino_err("failed")):
    print("error path")

digest = InoUtilHelper.hash_string("hello", algo="sha256", length=16)
uid = InoUtilHelper.generate_unique_id_by_time()
stamp = InoUtilHelper.get_date_time_utc_base64()
```

---

## Dependencies

| Package | Purpose |
|---------|---------|
| pillow | Image processing |
| pillow_heif | HEIF/HEIC format support |
| aioboto3 | Async S3 operations |
| aiofiles | Async file I/O |
| aiohttp | Async HTTP client |
| botocore / boto3 | AWS SDK |
| motor | Async MongoDB driver |
| openai | OpenAI-compatible API client |
| inocloudreve | Cloud storage integration |

**Optional:** FFmpeg — required for `InoMediaHelper` video conversion and `InoAudioHelper` audio transcoding.

---

## Project Info

- **Version**: 1.8.3
- **Status**: Beta
- **Python**: 3.9+
- **License**: [Mozilla Public License 2.0](LICENSE)
- **Homepage**: [github.com/nobandegani/InoPyUtils](https://github.com/nobandegani/InoPyUtils)
- **Issues**: [github.com/nobandegani/InoPyUtils/issues](https://github.com/nobandegani/InoPyUtils/issues)
- **PyPI**: [pypi.org/project/inopyutils](https://pypi.org/project/inopyutils/)
- **Contact**: contact@inoland.net
