# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

**inopyutils** is a Python utility library (published on PyPI) containing multiple independent helper classes, each for a specific use-case. Most classes use `@staticmethod` methods. Licensed under MPL-2.0, targets Python 3.9+.

## Build & Development Commands

```bash
# Install in development mode
pip install -e .

# Build distribution
python -m build
```

`tests/` and `tests_hidden/` are internal testing folders — not part of the package. Do not modify or rely on them.

## Architecture

- **Package source**: `src/inopyutils/` — all public classes exported via `__init__.py`
- **Build system**: setuptools with `pyproject.toml` (no setup.py), src-layout

### Key Patterns

- **Result envelopes**: Most methods return `{"success": bool, "msg": str, ...}` dicts. Use `ino_ok()` / `ino_err()` from `util_helper` to construct these, and `ino_is_err()` to check them. Always follow this pattern for new methods.
- **Async-first**: Most I/O operations are async (aiofiles, aioboto3, aiohttp, motor). CPU-bound work (Pillow, zipfile) runs via `asyncio.to_thread()`.
- **FFmpeg subprocess**: `InoMediaHelper` and `InoAudioHelper` shell out to `ffmpeg`/`ffprobe` via `asyncio.create_subprocess_exec`.

## Helper Classes

### Stateless (all `@staticmethod`, no instance)

- **`InoJsonHelper`** (`json_helper.py`) — JSON string/dict conversion, async+sync file read/write, deep merge, flatten/unflatten, safe dot-path get/set, compare, filter keys, remove nulls, find in arrays. All methods return `ino_ok`/`ino_err` dicts (except `is_valid` which returns bool, and `safe_get` which returns the value directly).

- **`InoFileHelper`** (`file_helper.py`) — File/folder operations: zip/unzip, copy with rename, move, remove, count files, validate media files (delegates to `InoMediaHelper`), save string to file, SHA-256 hashing. Depends on `media_helper` for `validate_files`.

- **`InoMediaHelper`** (`media_helper.py`) — Image validation/conversion via Pillow+pillow_heif (EXIF rotation fix, resize, JPEG conversion). Video conversion via ffmpeg subprocess (resolution/fps capping, libx264). Frame extraction from video. Note: `validate_video_res_fps` and `get_video_fps` are deprecated stubs.

- **`InoAudioHelper`** (`audio_helper.py`) — Raw PCM transcoding to OGG/Opus or WAV via ffmpeg pipe. Decode any audio format to raw PCM. Chunk PCM into fixed-size pieces for streaming. Estimate speech duration from text (WPM-based). Generate silent PCM buffers.

- **`InoThumbnailHelper`** (`thumbnail_helper.py`) — Generate square JPEG thumbnails at multiple sizes. Two modes: center-crop or pad with blurred background. Sync + async (via `asyncio.to_thread`). Strips EXIF metadata from output. Output naming: `{stem}_{prefix}_{size}.jpg`.

- **`InoCsvHelper`** (`csv_helper.py`) — Async CSV file read/write via aiofiles. In-memory utilities: get headers, get row/column by index/name, multi-key sort. Data always represented as `list[dict]`.

- **`InoUtilHelper`** (`util_helper.py`) — String hashing (configurable algo+length), time-based unique ID generation, UTC base32 timestamp IDs. Also contains the module-level `ino_ok()`, `ino_err()`, `ino_is_err()` functions.

- **`InoPhotoMetadata`** (`metadata_meida_helper.py`) — Dataclass holding EXIF-like photo metadata fields. Has pre-filled profiles: `iphone`, `samsung`. Used alongside your own EXIF writing pipeline.

### Stateful (hold connection/session state, need init + close)

- **`InoS3Helper`** (`s3_helper.py`) — Async S3 client wrapping aioboto3. Works with AWS S3, Backblaze B2, DigitalOcean Spaces, Wasabi, MinIO. Has retry with exponential backoff, upload/download (including folder sync), object listing, existence check, deletion, presigned URLs. Constructor takes credentials + endpoint + bucket. Also integrates `inocloudreve` for cloud storage.

- **`InoHttpHelper`** (`http_helper.py`) — Async HTTP client wrapping aiohttp. Configurable timeouts, connection limits, retries with exponential backoff on 429/5xx. Methods: get/post/put/delete/patch + `download()` with resume support, multi-connection parallel range downloads, atomic temp-file finalization. Supports `async with` context manager. Returns `ino_ok`/`ino_err` dicts with status_code, headers, data.

- **`InoCivitHelper`** (`civitai_helper.py`) — CivitAI model metadata fetch and file download with SHA-256 verification. Uses `InoHttpHelper` internally. Token from constructor or `CIVITAI_TOKEN` env var.

- **`InoMongoHelper`** (`mongo_helper.py`) — Async MongoDB helper wrapping Motor. `connect()` supports URI or host/port/username/password components. Auto-converts `_id` between str and ObjectId. Full CRUD: find_one/find_many, insert, update, delete, aggregate, count, create_index. Raises `NotInitializedError` if used before `connect()`.

- **`InoLogHelper`** (`log_helper.py`) — Async structured logger writing JSONL to `.inolog` files. Auto file rotation by size (default 10MB). Create via `await InoLogHelper.create(path, name)`. Log levels: DEBUG, INFO, WARNING, ERROR, CRITICAL (via `LogType` enum). Uses `InoFileHelper.increment_batch_name` for file rotation naming.

- **`InoConfigHelper`** (`config_helper.py`) — INI config file manager wrapping `configparser`. Constructor loads from file path. Type-safe get/get_bool with fallbacks. Sync + async set/save. Has optional debug mode for verbose logging.

## Code Patterns

### Result envelope pattern

Every method that can fail returns `ino_ok()`/`ino_err()` dicts. Extra data goes as keyword args:

```python
from .util_helper import ino_ok, ino_err, ino_is_err

# Returning success with extra data
return ino_ok("operation complete", data=result, count=5)

# Returning error
return ino_err(f"failed to process: {e}")

# Checking results
result = await some_helper_method()
if ino_is_err(result):
    return result  # propagate error up
```

### Error propagation

When a method calls another helper method, check with `ino_is_err()` and return early (propagate the error dict up):

```python
image_validate = await InoMediaHelper.image_validate_pillow(file, file)
if ino_is_err(image_validate):
    return image_validate
```

### Async file I/O

Use `aiofiles` for file read/write, create parent dirs first:

```python
import aiofiles
from pathlib import Path

path = Path(file_path)
path.parent.mkdir(parents=True, exist_ok=True)
async with aiofiles.open(path, 'w', encoding='utf-8') as f:
    await f.write(content)
```

### CPU-bound work in async context

Wrap blocking/CPU-bound operations with `asyncio.to_thread()`:

```python
await asyncio.to_thread(shutil.move, str(src), str(dest))

# Or for larger blocks of sync code, define an inner function:
def _work():
    img = Image.open(input_path)
    # ... heavy processing ...
    return result
result = await asyncio.to_thread(_work)
```

### FFmpeg subprocess pattern

Use `asyncio.create_subprocess_exec` with pipe I/O, check return code:

```python
proc = await asyncio.create_subprocess_exec(
    *args,
    stdin=asyncio.subprocess.PIPE,   # if piping input
    stdout=asyncio.subprocess.PIPE,
    stderr=asyncio.subprocess.PIPE,
)
out, err = await proc.communicate(input=input_bytes)
if proc.returncode != 0:
    return ino_err(f"ffmpeg failed: {err.decode()}")
```

### Stateful helper lifecycle

Stateful classes that hold sessions/connections follow init -> use -> close:

```python
# InoHttpHelper, InoMongoHelper support async context manager
async with InoHttpHelper(base_url="...") as client:
    resp = await client.get("/endpoint")

# Or manual lifecycle
client = InoHttpHelper(base_url="...")
resp = await client.get("/endpoint")
await client.close()

# InoLogHelper uses async factory
logger = await InoLogHelper.create(Path("logs"), "MyApp")
await logger.info("message", log_data={...})
```

### Static method class pattern

Most helpers are collections of static methods — no `self`, no instance state:

```python
class InoSomeHelper:
    @staticmethod
    async def do_something(path: Path) -> dict:
        try:
            # ... work ...
            return ino_ok("done", data=result)
        except Exception as e:
            return ino_err(f"failed: {e}")
```

### Path handling

Always use `pathlib.Path`. Create parent dirs with `mkdir(parents=True, exist_ok=True)` before writing.

### Sync + async variants

When providing both sync and async versions of a method, use `_sync` / `_async` suffix naming:

```python
@staticmethod
def save_json_as_json_sync(data, path) -> Dict: ...

@staticmethod
async def save_json_as_json_async(data, path) -> Dict: ...
```

## Module Dependencies

- `civitai_helper` -> `http_helper`, `file_helper`
- `file_helper` -> `media_helper`
- `log_helper` -> `file_helper`
- All helpers -> `util_helper` (for `ino_ok`/`ino_err`/`ino_is_err`)
