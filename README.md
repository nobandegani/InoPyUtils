# InoPyUtils

A comprehensive Python utility library designed for specific use-cases including S3-compatible storage operations, media processing, file management, AI workflow orchestration, configuration management, and logging.

---

## Important Note
> **Active Development**  
> This library is under active development and is not yet feature-complete. It was built to satisfy specific use-cases and may change without warning.
>
> **Not Production-Ready**  
> Use at your own risk. Do **not** deploy this in production environments unless you fully understand its internals and have thoroughly tested it for your needs.
>
> Contributions, feedback, and issue reports are welcome—but please be cautious if you plan to rely on this library for critical workloads.

---

## Features

### 🗄️ S3-Compatible Storage (`InoS3Helper`)
- **Universal S3 Support**: Compatible with AWS S3, Backblaze B2, DigitalOcean Spaces, Wasabi, MinIO, and other S3-compatible services
- **Async Operations**: Fully asynchronous file upload/download operations
- **Automatic Retry**: Configurable retry mechanism with exponential backoff
- **Flexible Authentication**: Support for access keys, environment variables, and IAM roles
- **Advanced Features**: Object listing, existence checking, deletion, and metadata support

```python
from inopyutils import InoS3Helper

# Initialize with Backblaze B2
s3_client = InoS3Helper(
    aws_access_key_id='your_key_id',
    aws_secret_access_key='your_secret_key',
    endpoint_url='https://s3.us-west-004.backblazeb2.com',
    region_name='us-west-004',
    bucket_name='your-bucket',
    retries=5
)

# Upload and download files
await s3_client.upload_file('local_file.txt', 'remote/file.txt')
await s3_client.download_file('remote/file.txt', 'downloaded_file.txt')
```

### 📁 File Management (`InoFileHelper`)
- **Archive Operations**: ZIP compression and extraction with customizable settings
- **File Operations**: Move, copy, remove files and folders with safety checks
- **Batch Processing**: Automatic batch name incrementing for organized workflows
- **File Analysis**: Count files recursively, get last modified files
- **Media Validation**: Validate image and video files with format conversion support

```python
from inopyutils import InoFileHelper
from pathlib import Path

# Zip a folder with compression
await InoFileHelper.zip(
    to_zip=Path("source_folder"),
    path_to_save=Path("output"),
    zip_file_name="archive.zip",
    compression_level=5
)

# Copy files with renaming
InoFileHelper.copy_files(
    from_path=Path("source"),
    to_path=Path("destination"),
    rename_files=True,
    prefix_name="ProcessedFile"
)
```

### 🎨 Media Processing (`InoMediaHelper`)
- **Video Processing**: FFmpeg-based video conversion with resolution and FPS control
- **Image Processing**: Pillow-based image validation, resizing, and format conversion
- **HEIF Support**: Handle HEIF/HEIC image formats with automatic registration
- **Quality Control**: Configurable JPEG quality and resolution limits
- **Format Validation**: Comprehensive media file validation and conversion

```python
from inopyutils import InoMediaHelper
from pathlib import Path

# Convert and resize image
await InoMediaHelper.image_validate_pillow(
    input_path=Path("input.heic"),
    output_path=Path("output.jpg"),
    max_res=2048,
    jpg_quality=90
)

# Process video with resolution/FPS limits
await InoMediaHelper.video_convert_ffmpeg(
    input_path=Path("input.mp4"),
    output_path=Path("output.mp4"),
    change_res=True,
    max_res=1920,
    change_fps=True,
    max_fps=30
)
```

### ⚡ AI Workflow Management (`SparkHelper`)
- **Workflow Enumeration**: Predefined AI/ML workflows (FaceSwapper, ImageGen, VideoGen, etc.)
- **Batch Management**: Automatic batch folder creation and organization
- **Cloud Integration**: Integration with Cloudreve storage systems
- **Storage Policies**: Default storage policy management for AI workflows

```python
from inopyutils import SparkHelper, SparkWorkflows

# Parse workflow from various inputs
workflow = SparkWorkflows.parse("FaceSwapper")  # or use ID: 0

# Get batch folder for organized processing
batch_info = await SparkHelper.get_batch_folder(
    cloud_client, 
    workflow, 
    "creator_name"
)
```

### ⚙️ Configuration Management (`InoConfigHelper`)
- **INI File Support**: Read and write INI-based configuration files
- **Type Safety**: Dedicated methods for strings and booleans
- **Debug Mode**: Optional debug logging for configuration operations
- **Auto-Save**: Automatic saving after configuration changes

```python
from inopyutils import InoConfigHelper

config = InoConfigHelper('config/app.ini')

# Get configuration values with fallbacks
api_key = config.get('api', 'key', fallback='default_key')
debug_mode = config.get_bool('app', 'debug', fallback=False)

# Set configuration values
config.set('api', 'endpoint', 'https://api.example.com')
```

### 📝 Structured Logging (`InoLogHelper`)
- **JSON-Lines Format**: Structured logging in JSONL format
- **Automatic Batching**: Automatic log file batch naming and rotation
- **Categorized Logging**: INFO, WARNING, ERROR categories
- **Timestamped Entries**: ISO format timestamps for all log entries
- **Flexible Data**: Log arbitrary dictionary data with messages

```python
from inopyutils import InoLogHelper, LogCategory
from pathlib import Path

logger = InoLogHelper(Path("logs"), "MyApp")

# Log with automatic categorization
logger.add({"status": "success", "processed": 100}, "Batch completed")

# Log with explicit category
logger.add({"error": "Connection timeout"}, "API failed", LogCategory.ERROR)
```

---

## Installation

```bash
# Install from PyPI
pip install inopyutils

# Install locally for development:
git clone https://github.com/nobandegani/InoPyUtils.git
cd InoPyUtils
pip install -e .
```

### Dependencies
- **pillow**: Image processing
- **pillow_heif**: HEIF/HEIC image format support
- **opencv-python**: Video processing capabilities
- **aioboto3**: Async AWS S3 operations
- **inocloudreve**: Cloud storage integration
---

## License
Mozilla Public License Version 2.0