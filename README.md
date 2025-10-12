# InoPyUtils

[![Python Version](https://img.shields.io/badge/python-3.9%2B-blue)](https://python.org)
[![Version](https://img.shields.io/badge/version-1.1.7-green)](https://pypi.org/project/inopyutils/)
[![License](https://img.shields.io/badge/license-MPL--2.0-orange)](LICENSE)
[![Development Status](https://img.shields.io/badge/status-beta-yellow)](https://pypi.org/project/inopyutils/)

A comprehensive Python utility library designed for modern development workflows, featuring S3-compatible storage operations, advanced JSON processing, media handling, file management, configuration management, and structured logging.

---

## 🚨 Important Notice

> **⚠️ Active Development**  
> This library is under active development and evolving rapidly. Built to satisfy specific use-cases, APIs may change without prior notice.
>
> **🔬 Beta Status**  
> Currently in **beta** stage. While functional, thorough testing is recommended before production use. Please review the code and test extensively for your specific requirements.
>
> **🤝 Community Welcome**  
> Contributions, feedback, and issue reports are actively encouraged. Help us make this library better for everyone!

---

## ✨ Key Features

### 🗄️ S3-Compatible Storage (`InoS3Helper`)
Universal cloud storage solution supporting **AWS S3**, **Backblaze B2**, **DigitalOcean Spaces**, **Wasabi**, **MinIO**, and other S3-compatible services.

**Features:**
- **Fully Async Operations** - Non-blocking upload/download operations
- **Smart Retry Logic** - Configurable exponential backoff retry mechanism
- **Flexible Authentication** - Access keys, environment variables, IAM roles
- **Advanced Operations** - Object listing, existence checking, deletion, metadata handling
- **Batch Operations** - Efficient bulk file operations

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

# Async file operations
await s3_client.upload_file('local_file.txt', 'remote/path/file.txt')
await s3_client.download_file('remote/path/file.txt', 'downloaded_file.txt')

# Check file existence and get metadata
exists = await s3_client.file_exists('remote/path/file.txt')
objects = await s3_client.list_objects(prefix='remote/path/')
```

---

### 🔧 Advanced JSON Processing (`InoJsonHelper`)
Comprehensive JSON manipulation toolkit with both synchronous and asynchronous operations, perfect for configuration management and data processing.

**Features:**
- **Async/Sync File Operations** - Both synchronous and asynchronous file I/O
- **Deep Data Manipulation** - Merge, flatten, unflatten complex nested structures
- **Advanced Querying** - Safe path-based data retrieval and modification
- **Data Comparison** - Intelligent JSON structure comparison with detailed differences
- **Filtering & Cleaning** - Remove null values, filter keys, clean data structures
- **Array Search** - Find specific elements in complex nested arrays

```python
from inopyutils import InoJsonHelper

# String/Dict conversions with error handling
result = InoJsonHelper.string_to_dict('{"key": "value"}')
if result["success"]:
    data = result["data"]

# Async file operations
await InoJsonHelper.save_json_as_json_async({"config": "data"}, "config.json")
loaded = await InoJsonHelper.read_json_from_file_async("config.json")

# Deep operations
merged = InoJsonHelper.deep_merge(dict1, dict2)
flattened = InoJsonHelper.flatten({"a": {"b": {"c": 1}}})  # {"a.b.c": 1}
original = InoJsonHelper.unflatten({"a.b.c": 1})  # {"a": {"b": {"c": 1}}}

# Safe path operations
value = InoJsonHelper.safe_get(data, "user.profile.name", default="Unknown")
InoJsonHelper.safe_set(data, "user.profile.age", 25)

# Advanced filtering and searching
cleaned = InoJsonHelper.remove_null_values(data, remove_empty=True)
filtered = InoJsonHelper.filter_keys(data, ["name", "email"], deep=True)
found = InoJsonHelper.find_field_from_array(data, "id", "user_123")

# Data comparison with detailed diff
differences = InoJsonHelper.compare(old_data, new_data)
```

---

### 📁 File Management (`InoFileHelper`)
Robust file and folder operations with advanced features for batch processing, archiving, and media validation.

**Features:**
- **Smart Archiving** - ZIP compression/extraction with customizable settings
- **Batch Processing** - Automatic batch naming and file organization
- **Safe Operations** - Move, copy, remove with comprehensive safety checks
- **Media Validation** - Validate and convert image/video files with format support
- **Recursive Operations** - Deep folder analysis and processing

```python
from inopyutils import InoFileHelper
from pathlib import Path

# Create compressed archives
await InoFileHelper.zip(
    to_zip=Path("source_folder"),
    path_to_save=Path("archives"),
    zip_file_name="backup.zip",
    compression_level=6,
    include_root=False
)

# Batch file operations with smart naming
InoFileHelper.copy_files(
    from_path=Path("source"),
    to_path=Path("processed"),
    rename_files=True,
    prefix_name="Processed_",
    iterate_subfolders=True
)

# File analysis and utilities
file_count = InoFileHelper.count_files(Path("folder"), recursive=True)
latest_file = InoFileHelper.get_last_file(Path("folder"))
batch_name = InoFileHelper.increment_batch_name("Batch_001")  # "Batch_002"

# Media validation and conversion
await InoFileHelper.validate_files(
    input_path=Path("media_folder"),
    include_image=True,
    include_video=True,
    image_valid_exts=['.jpg', '.png', '.heic'],
    video_valid_exts=['.mp4', '.mov']
)
```

---

### 🎨 Media Processing (`InoMediaHelper`)
Professional-grade media processing with FFmpeg integration and Pillow-based image manipulation.

**Features:**
- **Video Processing** - FFmpeg-based conversion with resolution/FPS control
- **Image Processing** - Pillow-based validation, resizing, format conversion
- **HEIF/HEIC Support** - Native support for modern image formats
- **Quality Control** - Configurable compression and resolution limits
- **Batch Operations** - Process multiple files efficiently

```python
from inopyutils import InoMediaHelper
from pathlib import Path

# Advanced image processing
await InoMediaHelper.image_validate_pillow(
    input_path=Path("photo.heic"),
    output_path=Path("converted.jpg"),
    max_res=2048,
    jpg_quality=85,
    png_compress_level=6
)

# Video processing with quality control
await InoMediaHelper.video_convert_ffmpeg(
    input_path=Path("input.mov"),
    output_path=Path("optimized.mp4"),
    change_res=True,
    max_res=1920,
    change_fps=True,
    max_fps=30
)

# Media validation
is_valid_image = await InoMediaHelper.validate_image(Path("image.jpg"))
is_valid_video = await InoMediaHelper.validate_video(Path("video.mp4"))
```

---

### ⚙️ Configuration Management (`InoConfigHelper`)
Robust INI-based configuration management with type safety and debugging capabilities.

**Features:**
- **Type-Safe Operations** - Dedicated methods for different data types
- **Fallback Support** - Graceful handling of missing configuration values
- **Debug Logging** - Optional verbose logging for troubleshooting
- **Auto-Save** - Automatic persistence of configuration changes

```python
from inopyutils import InoConfigHelper

# Initialize with debug logging
config = InoConfigHelper('config/application.ini', debug=True)

# Type-safe configuration access
database_url = config.get('database', 'url', fallback='sqlite:///default.db')
debug_mode = config.get_bool('app', 'debug', fallback=False)
max_connections = config.get_int('database', 'max_connections', fallback=10)

# Configuration updates
config.set('api', 'endpoint', 'https://api.production.com')
config.set_bool('features', 'cache_enabled', True)
config.save()  # Explicit save (or auto-save if configured)
```

---

### 📝 Structured Logging (`InoLogHelper`)
Advanced logging system with automatic batching, categorization, and JSON-Lines format output.

**Features:**
- **JSONL Format** - Machine-readable structured logging
- **Automatic Batching** - Smart log rotation and batch management
- **Categorized Logging** - INFO, WARNING, ERROR categories with filtering
- **Rich Context** - Log arbitrary data structures with messages
- **Timestamped** - ISO format timestamps for precise tracking

```python
from inopyutils import InoLogHelper, LogCategory
from pathlib import Path

# Initialize logger with automatic batching
logger = InoLogHelper(Path("logs"), "MyApplication")

# Context-rich logging
logger.add(
    {"user_id": 12345, "action": "login", "ip": "192.168.1.100"}, 
    "User login successful"
)

# Categorized logging
logger.add(
    {"error_code": 500, "endpoint": "/api/users", "duration_ms": 1200}, 
    "API endpoint timeout", 
    LogCategory.ERROR
)

# Batch processing logs
logger.add(
    {"processed": 150, "failed": 3, "batch_id": "batch_20241009"}, 
    "Batch processing completed",
    LogCategory.INFO
)
```

---

## 🚀 Installation

### PyPI Installation (Recommended)
```bash
pip install inopyutils
```

### Development Installation
```bash
# Clone the repository
git clone https://github.com/nobandegani/InoPyUtils.git
cd InoPyUtils

# Install in development mode
pip install -e .

# Install with development dependencies
pip install -e ".[dev]"
```

### System Requirements
- **Python**: 3.9 or higher
- **Operating System**: Cross-platform (Windows, macOS, Linux)
- **Optional**: FFmpeg (for video processing features)

---

## 📦 Dependencies

### Core Dependencies
- **pillow** - Image processing and manipulation
- **pillow_heif** - HEIF/HEIC image format support
- **opencv-python** - Advanced video processing capabilities
- **aioboto3** - Asynchronous AWS S3 operations
- **aiofiles** - Asynchronous file I/O operations
- **botocore** - AWS core functionality and exception handling
- **inocloudreve** - Extended cloud storage integration

### Optional Dependencies
- **FFmpeg** - Required for video processing features (install separately)

---

## 🛠️ Development & Contributing

### Development Setup
```bash
# Clone and setup
git clone https://github.com/nobandegani/InoPyUtils.git
cd InoPyUtils

# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install in development mode
pip install -e ".[dev]"

# Run tests
python -m pytest tests/
```

### Contributing Guidelines
1. **Fork** the repository
2. **Create** a feature branch (`git checkout -b feature/amazing-feature`)
3. **Test** your changes thoroughly
4. **Commit** your changes (`git commit -m 'Add amazing feature'`)
5. **Push** to the branch (`git push origin feature/amazing-feature`)
6. **Open** a Pull Request

---

## 📊 Project Status

- **Current Version**: 1.1.3
- **Development Status**: Beta
- **Python Support**: 3.9+
- **License**: Mozilla Public License 2.0
- **Maintenance**: Actively maintained

---

## 📞 Support & Links

- **Homepage**: [https://github.com/nobandegani/InoPyUtils](https://github.com/nobandegani/InoPyUtils)
- **Issues**: [https://github.com/nobandegani/InoPyUtils/issues](https://github.com/nobandegani/InoPyUtils/issues)
- **PyPI**: [https://pypi.org/project/inopyutils/](https://pypi.org/project/inopyutils/)
- **Contact**: contact@inoland.net

---

## 📄 License

This project is licensed under the **Mozilla Public License 2.0** (MPL-2.0). See the [LICENSE](LICENSE) file for details.

---

## 🙏 Acknowledgments

Built with ❤️ by the Inoland. Special thanks to all contributors and the open-source community for their invaluable tools and libraries that make this project possible.