"""
File Helper test — remove_duplicate_files.

Run:
    python tests/file_helper/test_file_helper.py
"""

import asyncio
import shutil
import sys
from pathlib import Path

# ---------------------------------------------------------------------------
# Import
# ---------------------------------------------------------------------------

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))
from inopyutils.file_helper import InoFileHelper

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

passed = 0
failed = 0
TEST_DIR = Path(__file__).resolve().parent / "_test_workspace"


def check(name: str, result: dict, extra_check=None):
    global passed, failed
    ok = result.get("success", False)
    if ok and extra_check:
        ok = extra_check(result)
    status = "PASS" if ok else "FAIL"
    if not ok:
        failed += 1
        print(f"  [{status}] {name}")
        print(f"         {result.get('msg', result)}")
    else:
        passed += 1
        print(f"  [{status}] {name}")


def check_bool(name: str, condition: bool, fail_msg: str = ""):
    global passed, failed
    if condition:
        passed += 1
        print(f"  [PASS] {name}")
    else:
        failed += 1
        print(f"  [FAIL] {name}  {fail_msg}")


def setup():
    if TEST_DIR.exists():
        shutil.rmtree(TEST_DIR)
    TEST_DIR.mkdir(parents=True)


def teardown():
    if TEST_DIR.exists():
        shutil.rmtree(TEST_DIR)


def create_file(path: Path, content: str):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

async def test_no_duplicates():
    """All unique files — nothing should be removed."""
    print("\n── test_no_duplicates")
    setup()
    create_file(TEST_DIR / "a.txt", "aaa")
    create_file(TEST_DIR / "b.txt", "bbb")
    create_file(TEST_DIR / "c.txt", "ccc")

    res = await InoFileHelper.remove_duplicate_files(TEST_DIR)
    check("returns success", res)
    check_bool("removed_count is 0", res.get("removed_count") == 0)
    check_bool("all 3 files remain", len(list(TEST_DIR.iterdir())) == 3)
    teardown()


async def test_flat_duplicates():
    """Duplicate files in a flat directory."""
    print("\n── test_flat_duplicates")
    setup()
    create_file(TEST_DIR / "a.txt", "same content")
    create_file(TEST_DIR / "b.txt", "same content")
    create_file(TEST_DIR / "c.txt", "same content")
    create_file(TEST_DIR / "unique.txt", "different")

    res = await InoFileHelper.remove_duplicate_files(TEST_DIR, recursive=False)
    check("returns success", res)
    check_bool("removed 2 duplicates", res.get("removed_count") == 2)
    remaining = [f.name for f in TEST_DIR.iterdir() if f.is_file()]
    check_bool("2 files remain", len(remaining) == 2)
    check_bool("unique.txt kept", "unique.txt" in remaining)
    teardown()


async def test_recursive_duplicates():
    """Duplicates across subdirectories with recursive=True."""
    print("\n── test_recursive_duplicates")
    setup()
    sub = TEST_DIR / "sub"
    create_file(TEST_DIR / "a.txt", "duplicate")
    create_file(sub / "b.txt", "duplicate")
    create_file(sub / "c.txt", "unique")

    res = await InoFileHelper.remove_duplicate_files(TEST_DIR, recursive=True)
    check("returns success", res)
    check_bool("removed 1 duplicate", res.get("removed_count") == 1)
    teardown()


async def test_recursive_false_ignores_subfolders():
    """With recursive=False, duplicates in subfolders are not touched."""
    print("\n── test_recursive_false_ignores_subfolders")
    setup()
    sub = TEST_DIR / "sub"
    create_file(TEST_DIR / "a.txt", "dup")
    create_file(sub / "b.txt", "dup")

    res = await InoFileHelper.remove_duplicate_files(TEST_DIR, recursive=False)
    check("returns success", res)
    check_bool("removed_count is 0", res.get("removed_count") == 0)
    check_bool("subfolder file still exists", (sub / "b.txt").exists())
    teardown()


async def test_invalid_path():
    """Non-existent path returns error."""
    print("\n── test_invalid_path")
    fake = TEST_DIR / "nonexistent"
    res = await InoFileHelper.remove_duplicate_files(fake)
    check_bool("returns error", res.get("success") is False)


async def test_empty_directory():
    """Empty directory — nothing to remove."""
    print("\n── test_empty_directory")
    setup()
    res = await InoFileHelper.remove_duplicate_files(TEST_DIR)
    check("returns success", res)
    check_bool("removed_count is 0", res.get("removed_count") == 0)
    teardown()


# ---------------------------------------------------------------------------
# file_to_base64_data_uri tests
# ---------------------------------------------------------------------------

IMAGE_PATH = Path(__file__).resolve().parents[1] / "assets" / "image.jpg"


async def test_base64_auto_detect_jpeg():
    """Auto-detect JPEG from magic bytes."""
    print("\n── test_base64_auto_detect_jpeg")
    if not IMAGE_PATH.exists():
        print(f"  [SKIP] image.jpg not found at {IMAGE_PATH}")
        return
    res = await InoFileHelper.file_to_base64_data_uri(IMAGE_PATH)
    check("returns success", res)
    check_bool("mime is image/jpeg", res.get("mime_type") == "image/jpeg",
               f"mime_type={res.get('mime_type')}")
    check_bool("data_uri starts correctly", res.get("data_uri", "").startswith("data:image/jpeg;base64,"),
               f"data_uri prefix: {res.get('data_uri', '')[:40]}")
    check_bool("data_uri has base64 content", len(res.get("data_uri", "")) > 50)
    print(f"         mime_type: {res.get('mime_type')}")
    print(f"         data_uri length: {len(res.get('data_uri', ''))}")


async def test_base64_explicit_mime():
    """Explicit mime_type override when magic bytes don't match."""
    print("\n── test_base64_explicit_mime")
    setup()
    # Write a file with no recognizable magic bytes
    test_file = TEST_DIR / "unknown.bin"
    test_file.write_bytes(b"\x00\x01\x02\x03some data here")
    res = await InoFileHelper.file_to_base64_data_uri(test_file, mime_type="application/custom")
    check("returns success", res)
    check_bool("uses provided mime_type", res.get("mime_type") == "application/custom",
               f"mime_type={res.get('mime_type')}")
    check_bool("data_uri uses provided mime", res.get("data_uri", "").startswith("data:application/custom;base64,"))
    teardown()


async def test_base64_fallback_octet_stream():
    """No magic bytes match, no extension match, no mime_type — falls back to application/octet-stream."""
    print("\n── test_base64_fallback_octet_stream")
    setup()
    test_file = TEST_DIR / "noext"
    test_file.write_bytes(b"\x00\x01\x02\x03")
    res = await InoFileHelper.file_to_base64_data_uri(test_file)
    check("returns success", res)
    check_bool("falls back to octet-stream", res.get("mime_type") == "application/octet-stream",
               f"mime_type={res.get('mime_type')}")
    teardown()


async def test_base64_png_detection():
    """Auto-detect PNG from magic bytes."""
    print("\n── test_base64_png_detection")
    setup()
    test_file = TEST_DIR / "fake.png"
    # PNG magic bytes + dummy data
    test_file.write_bytes(b'\x89PNG\r\n\x1a\n' + b'\x00' * 20)
    res = await InoFileHelper.file_to_base64_data_uri(test_file)
    check("returns success", res)
    check_bool("mime is image/png", res.get("mime_type") == "image/png",
               f"mime_type={res.get('mime_type')}")
    teardown()


async def test_base64_nonexistent_file():
    """Non-existent file returns error."""
    print("\n── test_base64_nonexistent_file")
    res = await InoFileHelper.file_to_base64_data_uri("/nonexistent/file.jpg")
    check_bool("returns error", res.get("success") is False)
    print(f"         msg: {res.get('msg')}")


async def test_base64_roundtrip():
    """Encode and decode — verify content matches."""
    print("\n── test_base64_roundtrip")
    if not IMAGE_PATH.exists():
        print(f"  [SKIP] image.jpg not found at {IMAGE_PATH}")
        return
    import base64
    original = IMAGE_PATH.read_bytes()
    res = await InoFileHelper.file_to_base64_data_uri(IMAGE_PATH)
    check("returns success", res)
    # Extract base64 portion and decode
    data_uri = res.get("data_uri", "")
    b64_part = data_uri.split(",", 1)[1] if "," in data_uri else ""
    decoded = base64.b64decode(b64_part)
    check_bool("roundtrip matches original", decoded == original,
               f"original={len(original)} bytes, decoded={len(decoded)} bytes")


# ---------------------------------------------------------------------------
# Runner
# ---------------------------------------------------------------------------

async def main():
    print("=" * 60)
    print("InoFileHelper tests")
    print("=" * 60)

    print("\n" + "-" * 40)
    print("remove_duplicate_files")
    print("-" * 40)

    await test_no_duplicates()
    await test_flat_duplicates()
    await test_recursive_duplicates()
    await test_recursive_false_ignores_subfolders()
    await test_invalid_path()
    await test_empty_directory()

    print("\n" + "-" * 40)
    print("file_to_base64_data_uri")
    print("-" * 40)

    await test_base64_auto_detect_jpeg()
    await test_base64_explicit_mime()
    await test_base64_fallback_octet_stream()
    await test_base64_png_detection()
    await test_base64_nonexistent_file()
    await test_base64_roundtrip()

    print("\n" + "=" * 60)
    print(f"Results: {passed} passed, {failed} failed")
    print("=" * 60)
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    asyncio.run(main())
