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
# Runner
# ---------------------------------------------------------------------------

async def main():
    print("=" * 60)
    print("InoFileHelper.remove_duplicate_files tests")
    print("=" * 60)

    await test_no_duplicates()
    await test_flat_duplicates()
    await test_recursive_duplicates()
    await test_recursive_false_ignores_subfolders()
    await test_invalid_path()
    await test_empty_directory()

    print("\n" + "=" * 60)
    print(f"Results: {passed} passed, {failed} failed")
    print("=" * 60)
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    asyncio.run(main())
