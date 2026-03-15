"""
S3 Helper integration test.

Reads credentials from .env at project root. Create a .env file like:

    S3_ACCESS_KEY_ID=your_key_id
    S3_SECRET_ACCESS_KEY=your_secret_key
    S3_ENDPOINT_URL=https://s3.us-west-004.backblazeb2.com
    S3_REGION_NAME=us-west-004
    S3_BUCKET_NAME=your-bucket
    S3_TEST_ROOT=inopyutils-test

Run:
    python tests/s3_helper/test_s3_helper.py
"""

import asyncio
import hashlib
import os
import shutil
from pathlib import Path

# ---------------------------------------------------------------------------
# Configuration — credentials from .env, paths from constants
# ---------------------------------------------------------------------------

def _load_env():
    """Load .env from project root into os.environ (simple key=value parser)."""
    env_path = Path(__file__).resolve().parents[2] / ".env"
    if not env_path.exists():
        return
    with open(env_path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, _, value = line.partition("=")
            key = key.strip()
            value = value.strip().strip("'\"")
            if key:
                os.environ.setdefault(key, value)

_load_env()

S3_ACCESS_KEY_ID = os.environ.get("S3_ACCESS_KEY_ID", "")
S3_SECRET_ACCESS_KEY = os.environ.get("S3_SECRET_ACCESS_KEY", "")
S3_ENDPOINT_URL = os.environ.get("S3_ENDPOINT_URL", "")
S3_REGION_NAME = os.environ.get("S3_REGION_NAME", "us-east-1")
S3_BUCKET_NAME = os.environ.get("S3_BUCKET_NAME", "")
S3_TEST_ROOT = os.environ.get("S3_TEST_ROOT", "inopyutils-test")

# Local working directory for test files (next to this script)
LOCAL_TEST_DIR = Path(__file__).resolve().parent / "_test_workspace"
LOCAL_UPLOAD_DIR = LOCAL_TEST_DIR / "upload"
LOCAL_DOWNLOAD_DIR = LOCAL_TEST_DIR / "download"

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _create_test_files():
    """Create a set of test files with known content."""
    LOCAL_UPLOAD_DIR.mkdir(parents=True, exist_ok=True)

    files = {}

    # small text file
    p = LOCAL_UPLOAD_DIR / "hello.txt"
    p.write_text("Hello from InoPyUtils S3 test!", encoding="utf-8")
    files["hello.txt"] = p

    # binary file (~64 KB of deterministic bytes)
    p = LOCAL_UPLOAD_DIR / "binary.bin"
    data = bytes(range(256)) * 256
    p.write_bytes(data)
    files["binary.bin"] = p

    # nested subfolder file
    sub = LOCAL_UPLOAD_DIR / "sub" / "nested.txt"
    sub.parent.mkdir(parents=True, exist_ok=True)
    sub.write_text("nested file content", encoding="utf-8")
    files["sub/nested.txt"] = sub

    return files


def _cleanup_local():
    if LOCAL_TEST_DIR.exists():
        shutil.rmtree(LOCAL_TEST_DIR, ignore_errors=True)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))
from inopyutils.s3_helper import InoS3Helper


async def run_tests():
    # --- validate config ---
    missing = []
    if not S3_ACCESS_KEY_ID:
        missing.append("S3_ACCESS_KEY_ID")
    if not S3_SECRET_ACCESS_KEY:
        missing.append("S3_SECRET_ACCESS_KEY")
    if not S3_BUCKET_NAME:
        missing.append("S3_BUCKET_NAME")
    if missing:
        print(f"SKIP: missing env vars: {', '.join(missing)}")
        print("Create a .env file at the project root. See docstring at top of this file.")
        return

    print("=" * 60)
    print("S3 Helper Integration Test")
    print(f"  endpoint : {S3_ENDPOINT_URL or '(default AWS)'}")
    print(f"  bucket   : {S3_BUCKET_NAME}")
    print(f"  region   : {S3_REGION_NAME}")
    print(f"  s3 root  : {S3_TEST_ROOT}/")
    print("=" * 60)

    _cleanup_local()
    test_files = _create_test_files()

    s3_root = S3_TEST_ROOT.strip("/") + "/"

    async with InoS3Helper(
        aws_access_key_id=S3_ACCESS_KEY_ID,
        aws_secret_access_key=S3_SECRET_ACCESS_KEY,
        endpoint_url=S3_ENDPOINT_URL or None,
        region_name=S3_REGION_NAME,
        bucket_name=S3_BUCKET_NAME,
        retries=3,
    ) as s3:

        passed = 0
        failed = 0

        def check(name: str, result: dict, extra_check=None):
            nonlocal passed, failed
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

        # ------------------------------------------------------------------
        # 1. Upload single files
        # ------------------------------------------------------------------
        print("\n--- Upload single files ---")

        for rel_name, local_path in test_files.items():
            s3_key = s3_root + rel_name.replace("\\", "/")
            res = await s3.upload_file(str(local_path), s3_key)
            check(f"upload_file {rel_name}", res)

        # ------------------------------------------------------------------
        # 2. Verify uploaded files exist
        # ------------------------------------------------------------------
        print("\n--- Verify objects exist ---")

        for rel_name in test_files:
            s3_key = s3_root + rel_name.replace("\\", "/")
            res = await s3.object_exists(s3_key)
            check(f"object_exists {rel_name}", res, lambda r: r.get("exists") is True)

        # ------------------------------------------------------------------
        # 3. List objects under test root
        # ------------------------------------------------------------------
        print("\n--- List objects ---")

        res = await s3.list_objects(prefix=s3_root)
        check("list_objects", res, lambda r: r.get("count", 0) >= len(test_files))
        if res.get("success"):
            print(f"         found {res['count']} objects")

        # ------------------------------------------------------------------
        # 4. Count files in folder
        # ------------------------------------------------------------------
        print("\n--- Count files ---")

        res = await s3.count_files_in_folder(s3_root, recursive=True)
        check("count_files_in_folder", res, lambda r: r.get("count", 0) >= len(test_files))

        # ------------------------------------------------------------------
        # 5. Download single files and verify content
        # ------------------------------------------------------------------
        print("\n--- Download single files ---")

        LOCAL_DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)
        for rel_name, local_upload_path in test_files.items():
            s3_key = s3_root + rel_name.replace("\\", "/")
            dl_path = LOCAL_DOWNLOAD_DIR / "single" / rel_name
            dl_path.parent.mkdir(parents=True, exist_ok=True)

            res = await s3.download_file(s3_key, str(dl_path))
            check(f"download_file {rel_name}", res)

            if res.get("success") and dl_path.exists():
                orig_hash = _sha256(local_upload_path)
                dl_hash = _sha256(dl_path)
                match = orig_hash == dl_hash
                if match:
                    passed += 1
                    print(f"  [PASS] sha256 match {rel_name}")
                else:
                    failed += 1
                    print(f"  [FAIL] sha256 mismatch {rel_name}")
                    print(f"         upload={orig_hash[:16]}... download={dl_hash[:16]}...")

        # ------------------------------------------------------------------
        # 6. Download via get_object (download_file_object)
        # ------------------------------------------------------------------
        print("\n--- Download file object ---")

        dl_obj_path = LOCAL_DOWNLOAD_DIR / "object" / "hello.txt"
        dl_obj_path.parent.mkdir(parents=True, exist_ok=True)
        res = await s3.download_file_object(s3_root + "hello.txt", str(dl_obj_path))
        check("download_file_object hello.txt", res)
        if dl_obj_path.exists():
            orig_hash = _sha256(test_files["hello.txt"])
            dl_hash = _sha256(dl_obj_path)
            match = orig_hash == dl_hash
            if match:
                passed += 1
                print(f"  [PASS] sha256 match (file_object)")
            else:
                failed += 1
                print(f"  [FAIL] sha256 mismatch (file_object)")

        # ------------------------------------------------------------------
        # 7. put_bytes / get_text round-trip
        # ------------------------------------------------------------------
        print("\n--- put_bytes / get_text ---")

        test_text = "put_bytes round-trip test content"
        res = await s3.put_bytes(
            test_text.encode("utf-8"),
            s3_root + "roundtrip.txt",
            content_type="text/plain; charset=utf-8",
        )
        check("put_bytes", res)

        res = await s3.get_text(s3_root + "roundtrip.txt")
        check("get_text", res, lambda r: r.get("text") == test_text)

        # ------------------------------------------------------------------
        # 8. put_text shortcut
        # ------------------------------------------------------------------
        print("\n--- put_text ---")

        res = await s3.put_text("shortcut test", s3_root + "shortcut.txt")
        check("put_text", res)

        res = await s3.get_text(s3_root + "shortcut.txt")
        check("get_text shortcut", res, lambda r: r.get("text") == "shortcut test")

        # ------------------------------------------------------------------
        # 9. Upload folder
        # ------------------------------------------------------------------
        print("\n--- Upload folder ---")

        folder_s3_key = s3_root + "folder_upload/"
        res = await s3.upload_folder(
            s3_folder_key=folder_s3_key,
            local_folder_path=str(LOCAL_UPLOAD_DIR),
        )
        check("upload_folder", res, lambda r: r.get("uploaded_successfully", 0) == len(test_files))

        # ------------------------------------------------------------------
        # 10. Download folder and verify
        # ------------------------------------------------------------------
        print("\n--- Download folder ---")

        dl_folder = LOCAL_DOWNLOAD_DIR / "folder_download"
        res = await s3.download_folder(
            s3_folder_key=folder_s3_key,
            local_folder_path=str(dl_folder),
        )
        check("download_folder", res, lambda r: r.get("downloaded_successfully", 0) == len(test_files))

        # verify each file hash
        if res.get("success"):
            for rel_name, local_upload_path in test_files.items():
                dl_path = dl_folder / rel_name
                if dl_path.exists():
                    orig_hash = _sha256(local_upload_path)
                    dl_hash = _sha256(dl_path)
                    match = orig_hash == dl_hash
                    if match:
                        passed += 1
                        print(f"  [PASS] folder sha256 match {rel_name}")
                    else:
                        failed += 1
                        print(f"  [FAIL] folder sha256 mismatch {rel_name}")
                else:
                    failed += 1
                    print(f"  [FAIL] folder missing {rel_name}")

        # ------------------------------------------------------------------
        # 11. Verify folder sync
        # ------------------------------------------------------------------
        print("\n--- Verify folder sync ---")

        res = await s3.verify_folder_sync(
            s3_folder_key=folder_s3_key,
            local_folder_path=str(dl_folder),
        )
        check("verify_folder_sync", res)

        # ------------------------------------------------------------------
        # 12. Get presigned download link
        # ------------------------------------------------------------------
        print("\n--- Presigned download link ---")

        res = await s3.get_download_link(s3_root + "hello.txt", expires_in=300)
        check("get_download_link", res, lambda r: r.get("url", "").startswith("http"))

        # ------------------------------------------------------------------
        # 13. Verify single file
        # ------------------------------------------------------------------
        print("\n--- Verify file ---")

        local_hello = LOCAL_DOWNLOAD_DIR / "single" / "hello.txt"
        if local_hello.exists():
            res = await s3.verify_file(
                local_file_path=str(local_hello),
                s3_key=s3_root + "hello.txt",
                use_md5=True,
            )
            check("verify_file", res)

        # ------------------------------------------------------------------
        # 14. Delete test objects
        # ------------------------------------------------------------------
        print("\n--- Cleanup: delete test objects ---")

        all_objs = await s3.list_objects(prefix=s3_root, max_keys=500)
        if all_objs.get("success"):
            for obj in all_objs.get("objects", []):
                key = obj["Key"]
                res = await s3.delete_object(key)
                check(f"delete_object {key}", res)

        # ------------------------------------------------------------------
        # 15. Confirm deleted
        # ------------------------------------------------------------------
        print("\n--- Confirm cleanup ---")

        res = await s3.list_objects(prefix=s3_root)
        check("list after cleanup", res, lambda r: r.get("count", -1) == 0)

        # ------------------------------------------------------------------
        # Summary
        # ------------------------------------------------------------------
        print("\n" + "=" * 60)
        total = passed + failed
        print(f"Results: {passed}/{total} passed, {failed} failed")
        print("=" * 60)

    _cleanup_local()


if __name__ == "__main__":
    asyncio.run(run_tests())
