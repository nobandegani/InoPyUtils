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

        # ==================================================================
        # 1. Upload single files
        # ==================================================================
        print("\n--- 1. Upload single files ---")

        for rel_name, local_path in test_files.items():
            s3_key = s3_root + rel_name.replace("\\", "/")
            res = await s3.upload_file(str(local_path), s3_key)
            check(f"upload_file {rel_name}", res)

        # ------------------------------------------------------------------
        # 1b. Upload with extra_args (custom ContentType + Metadata)
        # ------------------------------------------------------------------
        print("\n--- 1b. Upload with extra_args ---")

        res = await s3.upload_file(
            str(test_files["hello.txt"]),
            s3_root + "hello_custom.txt",
            extra_args={
                "ContentType": "text/plain; charset=utf-8",
                "Metadata": {"test-key": "test-value"},
            },
        )
        check("upload_file with extra_args", res)

        # ------------------------------------------------------------------
        # 1c. Upload non-existent local file (error path)
        # ------------------------------------------------------------------
        print("\n--- 1c. Upload non-existent file (error) ---")

        res = await s3.upload_file("/tmp/does_not_exist_xyz.txt", s3_root + "ghost.txt")
        check(
            "upload_file non-existent (should fail)",
            res,
            lambda r: r.get("success") is False and r.get("error_code") == "FileNotFound",
        )
        # invert: we expect failure
        if not res.get("success"):
            passed += 1  # undo the fail from check
            failed -= 1
        else:
            failed += 1
            passed -= 1

        # ==================================================================
        # 2. Verify uploaded files exist
        # ==================================================================
        print("\n--- 2. Verify objects exist ---")

        for rel_name in test_files:
            s3_key = s3_root + rel_name.replace("\\", "/")
            res = await s3.object_exists(s3_key)
            check(f"object_exists {rel_name}", res, lambda r: r.get("exists") is True)

        # ------------------------------------------------------------------
        # 2b. object_exists for non-existent key
        # ------------------------------------------------------------------
        print("\n--- 2b. object_exists non-existent ---")

        res = await s3.object_exists(s3_root + "this_does_not_exist_xyz.bin")
        check(
            "object_exists non-existent",
            res,
            lambda r: r.get("exists") is False,
        )

        # ==================================================================
        # 3. List objects under test root
        # ==================================================================
        print("\n--- 3. List objects ---")

        res = await s3.list_objects(prefix=s3_root)
        check("list_objects (recursive)", res, lambda r: r.get("count", 0) >= len(test_files))
        if res.get("success"):
            print(f"         found {res['count']} objects")

        # ------------------------------------------------------------------
        # 3b. List objects non-recursive (with delimiter)
        # ------------------------------------------------------------------
        print("\n--- 3b. List objects non-recursive ---")

        res = await s3.list_objects(prefix=s3_root, recursive=False)
        check("list_objects (non-recursive)", res)
        if res.get("success"):
            print(f"         found {res.get('count', 0)} objects (flat), prefixes={res.get('common_prefixes', [])}")

        # ==================================================================
        # 4. Count files in folder
        # ==================================================================
        print("\n--- 4. Count files (recursive) ---")

        res = await s3.count_files_in_folder(s3_root, recursive=True)
        check("count_files_in_folder (recursive)", res, lambda r: r.get("count", 0) >= len(test_files))

        # ------------------------------------------------------------------
        # 4b. Count files non-recursive
        # ------------------------------------------------------------------
        print("\n--- 4b. Count files (non-recursive) ---")

        res = await s3.count_files_in_folder(s3_root, recursive=False)
        check("count_files_in_folder (non-recursive)", res)
        if res.get("success"):
            print(f"         count={res.get('count', 0)} (flat, excludes sub/ contents)")

        # ==================================================================
        # 5. Download single files and verify content
        # ==================================================================
        print("\n--- 5. Download single files ---")

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
        # 5b. Download non-existent key (error path)
        # ------------------------------------------------------------------
        print("\n--- 5b. Download non-existent key (error) ---")

        res = await s3.download_file(s3_root + "no_such_file_xyz.bin", str(LOCAL_DOWNLOAD_DIR / "ghost.bin"))
        if not res.get("success"):
            passed += 1
            print(f"  [PASS] download_file non-existent key (expected failure)")
        else:
            failed += 1
            print(f"  [FAIL] download_file non-existent key should have failed")

        # ==================================================================
        # 6. download_file overwrite=False (should skip)
        # ==================================================================
        print("\n--- 6. download_file overwrite=False (skip) ---")

        dl_skip_path = LOCAL_DOWNLOAD_DIR / "single" / "hello.txt"
        res = await s3.download_file(s3_root + "hello.txt", str(dl_skip_path), overwrite=False)
        check(
            "download_file skip (overwrite=False)",
            res,
            lambda r: r.get("skipped") is True,
        )

        # ------------------------------------------------------------------
        # 6b. download_file overwrite=True (should re-download)
        # ------------------------------------------------------------------
        print("\n--- 6b. download_file overwrite=True ---")

        res = await s3.download_file(s3_root + "hello.txt", str(dl_skip_path), overwrite=True)
        check(
            "download_file force (overwrite=True)",
            res,
            lambda r: r.get("skipped") is False,
        )

        # ==================================================================
        # 7. put_bytes / get_text round-trip
        # ==================================================================
        print("\n--- 7. put_bytes / get_text ---")

        test_text = "put_bytes round-trip test content"
        res = await s3.put_bytes(
            test_text.encode("utf-8"),
            s3_root + "roundtrip.txt",
            content_type="text/plain; charset=utf-8",
        )
        check("put_bytes", res)

        res = await s3.get_text(s3_root + "roundtrip.txt")
        check("get_text", res, lambda r: r.get("text") == test_text)

        # ==================================================================
        # 8. put_text shortcut
        # ==================================================================
        print("\n--- 8. put_text ---")

        res = await s3.put_text("shortcut test", s3_root + "shortcut.txt")
        check("put_text", res)

        res = await s3.get_text(s3_root + "shortcut.txt")
        check("get_text shortcut", res, lambda r: r.get("text") == "shortcut test")

        # ==================================================================
        # 9. Upload folder
        # ==================================================================
        print("\n--- 9. Upload folder ---")

        folder_s3_key = s3_root + "folder_upload/"
        res = await s3.upload_folder(
            s3_folder_key=folder_s3_key,
            local_folder_path=str(LOCAL_UPLOAD_DIR),
        )
        check("upload_folder", res, lambda r: r.get("uploaded_successfully", 0) == len(test_files))

        # ------------------------------------------------------------------
        # 9b. Upload folder overwrite=False (should skip all)
        # ------------------------------------------------------------------
        print("\n--- 9b. Upload folder overwrite=False (skip) ---")

        res = await s3.upload_folder(
            s3_folder_key=folder_s3_key,
            local_folder_path=str(LOCAL_UPLOAD_DIR),
            overwrite=False,
        )
        check(
            "upload_folder skip (overwrite=False)",
            res,
            lambda r: r.get("skipped_files", 0) == len(test_files)
                       and r.get("uploaded_successfully", -1) == 0,
        )

        # ------------------------------------------------------------------
        # 9c. Upload folder overwrite=True (should re-upload all)
        # ------------------------------------------------------------------
        print("\n--- 9c. Upload folder overwrite=True ---")

        res = await s3.upload_folder(
            s3_folder_key=folder_s3_key,
            local_folder_path=str(LOCAL_UPLOAD_DIR),
            overwrite=True,
        )
        check(
            "upload_folder force (overwrite=True)",
            res,
            lambda r: r.get("uploaded_successfully", 0) == len(test_files)
                       and r.get("skipped_files", -1) == 0,
        )

        # ------------------------------------------------------------------
        # 9d. Upload folder with verify=True
        # ------------------------------------------------------------------
        print("\n--- 9d. Upload folder with verify ---")

        res = await s3.upload_folder(
            s3_folder_key=folder_s3_key,
            local_folder_path=str(LOCAL_UPLOAD_DIR),
            overwrite=True,
            verify=True,
        )
        check(
            "upload_folder with verify",
            res,
            lambda r: r.get("uploaded_successfully", 0) == len(test_files)
                       and r.get("verification", {}).get("success") is True,
        )

        # ------------------------------------------------------------------
        # 9e. Upload folder non-existent path (error)
        # ------------------------------------------------------------------
        print("\n--- 9e. Upload folder non-existent (error) ---")

        res = await s3.upload_folder(
            s3_folder_key=folder_s3_key,
            local_folder_path="/tmp/does_not_exist_folder_xyz",
        )
        if not res.get("success"):
            passed += 1
            print(f"  [PASS] upload_folder non-existent path (expected failure)")
        else:
            failed += 1
            print(f"  [FAIL] upload_folder non-existent path should have failed")

        # ==================================================================
        # 10. Download folder and verify
        # ==================================================================
        print("\n--- 10. Download folder ---")

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
        # 10b. Download folder overwrite=False (should skip all)
        # ------------------------------------------------------------------
        print("\n--- 10b. Download folder overwrite=False (skip) ---")

        res = await s3.download_folder(
            s3_folder_key=folder_s3_key,
            local_folder_path=str(dl_folder),
            overwrite=False,
        )
        check(
            "download_folder skip (overwrite=False)",
            res,
            lambda r: r.get("skipped_files", 0) == len(test_files)
                       and r.get("downloaded_successfully", -1) == 0,
        )

        # ------------------------------------------------------------------
        # 10c. Download folder overwrite=True (should re-download all)
        # ------------------------------------------------------------------
        print("\n--- 10c. Download folder overwrite=True ---")

        res = await s3.download_folder(
            s3_folder_key=folder_s3_key,
            local_folder_path=str(dl_folder),
            overwrite=True,
        )
        check(
            "download_folder force (overwrite=True)",
            res,
            lambda r: r.get("downloaded_successfully", 0) == len(test_files)
                       and r.get("skipped_files", -1) == 0,
        )

        # ------------------------------------------------------------------
        # 10d. Download folder with verify=True
        # ------------------------------------------------------------------
        print("\n--- 10d. Download folder with verify ---")

        dl_verify_folder = LOCAL_DOWNLOAD_DIR / "folder_download_verify"
        res = await s3.download_folder(
            s3_folder_key=folder_s3_key,
            local_folder_path=str(dl_verify_folder),
            verify=True,
        )
        check(
            "download_folder with verify",
            res,
            lambda r: r.get("downloaded_successfully", 0) == len(test_files)
                       and r.get("verification", {}).get("success") is True,
        )

        # ==================================================================
        # 11. Verify folder sync
        # ==================================================================
        print("\n--- 11. Verify folder sync ---")

        res = await s3.verify_folder_sync(
            s3_folder_key=folder_s3_key,
            local_folder_path=str(dl_folder),
        )
        check("verify_folder_sync", res)

        # ==================================================================
        # 12. sync_folder (sync_local=True: S3 -> local)
        # ==================================================================
        print("\n--- 12. sync_folder (S3 -> local) ---")

        sync_local_dir = LOCAL_DOWNLOAD_DIR / "sync_local"
        sync_s3_key = s3_root + "sync_test/"

        # Upload test files to S3 under sync_test/ prefix first
        for rel_name, local_path in test_files.items():
            s3_key = sync_s3_key + rel_name.replace("\\", "/")
            await s3.upload_file(str(local_path), s3_key)

        # 12a. Initial sync — should download all files
        res = await s3.sync_folder(
            s3_key=sync_s3_key,
            local_folder_path=str(sync_local_dir),
            sync_local=True,
            concurrency=3,
        )
        check(
            "sync_folder S3->local (initial)",
            res,
            lambda r: r.get("downloaded", 0) == len(test_files)
                       and r.get("failed", -1) == 0
                       and r.get("total_remote_files", 0) == len(test_files),
        )

        # Verify downloaded content matches
        if res.get("success"):
            for rel_name, local_upload_path in test_files.items():
                dl_path = sync_local_dir / rel_name
                if dl_path.exists():
                    orig_hash = _sha256(local_upload_path)
                    dl_hash = _sha256(dl_path)
                    if orig_hash == dl_hash:
                        passed += 1
                        print(f"  [PASS] sync_local sha256 match {rel_name}")
                    else:
                        failed += 1
                        print(f"  [FAIL] sync_local sha256 mismatch {rel_name}")
                else:
                    failed += 1
                    print(f"  [FAIL] sync_local missing {rel_name}")

        # 12b. Re-sync — everything should be skipped (unchanged)
        res = await s3.sync_folder(
            s3_key=sync_s3_key,
            local_folder_path=str(sync_local_dir),
            sync_local=True,
            concurrency=3,
        )
        check(
            "sync_folder S3->local (re-sync skip)",
            res,
            lambda r: r.get("skipped_unchanged", 0) == len(test_files)
                       and r.get("downloaded", -1) == 0,
        )

        # 12c. Add a stale local file — sync should remove it
        stale_file = sync_local_dir / "stale_extra.txt"
        stale_file.write_text("I should be removed", encoding="utf-8")

        res = await s3.sync_folder(
            s3_key=sync_s3_key,
            local_folder_path=str(sync_local_dir),
            sync_local=True,
            concurrency=3,
        )
        check(
            "sync_folder S3->local (remove stale)",
            res,
            lambda r: r.get("removed_local", 0) >= 1
                       and not stale_file.exists(),
        )

        # ==================================================================
        # 13. sync_folder (sync_local=False: local -> S3)
        # ==================================================================
        print("\n--- 13. sync_folder (local -> S3) ---")

        sync_remote_key = s3_root + "sync_upload_test/"

        # 13a. Initial sync — should upload all files
        res = await s3.sync_folder(
            s3_key=sync_remote_key,
            local_folder_path=str(LOCAL_UPLOAD_DIR),
            sync_local=False,
            concurrency=3,
        )
        check(
            "sync_folder local->S3 (initial)",
            res,
            lambda r: r.get("uploaded", 0) == len(test_files)
                       and r.get("failed", -1) == 0
                       and r.get("total_local_files", 0) == len(test_files),
        )

        # 13b. Re-sync — everything should be skipped (unchanged)
        res = await s3.sync_folder(
            s3_key=sync_remote_key,
            local_folder_path=str(LOCAL_UPLOAD_DIR),
            sync_local=False,
            concurrency=3,
        )
        check(
            "sync_folder local->S3 (re-sync skip)",
            res,
            lambda r: r.get("skipped_unchanged", 0) == len(test_files)
                       and r.get("uploaded", -1) == 0,
        )

        # 13c. Upload an extra file to S3 that is not local — sync should remove it
        extra_s3_key = sync_remote_key + "extra_remote.txt"
        await s3.put_text("I should be removed", extra_s3_key)

        res = await s3.sync_folder(
            s3_key=sync_remote_key,
            local_folder_path=str(LOCAL_UPLOAD_DIR),
            sync_local=False,
            concurrency=3,
        )
        check(
            "sync_folder local->S3 (remove stale remote)",
            res,
            lambda r: r.get("removed_remote", 0) >= 1,
        )

        # Confirm the extra remote file was actually deleted
        extra_exists = await s3.object_exists(extra_s3_key)
        if extra_exists.get("exists") is False:
            passed += 1
            print(f"  [PASS] stale remote object deleted")
        else:
            failed += 1
            print(f"  [FAIL] stale remote object still exists")

        # 13d. Verify uploaded content by downloading and comparing hashes
        sync_verify_dir = LOCAL_DOWNLOAD_DIR / "sync_upload_verify"
        res = await s3.download_folder(
            s3_folder_key=sync_remote_key,
            local_folder_path=str(sync_verify_dir),
        )
        if res.get("success"):
            for rel_name, local_upload_path in test_files.items():
                dl_path = sync_verify_dir / rel_name
                if dl_path.exists():
                    orig_hash = _sha256(local_upload_path)
                    dl_hash = _sha256(dl_path)
                    if orig_hash == dl_hash:
                        passed += 1
                        print(f"  [PASS] sync_remote sha256 match {rel_name}")
                    else:
                        failed += 1
                        print(f"  [FAIL] sync_remote sha256 mismatch {rel_name}")
                else:
                    failed += 1
                    print(f"  [FAIL] sync_remote missing {rel_name}")

        # ==================================================================
        # 14. Get presigned download link
        # ==================================================================
        print("\n--- 14. Presigned download link ---")

        res = await s3.get_download_link(s3_root + "hello.txt", expires_in=300)
        check("get_download_link", res, lambda r: r.get("url", "").startswith("http"))
        if res.get("success"):
            print(f"         URL: {res['url']}")

        # ------------------------------------------------------------------
        # 14b. Presigned download link (as attachment)
        # ------------------------------------------------------------------
        print("\n--- 14b. Presigned download link (as_attachment) ---")

        res = await s3.get_download_link(
            s3_root + "hello.txt",
            expires_in=300,
            as_attachment=True,
            filename="custom_hello.txt",
        )
        check(
            "get_download_link as_attachment",
            res,
            lambda r: r.get("url", "").startswith("http") and r.get("filename") == "custom_hello.txt",
        )
        if res.get("success"):
            print(f"         URL: {res['url']}")

        # ==================================================================
        # 15. Get folder download links
        # ==================================================================
        print("\n--- 15. Folder download links ---")

        res = await s3.get_folder_download_links(
            s3_folder_key=folder_s3_key,
            expires_in=300,
        )
        check(
            "get_folder_download_links",
            res,
            lambda r: r.get("count", 0) == len(test_files)
                       and all(link.get("url", "").startswith("http") for link in r.get("links", [])),
        )
        if res.get("success"):
            print(f"         got {res['count']} links")
            for link in res.get("links", []):
                print(f"         - {link['filename']}: {link['url']}")

        # ------------------------------------------------------------------
        # 15b. Folder download links (as_attachment)
        # ------------------------------------------------------------------
        print("\n--- 15b. Folder download links (as_attachment) ---")

        res = await s3.get_folder_download_links(
            s3_folder_key=folder_s3_key,
            expires_in=300,
            as_attachment=True,
        )
        check(
            "get_folder_download_links as_attachment",
            res,
            lambda r: r.get("count", 0) == len(test_files),
        )

        # ------------------------------------------------------------------
        # 15c. Folder download links (empty prefix)
        # ------------------------------------------------------------------
        print("\n--- 15c. Folder download links (empty folder) ---")

        res = await s3.get_folder_download_links(
            s3_folder_key=s3_root + "nonexistent_folder/",
            expires_in=300,
        )
        check(
            "get_folder_download_links empty",
            res,
            lambda r: r.get("count", 0) == 0 and r.get("links") == [],
        )

        # ==================================================================
        # 16. Verify single file
        # ==================================================================
        print("\n--- 16. Verify file ---")

        local_hello = LOCAL_DOWNLOAD_DIR / "single" / "hello.txt"
        if local_hello.exists():
            # 16a. verify with MD5
            res = await s3.verify_file(
                local_file_path=str(local_hello),
                s3_key=s3_root + "hello.txt",
                use_md5=True,
            )
            check("verify_file (use_md5)", res)

            # 16b. verify with SHA256
            res = await s3.verify_file(
                local_file_path=str(local_hello),
                s3_key=s3_root + "hello.txt",
                use_sha256=True,
            )
            check("verify_file (use_sha256)", res)

            # 16c. verify with both MD5 + SHA256
            res = await s3.verify_file(
                local_file_path=str(local_hello),
                s3_key=s3_root + "hello.txt",
                use_md5=True,
                use_sha256=True,
            )
            check("verify_file (md5+sha256)", res)

        # ------------------------------------------------------------------
        # 16d. Verify file against non-existent remote key
        # ------------------------------------------------------------------
        print("\n--- 16d. Verify file non-existent remote (error) ---")

        if local_hello.exists():
            res = await s3.verify_file(
                local_file_path=str(local_hello),
                s3_key=s3_root + "no_such_file_xyz.bin",
            )
            if not res.get("success") and res.get("error_code") == "NoSuchKey":
                passed += 1
                print(f"  [PASS] verify_file non-existent remote (expected failure)")
            elif not res.get("success"):
                passed += 1
                print(f"  [PASS] verify_file non-existent remote (failed as expected: {res.get('error_code')})")
            else:
                failed += 1
                print(f"  [FAIL] verify_file non-existent remote should have failed")

        # ------------------------------------------------------------------
        # 16e. Verify file with non-existent local file
        # ------------------------------------------------------------------
        print("\n--- 16e. Verify file non-existent local (error) ---")

        res = await s3.verify_file(
            local_file_path="/tmp/does_not_exist_xyz.txt",
            s3_key=s3_root + "hello.txt",
        )
        if not res.get("success") and res.get("error_code") == "FileNotFound":
            passed += 1
            print(f"  [PASS] verify_file non-existent local (expected failure)")
        else:
            failed += 1
            print(f"  [FAIL] verify_file non-existent local should have failed with FileNotFound")

        # ==================================================================
        # 17. Delete test objects
        # ==================================================================
        print("\n--- 17. Cleanup: delete test objects ---")

        all_objs = await s3.list_objects(prefix=s3_root, max_keys=500)
        if all_objs.get("success"):
            for obj in all_objs.get("objects", []):
                key = obj["Key"]
                res = await s3.delete_object(key)
                check(f"delete_object {key}", res)

        # ==================================================================
        # 18. Confirm deleted
        # ==================================================================
        print("\n--- 18. Confirm cleanup ---")

        res = await s3.list_objects(prefix=s3_root)
        check("list after cleanup", res, lambda r: r.get("count", -1) == 0)

        # ==================================================================
        # Summary
        # ==================================================================
        print("\n" + "=" * 60)
        total = passed + failed
        print(f"Results: {passed}/{total} passed, {failed} failed")
        print("=" * 60)

    _cleanup_local()


if __name__ == "__main__":
    asyncio.run(run_tests())
