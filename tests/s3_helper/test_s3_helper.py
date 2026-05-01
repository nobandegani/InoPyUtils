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

        def check_fail(name: str, result: dict, extra_check=None):
            """Check that a result is a failure (for error-path tests)."""
            nonlocal passed, failed
            ok = not result.get("success", False)
            if ok and extra_check:
                ok = extra_check(result)
            status = "PASS" if ok else "FAIL"
            if not ok:
                failed += 1
                print(f"  [{status}] {name} (expected failure but got success)")
            else:
                passed += 1
                print(f"  [{status}] {name}")

        # ==================================================================
        # 0. Cleanup S3 from any previous test run
        # ==================================================================
        print("\n--- 0. Cleanup S3 (previous run) ---")

        prev_objs = await s3.list_objects(prefix=s3_root, max_keys=1000)
        if prev_objs.get("success"):
            prev_list = prev_objs.get("objects", [])
            if prev_list:
                for obj in prev_list:
                    await s3.delete_object(obj["Key"])
                print(f"  Cleaned up {len(prev_list)} leftover objects")
            else:
                print(f"  No leftover objects found")

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
        check_fail(
            "upload_file non-existent (should fail)",
            res,
            lambda r: r.get("error_code") == "FileNotFound",
        )

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
        check_fail("download_file non-existent key (should fail)", res)

        # ==================================================================
        # 6. download_file overwrite=False (should skip)
        # ==================================================================
        print("\n--- 6. download_file overwrite=False (skip) ---")

        dl_skip_path = LOCAL_DOWNLOAD_DIR / "single" / "hello.txt"
        res = await s3.download_file(s3_root + "hello.txt", str(dl_skip_path), overwrite=False)
        check(
            "download_file skip (overwrite=False)",
            res,
            lambda r: r.get("skipped") is True
                       and r.get("verify_method") in ("sha256", "md5", "size"),
        )
        if res.get("success"):
            print(f"         verify_method: {res.get('verify_method')}")

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
        # Sanity-check that at least one skip result actually carried verify_method.
        # (We can't easily inspect individual file results from upload_folder's
        # aggregate dict, so we re-derive via verify_file on one of the files.)
        verify_check = await s3.verify_file(
            local_file_path=str(test_files["hello.txt"]),
            s3_key=folder_s3_key + "hello.txt",
            use_md5=True,
            use_sha256=True,
        )
        check(
            "upload_folder skip carries verify_method (probe)",
            verify_check,
            lambda r: r.get("verify_method") in ("sha256", "md5", "size"),
        )
        print(f"         verify_method (probe): {verify_check.get('verify_method')}")

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
        check_fail("upload_folder non-existent (should fail)", res, lambda r: r.get("error_code") == "FolderNotFound")

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
        # Probe verify_method via verify_file on one of the just-skipped files
        verify_probe = await s3.verify_file(
            local_file_path=str(dl_folder / "hello.txt"),
            s3_key=folder_s3_key + "hello.txt",
            use_md5=True,
            use_sha256=True,
        )
        check(
            "download_folder skip carries verify_method (probe)",
            verify_probe,
            lambda r: r.get("verify_method") in ("sha256", "md5", "size"),
        )
        print(f"         verify_method (probe): {verify_probe.get('verify_method')}")

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
            res = await s3.upload_file(str(local_path), s3_key)
            check(f"sync setup upload {rel_name}", res)

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

        # 12d. Stale local file with delete=False — sync should keep it
        keeper_file = sync_local_dir / "keeper_extra.txt"
        keeper_file.write_text("delete=False should keep me", encoding="utf-8")

        res = await s3.sync_folder(
            s3_key=sync_s3_key,
            local_folder_path=str(sync_local_dir),
            sync_local=True,
            concurrency=3,
            delete=False,
        )
        check(
            "sync_folder S3->local (delete=False keeps stale)",
            res,
            lambda r: r.get("removed_local", -1) == 0
                       and keeper_file.exists(),
        )

        # 12e. Confirm verified count matches remote total when delete=False
        check(
            "sync_folder S3->local (verified count)",
            res,
            lambda r: r.get("verified", 0) == len(test_files),
        )

        # Cleanup the keeper file so subsequent assertions aren't off
        keeper_file.unlink(missing_ok=True)

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
        check("stale remote object deleted", extra_exists, lambda r: r.get("exists") is False)

        # 13d. Stale remote with delete=False — sync should keep it
        keeper_remote_key = sync_remote_key + "remote_keeper.txt"
        await s3.put_text("delete=False should keep me on S3", keeper_remote_key)

        res = await s3.sync_folder(
            s3_key=sync_remote_key,
            local_folder_path=str(LOCAL_UPLOAD_DIR),
            sync_local=False,
            concurrency=3,
            delete=False,
        )
        check(
            "sync_folder local->S3 (delete=False keeps remote extras)",
            res,
            lambda r: r.get("removed_remote", -1) == 0,
        )
        keeper_still = await s3.object_exists(keeper_remote_key)
        check(
            "remote keeper still exists after delete=False",
            keeper_still,
            lambda r: r.get("exists") is True,
        )

        # 13d2. Confirm verified count matches local total
        check(
            "sync_folder local->S3 (verified count)",
            res,
            lambda r: r.get("verified", 0) == len(test_files),
        )

        # Now clean up the keeper so it doesn't pollute later assertions
        await s3.delete_object(keeper_remote_key)

        # 13e. Batched-delete stress: create 7 orphan remote objects, then
        # sync with delete=True. Exercises the batched delete_objects path.
        orphan_keys = [sync_remote_key + f"orphan_{i:02d}.txt" for i in range(7)]
        for ok in orphan_keys:
            await s3.put_text("orphan to be batch-deleted", ok)

        res = await s3.sync_folder(
            s3_key=sync_remote_key,
            local_folder_path=str(LOCAL_UPLOAD_DIR),
            sync_local=False,
            concurrency=3,
            delete=True,
        )
        check(
            "sync_folder local->S3 (batched delete of 7 orphans)",
            res,
            lambda r: r.get("removed_remote", 0) == len(orphan_keys),
        )
        # Confirm none of the orphans survive
        all_gone = True
        for ok in orphan_keys:
            chk = await s3.object_exists(ok)
            if chk.get("exists") is not False:
                all_gone = False
                break
        if all_gone:
            passed += 1
            print(f"  [PASS] all 7 orphans confirmed deleted")
        else:
            failed += 1
            print(f"  [FAIL] some orphans still exist after batched delete")

        # 13g. Empty prefix safety guard — refuses without allow_full_bucket
        empty_prefix_res = await s3.sync_folder(
            s3_key="",
            local_folder_path=str(LOCAL_UPLOAD_DIR),
            sync_local=False,
        )
        check_fail(
            "sync_folder refuses empty prefix without allow_full_bucket",
            empty_prefix_res,
            lambda r: r.get("error_code") == "EmptyPrefixRefused",
        )
        print(f"         msg: {empty_prefix_res.get('msg')}")

        # 13f. Verify uploaded content by downloading and comparing hashes
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
            check_fail("verify_file non-existent remote (should fail)", res)

        # ------------------------------------------------------------------
        # 16e. Verify file with non-existent local file
        # ------------------------------------------------------------------
        print("\n--- 16e. Verify file non-existent local (error) ---")

        res = await s3.verify_file(
            local_file_path="/tmp/does_not_exist_xyz.txt",
            s3_key=s3_root + "hello.txt",
        )
        check_fail("verify_file non-existent local (should fail)", res, lambda r: r.get("error_code") == "FileNotFound")

        # ==================================================================
        # 17. Concurrent operations — exercise the shared client under load
        # ==================================================================
        print("\n--- 17. Concurrent uploads (shared client) ---")

        concurrent_root = s3_root + "concurrent/"

        # Build 12 in-memory payloads and upload them concurrently
        concurrent_keys = [concurrent_root + f"file_{i:02d}.txt" for i in range(12)]
        concurrent_tasks = [
            s3.put_text(f"concurrent payload #{i}", k)
            for i, k in enumerate(concurrent_keys)
        ]
        results = await asyncio.gather(*concurrent_tasks, return_exceptions=True)

        ok_count = sum(
            1 for r in results
            if isinstance(r, dict) and r.get("success", False)
        )
        if ok_count == len(concurrent_keys):
            passed += 1
            print(f"  [PASS] concurrent put_text x{len(concurrent_keys)}")
        else:
            failed += 1
            print(f"  [FAIL] concurrent put_text: {ok_count}/{len(concurrent_keys)} ok")
            for i, r in enumerate(results):
                if isinstance(r, Exception):
                    print(f"         file_{i:02d}: exception {type(r).__name__}: {r}")
                elif isinstance(r, dict) and not r.get("success"):
                    print(f"         file_{i:02d}: {r.get('msg')}")

        # 17b. Concurrent reads — get_text on all of them in parallel
        read_tasks = [s3.get_text(k) for k in concurrent_keys]
        read_results = await asyncio.gather(*read_tasks, return_exceptions=True)
        all_match = all(
            isinstance(r, dict)
            and r.get("success", False)
            and r.get("text") == f"concurrent payload #{i}"
            for i, r in enumerate(read_results)
        )
        if all_match:
            passed += 1
            print(f"  [PASS] concurrent get_text x{len(concurrent_keys)} (content matches)")
        else:
            failed += 1
            print(f"  [FAIL] concurrent get_text: not all content matched")

        # 17c. Concurrent object_exists
        exist_tasks = [s3.object_exists(k) for k in concurrent_keys]
        exist_results = await asyncio.gather(*exist_tasks, return_exceptions=True)
        all_exist = all(
            isinstance(r, dict)
            and r.get("success", False)
            and r.get("exists") is True
            for r in exist_results
        )
        if all_exist:
            passed += 1
            print(f"  [PASS] concurrent object_exists x{len(concurrent_keys)}")
        else:
            failed += 1
            print(f"  [FAIL] concurrent object_exists: not all reported exists=True")

        # ==================================================================
        # 18. Edge cases: empty file, unicode filename, deeply nested key
        # ==================================================================
        print("\n--- 18. Edge cases ---")

        # 18a. Empty file (0 bytes)
        empty_local = LOCAL_UPLOAD_DIR / "empty.bin"
        empty_local.write_bytes(b"")
        empty_key = s3_root + "edge/empty.bin"

        res = await s3.upload_file(str(empty_local), empty_key)
        check("upload_file empty (0 bytes)", res)

        empty_dl = LOCAL_DOWNLOAD_DIR / "edge" / "empty.bin"
        empty_dl.parent.mkdir(parents=True, exist_ok=True)
        res = await s3.download_file(empty_key, str(empty_dl), overwrite=True)
        check("download_file empty (0 bytes)", res)

        if empty_dl.exists():
            check_bool_size = empty_dl.stat().st_size == 0
            if check_bool_size:
                passed += 1
                print(f"  [PASS] empty file size=0 round-trip")
            else:
                failed += 1
                print(f"  [FAIL] empty file size mismatch: got {empty_dl.stat().st_size}")

        # 18b. verify_file on empty file (size match + sha256)
        res = await s3.verify_file(
            local_file_path=str(empty_local),
            s3_key=empty_key,
            use_md5=True,
        )
        check("verify_file empty", res)

        # 18c. Unicode filename
        unicode_local = LOCAL_UPLOAD_DIR / "测试文件.txt"
        unicode_local.write_text("unicode content: café 测试 🎉", encoding="utf-8")
        unicode_key = s3_root + "edge/测试文件.txt"

        res = await s3.upload_file(str(unicode_local), unicode_key)
        check("upload_file unicode filename", res)

        res = await s3.object_exists(unicode_key)
        check("object_exists unicode", res, lambda r: r.get("exists") is True)

        unicode_dl = LOCAL_DOWNLOAD_DIR / "edge" / "测试文件.txt"
        res = await s3.download_file(unicode_key, str(unicode_dl), overwrite=True)
        check("download_file unicode filename", res)

        if unicode_dl.exists():
            content = unicode_dl.read_text(encoding="utf-8")
            if content == "unicode content: café 测试 🎉":
                passed += 1
                print(f"  [PASS] unicode content round-trip")
            else:
                failed += 1
                print(f"  [FAIL] unicode content mismatch: {content!r}")

        # 18d. Presigned link for unicode filename (Content-Disposition encoding)
        res = await s3.get_download_link(
            unicode_key,
            expires_in=300,
            as_attachment=True,
        )
        check(
            "get_download_link unicode (as_attachment)",
            res,
            lambda r: r.get("url", "").startswith("http"),
        )

        # 18e. Deeply nested key
        deep_local = test_files["hello.txt"]
        deep_key = s3_root + "edge/a/b/c/d/e/f/g/deep.txt"

        res = await s3.upload_file(str(deep_local), deep_key)
        check("upload_file deeply nested key", res)

        deep_dl = LOCAL_DOWNLOAD_DIR / "edge" / "a" / "b" / "c" / "d" / "e" / "f" / "g" / "deep.txt"
        res = await s3.download_file(deep_key, str(deep_dl), overwrite=True)
        check(
            "download_file deeply nested (parents created)",
            res,
            lambda r: deep_dl.exists(),
        )

        # ==================================================================
        # 19. extra_args_provider callback in upload_folder
        # ==================================================================
        print("\n--- 19. upload_folder extra_args_provider ---")

        provider_root = s3_root + "provider_test/"
        # Wipe any previous run
        prev = await s3.list_objects(prefix=provider_root, max_keys=1000)
        if prev.get("success"):
            for o in prev.get("objects", []):
                await s3.delete_object(o["Key"])

        seen_relative_paths: list = []

        def _extra_args_provider(rel_path: str) -> dict:
            seen_relative_paths.append(rel_path)
            # S3 user metadata only allows ASCII; URL-encode anything else.
            from urllib.parse import quote as _q
            safe_path = _q(rel_path.replace("\\", "/"), safe="/")
            return {"Metadata": {"src-rel-path": safe_path}}

        res = await s3.upload_folder(
            s3_folder_key=provider_root,
            local_folder_path=str(LOCAL_UPLOAD_DIR),
            extra_args_provider=_extra_args_provider,
            overwrite=True,
        )
        check(
            "upload_folder with extra_args_provider",
            res,
            lambda r: r.get("uploaded_successfully", 0) >= len(test_files)
                       and len(seen_relative_paths) >= len(test_files),
        )

        # ==================================================================
        # 20. upload_file with overwrite=False (skip-if-matches with cascade)
        # ==================================================================
        print("\n--- 20. upload_file overwrite=False (skip if matches) ---")

        skip_root = s3_root + "upload_skip/"
        # Wipe any leftovers
        prev = await s3.list_objects(prefix=skip_root, max_keys=1000)
        if prev.get("success"):
            for o in prev.get("objects", []):
                await s3.delete_object(o["Key"])

        skip_local = LOCAL_UPLOAD_DIR / "skip_test.txt"
        skip_local.write_text("upload_file skip-if-matches test", encoding="utf-8")
        skip_key = skip_root + "skip_test.txt"

        # 20a. First upload — should actually upload (not skipped)
        res = await s3.upload_file(str(skip_local), skip_key, overwrite=False)
        check(
            "upload_file initial (overwrite=False, no remote)",
            res,
            lambda r: r.get("skipped") is False,
        )

        # 20b. Re-upload identical file with overwrite=False — should skip
        res = await s3.upload_file(str(skip_local), skip_key, overwrite=False)
        check(
            "upload_file skip when remote matches (overwrite=False)",
            res,
            lambda r: r.get("skipped") is True
                       and r.get("verify_method") in ("sha256", "md5", "size"),
        )
        if res.get("success"):
            print(f"         verify_method: {res.get('verify_method')}")

        # 20c. Modify content, re-upload with overwrite=False — should NOT skip
        skip_local.write_text("upload_file skip-if-matches test (CHANGED)", encoding="utf-8")
        res = await s3.upload_file(str(skip_local), skip_key, overwrite=False)
        check(
            "upload_file does NOT skip when content differs",
            res,
            lambda r: r.get("skipped") is False,
        )

        # 20d. Re-upload with overwrite=True (default) — never skips
        res = await s3.upload_file(str(skip_local), skip_key)
        check(
            "upload_file overwrite=True default (always uploads)",
            res,
            lambda r: r.get("skipped") is False,
        )

        # 20e. Verify cascade picks "md5" for small single-part Backblaze upload
        verify_res = await s3.verify_file(
            local_file_path=str(skip_local),
            s3_key=skip_key,
            use_md5=True,
            use_sha256=True,
        )
        check(
            "verify_file returns verify_method field",
            verify_res,
            lambda r: r.get("verify_method") in ("sha256", "md5", "size"),
        )
        print(f"         small file cascade picked: {verify_res.get('verify_method')}")

        # 20f. verify_method should be None when remote doesn't exist
        verify_missing = await s3.verify_file(
            local_file_path=str(skip_local),
            s3_key=skip_root + "definitely_not_there.txt",
        )
        check_fail(
            "verify_file fails when remote missing",
            verify_missing,
        )
        if verify_missing.get("verify_method") is None:
            passed += 1
            print("  [PASS] verify_method is None when remote missing")
        else:
            failed += 1
            print(f"  [FAIL] verify_method should be None when remote missing, got {verify_missing.get('verify_method')!r}")

        # ==================================================================
        # 21. Delete test objects
        # ==================================================================
        print("\n--- 21. Cleanup: delete test objects ---")

        all_objs = await s3.list_objects(prefix=s3_root, max_keys=500)
        if all_objs.get("success"):
            for obj in all_objs.get("objects", []):
                key = obj["Key"]
                res = await s3.delete_object(key)
                check(f"delete_object {key}", res)

        # ==================================================================
        # 22. Confirm deleted
        # ==================================================================
        print("\n--- 22. Confirm cleanup ---")

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
