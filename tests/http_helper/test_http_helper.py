"""
HTTP Helper integration test.

Uses httpbin.org (or a custom base URL from .env) to test all HTTP methods,
retry behavior, download, and auth.

Optional .env vars at project root:

    HTTP_TEST_BASE_URL=https://httpbin.org   (default)

Run:
    python tests/http_helper/test_http_helper.py
"""

import asyncio
import hashlib
import os
import shutil
import sys
from pathlib import Path

# ---------------------------------------------------------------------------
# .env loader
# ---------------------------------------------------------------------------

def _load_env():
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

BASE_URL = os.environ.get("HTTP_TEST_BASE_URL", "https://httpbin.org")

# Local workspace
LOCAL_TEST_DIR = Path(__file__).resolve().parent / "_test_workspace"

# ---------------------------------------------------------------------------
# Import
# ---------------------------------------------------------------------------

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))
from inopyutils.http_helper import InoHttpHelper

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

passed = 0
failed = 0

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
        print(f"  [FAIL] {name}")
        if fail_msg:
            print(f"         {fail_msg}")


def _cleanup():
    if LOCAL_TEST_DIR.exists():
        shutil.rmtree(LOCAL_TEST_DIR, ignore_errors=True)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

async def run_tests():
    global passed, failed

    print("=" * 60)
    print("HTTP Helper Integration Test")
    print(f"  base_url: {BASE_URL}")
    print("=" * 60)

    _cleanup()
    LOCAL_TEST_DIR.mkdir(parents=True, exist_ok=True)

    # ------------------------------------------------------------------
    # 1. GET — JSON response
    # ------------------------------------------------------------------
    print("\n--- GET JSON ---")

    async with InoHttpHelper(base_url=BASE_URL, retries=1) as client:

        res = await client.get("/get", json=True, params={"foo": "bar"})
        check("GET /get", res, lambda r: isinstance(r.get("data"), dict))
        if res.get("success"):
            args = res["data"].get("args", {})
            check_bool("GET query param", args.get("foo") == "bar", f"args={args}")

        # ------------------------------------------------------------------
        # 2. GET — text response
        # ------------------------------------------------------------------
        print("\n--- GET text ---")

        res = await client.get("/html")
        check("GET /html", res, lambda r: "Herman Melville" in str(r.get("data", "")))

        # ------------------------------------------------------------------
        # 3. GET — bytes response
        # ------------------------------------------------------------------
        print("\n--- GET bytes ---")

        res = await client.get("/bytes/512", return_bytes=True)
        check("GET /bytes/512", res, lambda r: isinstance(r.get("data"), bytes) and len(r["data"]) == 512)

        # ------------------------------------------------------------------
        # 4. POST — JSON body + JSON response
        # ------------------------------------------------------------------
        print("\n--- POST JSON ---")

        payload = {"name": "InoPyUtils", "version": "1.7.7"}
        res = await client.post("/post", json=payload, json_response=True)
        check("POST /post", res)
        if res.get("success"):
            returned_json = res["data"].get("json", {})
            check_bool("POST body echo", returned_json == payload, f"got={returned_json}")

        # ------------------------------------------------------------------
        # 5. POST — form data
        # ------------------------------------------------------------------
        print("\n--- POST form data ---")

        from aiohttp import FormData
        form = FormData()
        form.add_field("field1", "value1")
        form.add_field("field2", "value2")
        res = await client.post("/post", data=form, json_response=True)
        check("POST form", res)
        if res.get("success"):
            form_data = res["data"].get("form", {})
            check_bool("POST form echo", form_data.get("field1") == "value1", f"form={form_data}")

        # ------------------------------------------------------------------
        # 6. PUT
        # ------------------------------------------------------------------
        print("\n--- PUT ---")

        res = await client.put("/put", json={"key": "val"}, json_response=True)
        check("PUT /put", res)
        if res.get("success"):
            check_bool("PUT body echo", res["data"].get("json", {}).get("key") == "val")

        # ------------------------------------------------------------------
        # 7. PATCH
        # ------------------------------------------------------------------
        print("\n--- PATCH ---")

        res = await client.patch("/patch", json={"patched": True}, json_response=True)
        check("PATCH /patch", res)
        if res.get("success"):
            check_bool("PATCH body echo", res["data"].get("json", {}).get("patched") is True)

        # ------------------------------------------------------------------
        # 8. DELETE
        # ------------------------------------------------------------------
        print("\n--- DELETE ---")

        res = await client.delete("/delete", json=True)
        check("DELETE /delete", res)

        # ------------------------------------------------------------------
        # 9. Custom headers
        # ------------------------------------------------------------------
        print("\n--- Custom headers ---")

        res = await client.get(
            "/headers",
            headers={"X-Custom-Test": "hello123"},
            json=True,
        )
        check("GET /headers", res)
        if res.get("success"):
            returned_headers = res["data"].get("headers", {})
            check_bool(
                "custom header echo",
                returned_headers.get("X-Custom-Test") == "hello123",
                f"headers={returned_headers}",
            )

        # ------------------------------------------------------------------
        # 10. Status codes — 4xx returns success=False
        # ------------------------------------------------------------------
        print("\n--- Status codes ---")

        res = await client.get("/status/404")
        check_bool("404 returns success=False", not res.get("success"))
        check_bool("404 status_code", res.get("status_code") == 404, f"got {res.get('status_code')}")

        res = await client.get("/status/200")
        check("200 returns success=True", res)

        # ------------------------------------------------------------------
        # 11. Retry on 5xx
        # ------------------------------------------------------------------
        print("\n--- Retry on 5xx ---")

        res = await client.get("/status/503")
        check_bool("503 retried and failed", not res.get("success"))
        check_bool("503 attempts > 1", (res.get("attempts", 0)) > 1, f"attempts={res.get('attempts')}")

        # ------------------------------------------------------------------
        # 12. Timeout handling
        # ------------------------------------------------------------------
        print("\n--- Timeout ---")

        import aiohttp
        short_timeout = aiohttp.ClientTimeout(total=2)
        res = await client.get("/delay/10", timeout=short_timeout)
        check_bool("timeout returns success=False", not res.get("success"))

        # ------------------------------------------------------------------
        # 13. Basic auth
        # ------------------------------------------------------------------
        print("\n--- Basic auth ---")

        res = await client.get(
            "/basic-auth/testuser/testpass",
            auth=("testuser", "testpass"),
            json=True,
        )
        check("basic auth", res, lambda r: r.get("data", {}).get("authenticated") is True)

        # ------------------------------------------------------------------
        # 14. Default headers via constructor
        # ------------------------------------------------------------------
        print("\n--- Default headers ---")

    async with InoHttpHelper(
        base_url=BASE_URL,
        default_headers={"X-Default": "from-constructor"},
        retries=0,
    ) as client2:
        res = await client2.get("/headers", json=True)
        check("default headers", res)
        if res.get("success"):
            h = res["data"].get("headers", {})
            check_bool("default header present", h.get("X-Default") == "from-constructor", f"headers={h}")

        # ------------------------------------------------------------------
        # 15. Base URL composition
        # ------------------------------------------------------------------
        print("\n--- Base URL ---")

        res = await client2.get("/get", json=True)
        check("relative URL with base", res, lambda r: r.get("url", "").startswith(BASE_URL))

        # Full URL should bypass base
        res = await client2.get(f"{BASE_URL}/get", json=True)
        check("absolute URL bypasses base", res)

    # ------------------------------------------------------------------
    # 16. Download to file
    # ------------------------------------------------------------------
    print("\n--- Download ---")

    async with InoHttpHelper(retries=1) as dl_client:
        dl_path = LOCAL_TEST_DIR / "download"
        dl_path.mkdir(parents=True, exist_ok=True)

        # Download a known-size binary
        res = await dl_client.download(
            f"{BASE_URL}/bytes/2048",
            dest_path=str(dl_path / "test.bin"),
            overwrite=True,
        )
        check("download file", res)
        if res.get("success"):
            downloaded_file = Path(res.get("path", ""))
            check_bool("download file exists", downloaded_file.exists())
            check_bool("download file size", downloaded_file.stat().st_size == 2048, f"size={downloaded_file.stat().st_size}")

        # Download to directory (auto-derive filename)
        res = await dl_client.download(
            f"{BASE_URL}/image/png",
            dest_path=str(dl_path),
            overwrite=True,
        )
        check("download to dir", res)
        if res.get("success"):
            dl_file = Path(res.get("path", ""))
            check_bool("download auto-name exists", dl_file.exists() and dl_file.stat().st_size > 0)

        # Download with explicit filename
        res = await dl_client.download(
            f"{BASE_URL}/bytes/1024",
            dest_path=str(dl_path),
            filename="custom_name.bin",
            overwrite=True,
        )
        check("download custom filename", res)
        if res.get("success"):
            check_bool("custom filename used", Path(res.get("path", "")).name == "custom_name.bin")

        # Download overwrite=False on existing file
        res = await dl_client.download(
            f"{BASE_URL}/bytes/1024",
            dest_path=str(dl_path / "test.bin"),
            overwrite=False,
        )
        check_bool("download overwrite=False blocked", not res.get("success"))

    # ------------------------------------------------------------------
    # 17. Context manager + manual close
    # ------------------------------------------------------------------
    print("\n--- Lifecycle ---")

    client_manual = InoHttpHelper(base_url=BASE_URL, retries=0)
    res = await client_manual.get("/get", json=True)
    check("manual (no context manager)", res)
    await client_manual.close()
    check_bool("close() completed", True)

    # Session recreated after close
    res = await client_manual.get("/get", json=True)
    check("request after close (session recreated)", res)
    await client_manual.close()

    # ------------------------------------------------------------------
    # Summary
    # ------------------------------------------------------------------
    _cleanup()

    print("\n" + "=" * 60)
    total = passed + failed
    print(f"Results: {passed}/{total} passed, {failed} failed")
    print("=" * 60)


if __name__ == "__main__":
    asyncio.run(run_tests())
