"""
RunPod Helper integration test.

Uses a real RunPod serverless vLLM endpoint.

Required .env vars at project root:

    RUNPOD_API_KEY=your_api_key
    RUNPOD_ENDPOINT_URL=https://api.runpod.ai/v2/YOUR_ENDPOINT_ID/runsync

Run:
    python tests/runpod_helper/test_runpod_helper.py
"""

import asyncio
import os
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

API_KEY = os.environ.get("RUNPOD_API_KEY", "")
ENDPOINT_URL = os.environ.get("RUNPOD_ENDPOINT_URL", "")

# ---------------------------------------------------------------------------
# Import
# ---------------------------------------------------------------------------

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))
from inopyutils.runpod_helper import InoRunpodHelper

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

# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

async def run_tests():
    global passed, failed

    print("=" * 60)
    print("RunPod Helper Integration Test")
    print(f"  endpoint: {ENDPOINT_URL}")
    print("=" * 60)

    if not API_KEY or not ENDPOINT_URL:
        print("\n  [SKIP] RUNPOD_API_KEY and RUNPOD_ENDPOINT_URL must be set in .env")
        return

    # ------------------------------------------------------------------
    # 1. Basic text chat completion — success fields
    # ------------------------------------------------------------------
    print("\n--- Basic runsync (text) ---")

    res = await InoRunpodHelper.serverless_vllm_runsync(
        url=ENDPOINT_URL,
        api_key=API_KEY,
        system_prompt="You are a helpful assistant. Reply in one short sentence.",
        user_prompt="What is 2 + 2?",
        temperature=0.1,
        max_tokens=64,
    )
    check("runsync text", res)

    if res.get("success"):
        check_bool("has id", res.get("id") is not None, f"id={res.get('id')}")
        check_bool("status is COMPLETED", res.get("status") == "COMPLETED", f"status={res.get('status')}")
        check_bool("has delay_time", res.get("delay_time") is not None, f"delay_time={res.get('delay_time')}")
        check_bool("has execution_time", res.get("execution_time") is not None, f"execution_time={res.get('execution_time')}")
        check_bool("has output", res.get("output") is not None, f"output={res.get('output')}")
        check_bool("has choices", isinstance(res.get("choices"), list) and len(res["choices"]) > 0,
                    f"choices={res.get('choices')}")
        check_bool("has usage", res.get("usage") is not None, f"usage={res.get('usage')}")
        print(f"         choices: {res.get('choices')}")
        print(f"         usage: {res.get('usage')}")
        print(f"         delay_time: {res.get('delay_time')}ms, execution_time: {res.get('execution_time')}ms")

    # ------------------------------------------------------------------
    # 2. Custom sampling params
    # ------------------------------------------------------------------
    print("\n--- Custom sampling params ---")

    res = await InoRunpodHelper.serverless_vllm_runsync(
        url=ENDPOINT_URL,
        api_key=API_KEY,
        system_prompt="You are a helpful assistant.",
        user_prompt="Say the word 'hello' and nothing else.",
        temperature=0.0,
        max_tokens=16,
    )
    check("runsync custom params", res)
    if res.get("success"):
        print(f"         choices: {res.get('choices')}")

    # ------------------------------------------------------------------
    # 3. Invalid API key — should return ino_err with status_code
    # ------------------------------------------------------------------
    print("\n--- Invalid API key ---")

    res = await InoRunpodHelper.serverless_vllm_runsync(
        url=ENDPOINT_URL,
        api_key="invalid_key_12345",
        system_prompt="You are a helpful assistant.",
        user_prompt="Hello",
        max_tokens=16,
    )
    check_bool("invalid key returns error", not res.get("success"),
               f"expected failure but got: {res}")
    check_bool("error has status_code", res.get("status_code") is not None or res.get("error_code") is not None,
               f"result={res}")
    print(f"         msg: {res.get('msg')}")

    # ------------------------------------------------------------------
    # 4. Invalid URL — should return ino_err
    # ------------------------------------------------------------------
    print("\n--- Invalid endpoint URL ---")

    res = await InoRunpodHelper.serverless_vllm_runsync(
        url="https://api.runpod.ai/v2/nonexistent_endpoint_xyz/runsync",
        api_key=API_KEY,
        system_prompt="You are a helpful assistant.",
        user_prompt="Hello",
        max_tokens=16,
    )
    check_bool("invalid endpoint returns error", not res.get("success"),
               f"expected failure but got: {res}")
    print(f"         msg: {res.get('msg')}")

    # ------------------------------------------------------------------
    # Summary
    # ------------------------------------------------------------------
    print("\n" + "=" * 60)
    total = passed + failed
    print(f"Results: {passed}/{total} passed, {failed} failed")
    print("=" * 60)


if __name__ == "__main__":
    asyncio.run(run_tests())
