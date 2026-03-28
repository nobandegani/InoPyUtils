"""
OpenAI Helper integration test.

Uses a RunPod serverless vLLM endpoint via the OpenAI-compatible API.

Required .env vars at project root:

    RUNPOD_API_KEY=your_api_key
    RUNPOD_ENDPOINT_URL=https://api.runpod.ai/v2/YOUR_ENDPOINT_ID

Run:
    python tests/openai_helper/test_openai_helper.py
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

API_KEY = os.environ.get("OPENAI_API_KEY", "")
ENDPOINT_URL = os.environ.get("OPENAI_ENDPOINT_URL", "")
BASE_URL = ENDPOINT_URL.rstrip("/") if ENDPOINT_URL else ""
MODEL = os.environ.get("OPENAI_MODEL", "meta-llama/Llama-2-7b-chat-hf")

# ---------------------------------------------------------------------------
# Import
# ---------------------------------------------------------------------------

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))
from inopyutils.openai_helper import InoOpenAIHelper

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
    print("OpenAI Helper Integration Test")
    print(f"  base_url: {BASE_URL}")
    print(f"  model:    {MODEL}")
    print("=" * 60)

    if not API_KEY or not ENDPOINT_URL:
        print("\n  [SKIP] RUNPOD_API_KEY and RUNPOD_ENDPOINT_URL must be set in .env")
        return

    # ------------------------------------------------------------------
    # 1. Basic chat — user prompt only
    # ------------------------------------------------------------------
    print("\n--- Basic chat (user prompt only) ---")

    res = await InoOpenAIHelper.chat_completions(
        api_key=API_KEY,
        base_url=BASE_URL,
        model=MODEL,
        user_prompt="List 3 benefits of drinking water. Keep each to one sentence.",
        temperature=0.3,
        max_tokens=256,
    )
    check("basic chat", res)

    if res.get("success"):
        check_bool("has response text", isinstance(res.get("response"), str) and len(res["response"]) > 0,
                    f"response={res.get('response')}")
        check_bool("has finish_reason", res.get("finish_reason") is not None,
                    f"finish_reason={res.get('finish_reason')}")
        check_bool("has usage", res.get("usage") is not None, f"usage={res.get('usage')}")
        check_bool("has raw response", res.get("raw") is not None)
        print(f"\n         --- Response ---\n{res.get('response')}")
        print(f"\n         usage: {res.get('usage')}")

    # ------------------------------------------------------------------
    # 2. With system prompt — constrained persona
    # ------------------------------------------------------------------
    print("\n--- With system prompt (persona) ---")

    res = await InoOpenAIHelper.chat_completions(
        api_key=API_KEY,
        base_url=BASE_URL,
        model=MODEL,
        system_prompt="You are a pirate captain. Always respond in pirate speak. Keep it short.",
        user_prompt="What's the weather like today?",
        temperature=0.7,
        max_tokens=128,
    )
    check("system prompt persona", res)
    if res.get("success"):
        print(f"\n         --- Response ---\n{res.get('response')}")

    # ------------------------------------------------------------------
    # 3. With system prompt — structured output
    # ------------------------------------------------------------------
    print("\n--- With system prompt (structured output) ---")

    res = await InoOpenAIHelper.chat_completions(
        api_key=API_KEY,
        base_url=BASE_URL,
        model=MODEL,
        system_prompt="You are a JSON API. Always respond with valid JSON only, no markdown. Schema: {\"sentiment\": \"positive|negative|neutral\", \"confidence\": 0.0-1.0}",
        user_prompt="I absolutely love this new phone, it's the best purchase I've made all year!",
        temperature=0,
        max_tokens=64,
    )
    check("system prompt structured", res)
    if res.get("success"):
        print(f"\n         --- Response ---\n{res.get('response')}")

    # ------------------------------------------------------------------
    # 4. Creative generation
    # ------------------------------------------------------------------
    print("\n--- Creative generation (high temperature) ---")

    res = await InoOpenAIHelper.chat_completions(
        api_key=API_KEY,
        base_url=BASE_URL,
        model=MODEL,
        system_prompt="You are a creative writing assistant. Write vivid, concise prose.",
        user_prompt="Write a 2-sentence story about a robot discovering music for the first time.",
        temperature=0.9,
        max_tokens=128,
    )
    check("creative generation", res)
    if res.get("success"):
        print(f"\n         --- Response ---\n{res.get('response')}")

    # ------------------------------------------------------------------
    # 5. Image input — local file as base64 data URI
    # ------------------------------------------------------------------
    print("\n--- Image input (base64 data URI) ---")

    image_path = Path(__file__).resolve().parents[1] / "assets" / "image.jpg"
    if image_path.exists():
        import base64
        image_bytes = image_path.read_bytes()
        image_b64 = base64.b64encode(image_bytes).decode("ascii")
        image_data_uri = f"data:image/jpeg;base64,{image_b64}"

        res = await InoOpenAIHelper.chat_completions(
            api_key=API_KEY,
            base_url=BASE_URL,
            model=MODEL,
            system_prompt="Describe images concisely in 1-2 sentences.",
            user_prompt="What do you see in this image?",
            image=image_data_uri,
            temperature=0.3,
            max_tokens=128,
        )
        check("image base64", res)
        if res.get("success"):
            check_bool("image has response", isinstance(res.get("response"), str) and len(res["response"]) > 0,
                        f"response={res.get('response')}")
            print(f"\n         --- Response ---\n{res.get('response')}")
        else:
            print(f"         msg: {res.get('msg')}")
    else:
        print(f"  [SKIP] image.jpg not found at {image_path}")

    # ------------------------------------------------------------------
    # 6. Invalid API key — should return ino_err
    # ------------------------------------------------------------------
    print("\n--- Invalid API key ---")

    res = await InoOpenAIHelper.chat_completions(
        api_key="invalid_key_12345",
        base_url=BASE_URL,
        model=MODEL,
        user_prompt="Hello",
        max_tokens=8,
    )
    check_bool("invalid key returns error", not res.get("success"),
               f"expected failure but got: {res}")
    print(f"         msg: {res.get('msg')}")

    # ------------------------------------------------------------------
    # 6. Invalid base_url — should return ino_err
    # ------------------------------------------------------------------
    print("\n--- Invalid base URL ---")

    res = await InoOpenAIHelper.chat_completions(
        api_key=API_KEY,
        base_url="https://api.runpod.ai/v2/nonexistent_xyz/openai/v1",
        model=MODEL,
        user_prompt="Hello",
        max_tokens=8,
    )
    check_bool("invalid url returns error", not res.get("success"),
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
