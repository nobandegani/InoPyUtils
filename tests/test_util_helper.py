import re
import time
from src.inopyutils.util_helper import InoUtilHelper


def test_get_date_time_utc_base64_returns_string():
    result = InoUtilHelper.get_date_time_utc_base64()
    assert isinstance(result, str)
    assert len(result) > 0


def test_get_date_time_utc_base64_is_lowercase_base32():
    result = InoUtilHelper.get_date_time_utc_base64()
    assert re.fullmatch(r"[a-z2-7]+", result), f"Expected lowercase base32 chars, got: {result}"


def test_get_date_time_utc_base64_uniqueness():
    results = {InoUtilHelper.get_date_time_utc_base64() for _ in range(1000)}
    assert len(results) == 1000, f"Expected 1000 unique IDs, got {len(results)}"


def test_get_date_time_utc_base64_longer_with_more_random_bytes():
    short = InoUtilHelper.get_date_time_utc_base64(random_token=2)
    long = InoUtilHelper.get_date_time_utc_base64(random_token=8)
    assert len(long) > len(short)


def test_generate_unique_id_by_time_returns_sha256_hex():
    result = InoUtilHelper.generate_unique_id_by_time()
    assert isinstance(result, str)
    assert len(result) == 64
    assert re.fullmatch(r"[0-9a-f]{64}", result)


def test_generate_unique_id_by_time_uniqueness():
    results = set()
    for _ in range(10):
        results.add(InoUtilHelper.generate_unique_id_by_time())
        time.sleep(0.001)
    assert len(results) >= 5, f"Expected at least 5 unique IDs out of 10, got {len(results)}"


if __name__ == "__main__":
    test_get_date_time_utc_base64_returns_string()
    test_get_date_time_utc_base64_is_lowercase_base32()
    test_get_date_time_utc_base64_uniqueness()
    test_get_date_time_utc_base64_longer_with_more_random_bytes()
    test_generate_unique_id_by_time_returns_sha256_hex()
    test_generate_unique_id_by_time_uniqueness()
    print("All tests passed!")
