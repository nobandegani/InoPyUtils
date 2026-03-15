"""
Publish inopyutils to PyPI.

Usage:
    python publish.py          — bump patch version (1.7.7 → 1.7.8) and publish
    python publish.py 1.8.0    — set exact version and publish

Requires:
    pip install build twine

Reads PYPI_API_TOKEN from .env at project root.
"""

import os
import re
import sys
import shutil
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parent
PYPROJECT = ROOT / "pyproject.toml"
DIST = ROOT / "dist"
ENV_FILE = ROOT / ".env"


# ---------------------------------------------------------------------------
# .env loader
# ---------------------------------------------------------------------------

def load_env():
    if not ENV_FILE.exists():
        return
    with open(ENV_FILE, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, _, value = line.partition("=")
            key = key.strip()
            value = value.strip().strip("'\"")
            if key:
                os.environ.setdefault(key, value)


# ---------------------------------------------------------------------------
# Version helpers
# ---------------------------------------------------------------------------

def read_version() -> str:
    text = PYPROJECT.read_text(encoding="utf-8")
    m = re.search(r'^version\s*=\s*"([^"]+)"', text, re.MULTILINE)
    if not m:
        print("ERROR: could not find version in pyproject.toml")
        sys.exit(1)
    return m.group(1)


def bump_patch(version: str) -> str:
    parts = version.split(".")
    if len(parts) != 3 or not all(p.isdigit() for p in parts):
        print(f"ERROR: cannot auto-bump non-standard version '{version}'")
        sys.exit(1)
    parts[2] = str(int(parts[2]) + 1)
    return ".".join(parts)


def write_version(new_version: str):
    text = PYPROJECT.read_text(encoding="utf-8")
    updated = re.sub(
        r'^(version\s*=\s*)"[^"]+"',
        f'\\1"{new_version}"',
        text,
        count=1,
        flags=re.MULTILINE,
    )
    PYPROJECT.write_text(updated, encoding="utf-8")


# ---------------------------------------------------------------------------
# Build & publish
# ---------------------------------------------------------------------------

def clean_dist():
    if DIST.exists():
        shutil.rmtree(DIST)
    print(f"  cleaned {DIST}")


def build():
    print("\n--- Building ---")
    result = subprocess.run(
        [sys.executable, "-m", "build"],
        cwd=str(ROOT),
    )
    if result.returncode != 0:
        print("ERROR: build failed")
        sys.exit(1)

    # Show what was built
    files = list(DIST.glob("*"))
    for f in files:
        size_kb = f.stat().st_size / 1024
        print(f"  {f.name}  ({size_kb:.1f} KB)")


def upload(token: str):
    print("\n--- Uploading to PyPI ---")
    env = os.environ.copy()
    env["TWINE_USERNAME"] = "__token__"
    env["TWINE_PASSWORD"] = token

    result = subprocess.run(
        [sys.executable, "-m", "twine", "upload", "dist/*"],
        cwd=str(ROOT),
        env=env,
    )
    if result.returncode != 0:
        print("ERROR: upload failed")
        sys.exit(1)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    load_env()

    current = read_version()
    print(f"Current version: {current}")

    # Determine new version
    if len(sys.argv) > 1:
        new_version = sys.argv[1].strip()
        print(f"Version from argument: {new_version}")
    else:
        auto = bump_patch(current)
        answer = input(f"Enter version (or press Enter for {auto}): ").strip()
        new_version = answer if answer else auto

    # Validate format
    if not re.match(r"^\d+\.\d+\.\d+$", new_version):
        print(f"ERROR: invalid version format '{new_version}' (expected X.Y.Z)")
        sys.exit(1)

    # Check token
    token = os.environ.get("PYPI_API_TOKEN", "")
    if not token:
        print("ERROR: PYPI_API_TOKEN not set. Add it to .env or set as environment variable.")
        sys.exit(1)

    # Confirm
    print(f"\n  {current} → {new_version}")
    print(f"  target: PyPI (pypi.org)")
    confirm = input("\nProceed? [y/N] ").strip().lower()
    if confirm != "y":
        print("Aborted.")
        sys.exit(0)

    # Update version
    if new_version != current:
        write_version(new_version)
        print(f"\nUpdated pyproject.toml: {current} → {new_version}")

    # Build & upload
    clean_dist()
    build()
    upload(token)

    print(f"\n--- Published inopyutils {new_version} to PyPI ---")


if __name__ == "__main__":
    main()
