"""Launch the local Streamlit application."""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parent


def main() -> int:
    command = [
        sys.executable,
        "-m",
        "streamlit",
        "run",
        str(ROOT / "app" / "home.py"),
        "--server.headless=false",
        "--browser.gatherUsageStats=false",
    ]
    return subprocess.call(command, cwd=ROOT)


if __name__ == "__main__":
    raise SystemExit(main())
