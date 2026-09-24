"""Launch the local Streamlit application."""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

from core.logging_utils import configure_logging, get_logger, log_exception


ROOT = Path(__file__).resolve().parent
LOGGER = get_logger("fourty.run")


def main() -> int:
    configure_logging()
    command = [
        sys.executable,
        "-m",
        "streamlit",
        "run",
        str(ROOT / "app" / "home.py"),
        "--server.headless=false",
        "--browser.gatherUsageStats=false",
    ]
    LOGGER.info("launch streamlit")
    try:
        return subprocess.call(command, cwd=ROOT)
    except Exception as exc:
        log_exception(LOGGER, "streamlit launch failed", exc)
        raise


if __name__ == "__main__":
    raise SystemExit(main())
