"""Application logging helpers."""

from __future__ import annotations

import logging
import sys
import threading
from logging.handlers import RotatingFileHandler
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_LOG_PATH = ROOT / "data" / "app.log"
_configured_path: Path | None = None


def configure_logging(
    log_path: str | Path | None = None,
    level: int = logging.INFO,
    force: bool = False,
) -> Path:
    """Configure one rotating application log file."""
    global _configured_path

    path = Path(log_path) if log_path is not None else DEFAULT_LOG_PATH
    path = path if path.is_absolute() else ROOT / path
    if _configured_path == path and not force:
        return path

    path.parent.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger("fourty")
    logger.setLevel(level)
    logger.propagate = False
    for handler in list(logger.handlers):
        logger.removeHandler(handler)
        handler.close()

    handler = RotatingFileHandler(
        path,
        maxBytes=2 * 1024 * 1024,
        backupCount=3,
        encoding="utf-8",
    )
    handler.setFormatter(
        logging.Formatter(
            "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
        )
    )
    logger.addHandler(handler)
    _configured_path = path
    _install_exception_hooks(logger)
    return path


def get_logger(name: str = "fourty") -> logging.Logger:
    """Return a configured logger."""
    if _configured_path is None:
        configure_logging()
    logger_name = (
        name
        if name == "fourty" or name.startswith("fourty.")
        else f"fourty.{name}"
    )
    return logging.getLogger(logger_name)


def log_exception(
    logger: logging.Logger,
    context: str,
    exc: BaseException,
) -> None:
    """Log an exception with traceback and context."""
    logger.error("%s: %s", context, exc, exc_info=True)
    for handler in logging.getLogger("fourty").handlers:
        handler.flush()


def _install_exception_hooks(logger: logging.Logger) -> None:
    """Capture uncaught exceptions from the main and worker threads."""

    def handle_exception(exc_type, exc_value, exc_traceback) -> None:
        if issubclass(exc_type, KeyboardInterrupt):
            sys.__excepthook__(exc_type, exc_value, exc_traceback)
            return
        logger.critical(
            "uncaught exception",
            exc_info=(exc_type, exc_value, exc_traceback),
        )

    def handle_thread_exception(args: threading.ExceptHookArgs) -> None:
        if args.exc_type is SystemExit:
            return
        logger.critical(
            "uncaught thread exception thread=%s",
            args.thread.name if args.thread else "unknown",
            exc_info=(args.exc_type, args.exc_value, args.exc_traceback),
        )

    sys.excepthook = handle_exception
    threading.excepthook = handle_thread_exception
