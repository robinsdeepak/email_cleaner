"""
Centralized logging engine for Gmail AI Cleaner.

Provides simultaneous console formatting and persistent rotating file logging
with millisecond-precision timestamps, thread IDs, log levels, and module contexts.
"""

from datetime import datetime
import logging
from logging.handlers import RotatingFileHandler
import os
import sys
from typing import Optional

from gmail_cleaner.config import GMAIL_USER
from gmail_cleaner.state import get_step_dir

# Default root logger name for the application
APP_LOGGER_NAME = "gmail_cleaner"

_is_configured = False
_current_log_file = None


def setup_logger(email_addr: Optional[str] = None, log_file: Optional[str] = None,
                 log_level: int = logging.INFO) -> logging.Logger:
    """
    Configures and returns the application root logger.
    Attaches both a clean Console handler and a detailed RotatingFileHandler.
    """
    global _is_configured, _current_log_file
    logger = logging.getLogger(APP_LOGGER_NAME)

    if not log_file:
        base_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        log_dir = os.path.join(base_dir, "logs")
        log_file = os.path.join(log_dir, "cleaner.log")

    log_file = os.path.abspath(log_file)

    if _is_configured and log_file == _current_log_file:
        return logger

    logger.setLevel(logging.DEBUG)  # Allow all levels; handlers filter independently
    logger.propagate = False

    # Clear existing handlers to prevent duplicate outputs
    for h in list(logger.handlers):
        logger.removeHandler(h)

    # 1. Console Handler (human-friendly, preserves formatting)
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(log_level)
    console_formatter = logging.Formatter(fmt="%(message)s")
    console_handler.setFormatter(console_formatter)
    logger.addHandler(console_handler)

    # 2. File Handler (detailed, rotating, timestamped with thread/module info)
    os.makedirs(os.path.dirname(os.path.abspath(log_file)), exist_ok=True)

    # 10 MB per log file, keeps up to 5 historical rotated backups
    file_handler = RotatingFileHandler(
        log_file,
        maxBytes=10 * 1024 * 1024,
        backupCount=5,
        encoding="utf-8"
    )
    file_handler.setLevel(logging.DEBUG)  # File captures debug details
    file_formatter = logging.Formatter(
        fmt="%(asctime)s.%(msecs)03d | %(levelname)-7s | [%(name)s:%(threadName)s] | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    file_handler.setFormatter(file_formatter)
    logger.addHandler(file_handler)

    _is_configured = True
    _current_log_file = log_file
    logger.debug(f"Logger initialized. File logs active at: {log_file}")

    # Suppress verbose 3rd party SDK internal warnings
    logging.getLogger("google").setLevel(logging.ERROR)
    logging.getLogger("google.genai").setLevel(logging.ERROR)

    return logger


def get_logger(module_name: Optional[str] = None) -> logging.Logger:
    """
    Returns a child logger for a specific module or component.
    Example: logger = get_logger(__name__)
    """
    if not _is_configured:
        setup_logger()

    if module_name:
        if module_name.startswith(APP_LOGGER_NAME):
            name = module_name
        else:
            short_name = module_name.split(".")[-1]
            name = f"{APP_LOGGER_NAME}.{short_name}"
        return logging.getLogger(name)
    return logging.getLogger(APP_LOGGER_NAME)


def set_console_level(level: int):
    """Dynamically adjusts the console log handler level (e.g. logging.DEBUG for verbose mode)."""
    logger = logging.getLogger(APP_LOGGER_NAME)
    for h in logger.handlers:
        if isinstance(h, logging.StreamHandler) and not isinstance(h, RotatingFileHandler):
            h.setLevel(level)


def get_active_log_file() -> Optional[str]:
    """Returns the absolute path to the active log file."""
    return _current_log_file
