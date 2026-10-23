"""Structured console and rotating file logger for the Lakehouse pipeline."""

from __future__ import annotations

import logging
import sys
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Optional

from src.config.settings import get_settings

_CONFIGURED_LOGGERS: set[str] = set()


def get_logger(name: str, log_file: Optional[Path] = None) -> logging.Logger:
    """Return a logger configured with both stdout and local rotating file output."""
    settings = get_settings()
    logger = logging.getLogger(name)

    if name in _CONFIGURED_LOGGERS:
        return logger

    level = getattr(logging, settings.log_level.upper(), logging.INFO)
    logger.setLevel(level)
    logger.propagate = False

    formatter = logging.Formatter(
        fmt="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # 1. Console Handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(level)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    # 2. Local Rotating File Handler (logs/pipeline_execution.log)
    target_log_file = log_file or (settings.log_dir / "pipeline_execution.log")
    target_log_file.parent.mkdir(parents=True, exist_ok=True)
    file_handler = RotatingFileHandler(
        filename=target_log_file,
        maxBytes=10 * 1024 * 1024,  # 10 MB per file
        backupCount=5,
        encoding="utf-8",
    )
    file_handler.setLevel(level)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    _CONFIGURED_LOGGERS.add(name)
    return logger
