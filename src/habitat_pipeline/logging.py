"""Structured logging setup.

structlog is configured once at startup and produces:
- ``prod``: single-line JSON, suitable for any log sink.
- anything else: colored console with key-value pairs.
"""

from __future__ import annotations

import logging
import sys
from typing import cast

import structlog


def configure_logging(env: str = "dev", level: int = logging.INFO) -> None:
    """Configure structlog + stdlib logging in one call."""
    timestamper = structlog.processors.TimeStamper(fmt="iso", utc=True)

    shared_processors: list[structlog.types.Processor] = [
        structlog.contextvars.merge_contextvars,
        structlog.processors.add_log_level,
        structlog.processors.StackInfoRenderer(),
        timestamper,
    ]

    if env == "prod":
        renderer: structlog.types.Processor = structlog.processors.JSONRenderer()
    else:
        renderer = structlog.dev.ConsoleRenderer(colors=sys.stderr.isatty())

    structlog.configure(
        processors=[*shared_processors, renderer],
        wrapper_class=structlog.make_filtering_bound_logger(level),
        logger_factory=structlog.PrintLoggerFactory(file=sys.stderr),
        cache_logger_on_first_use=True,
    )

    # Route stdlib logs (from httpx, etc.) through structlog as well.
    logging.basicConfig(level=level, handlers=[], force=True)


def get_logger(name: str | None = None) -> structlog.types.FilteringBoundLogger:
    """Return a bound logger. Prefer per-module loggers named for their module."""
    # structlog.get_logger() is typed to return Any; the actual runtime type
    # is FilteringBoundLogger, matching the wrapper_class configured above.
    return cast(structlog.types.FilteringBoundLogger, structlog.get_logger(name))
