"""Small helper so every module logs the same way.

Pipeline code must use ``logging`` (never bare ``print``). Import ``get_logger``
and log through it; the root handler is configured once, idempotently.
"""

from __future__ import annotations

import logging
import os

_DEFAULT_FORMAT = "%(asctime)s %(levelname)-8s %(name)s: %(message)s"
_configured = False


def configure_logging(level: int | str | None = None) -> None:
    """Configure the root logger once. Safe to call repeatedly."""
    global _configured
    if _configured:
        return
    resolved = level if level is not None else os.environ.get("LOG_LEVEL", "INFO")
    logging.basicConfig(level=resolved, format=_DEFAULT_FORMAT)
    _configured = True


def get_logger(name: str) -> logging.Logger:
    """Return a configured logger for ``name`` (typically ``__name__``)."""
    configure_logging()
    return logging.getLogger(name)
