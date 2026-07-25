"""Structured JSON logging (task req 10).

Installs a single ``JsonFormatter`` on the *root* logger so every log record
— ours, Scrapy's, twisted's, MinIO's, PyMongo's — comes out as one JSON
object per line. Any keyword args passed via ``extra={...}`` are merged into
the top-level object, which is how callers attach the ``event``,
``partition_date``, ``body``, counts etc. that the task asks for.

Idempotent: repeated ``install_json_root_logging`` calls only touch the
level.
"""

from __future__ import annotations

import json
import logging
import sys
from datetime import datetime, timezone

# Standard LogRecord attributes — anything else on the record was passed via
# ``extra={...}`` and belongs in the JSON output.
_RESERVED = frozenset(
    (
        "name", "msg", "args", "levelname", "levelno", "pathname", "filename",
        "module", "exc_info", "exc_text", "stack_info", "lineno", "funcName",
        "created", "msecs", "relativeCreated", "thread", "threadName",
        "processName", "process", "message", "taskName",
    )
)


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload: dict = {
            "ts": datetime.fromtimestamp(record.created, tz=timezone.utc).isoformat(timespec="milliseconds"),
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
        }
        for k, v in record.__dict__.items():
            if k not in _RESERVED and not k.startswith("_"):
                payload[k] = v
        if record.exc_info:
            payload["exc"] = self.formatException(record.exc_info)
        return json.dumps(payload, default=str, ensure_ascii=False)


def _resolve_level(level: int | str) -> int:
    if isinstance(level, int):
        return level
    return getattr(logging, str(level).upper(), logging.INFO)


def install_json_root_logging(level: int | str = "INFO") -> None:
    """Attach ``JsonFormatter`` to the root logger. Safe to call repeatedly.

    Called once at process start (from Scrapy settings). All named loggers
    propagate here, so we do not need per-logger handlers — no risk of
    duplicate output.
    """
    root = logging.getLogger()
    lvl = _resolve_level(level)
    if getattr(root, "_wrc_json_configured", False):
        root.setLevel(lvl)
        return
    for h in list(root.handlers):
        root.removeHandler(h)
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JsonFormatter())
    root.addHandler(handler)
    root.setLevel(lvl)
    root._wrc_json_configured = True  # type: ignore[attr-defined]


def get_json_logger(name: str) -> logging.Logger:
    """Return a named logger that will emit JSON via the root handler."""
    install_json_root_logging()
    return logging.getLogger(name)
