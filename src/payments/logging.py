"""Fail-closed JSON logging: neither messages nor traceback text are trusted."""

import json
import logging
import math
import sys
from contextlib import suppress
from datetime import UTC, datetime
from uuid import UUID

EVENTS = frozenset(
    {
        "payment_processed",
        "payment_rejected",
        "payment_requeued",
        "worker_heartbeat",
        "worker_started",
        "worker_stopped",
        "worker_failed",
        "outbox_iteration_failed",
        "replay_created",
        "replay_rejected",
        "command_failed",
        "log_redacted",
    }
)
CATEGORIES = frozenset(
    {
        "infrastructure_error",
        "invalid_event",
        "worker_running",
        "worker_not_ready",
        "worker_fatal",
        "unexpected_error",
        "shutdown_error",
        "configuration_error",
    }
)


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        result: dict[str, object] = {
            "timestamp": datetime.fromtimestamp(record.created, UTC).isoformat(),
            "level": logging.getLevelName(record.levelno)
            if record.levelno in {10, 20, 30, 40, 50}
            else "INFO",
            "event": record.msg
            if isinstance(record.msg, str) and record.msg in EVENTS
            else "log_redacted",
        }
        # Validate values too: a safe field name must not smuggle a URL/exception.
        for field in ("event_id", "payment_id"):
            value = getattr(record, field, None)
            if isinstance(value, (str, UUID)):
                with suppress(ValueError):
                    result[field] = str(UUID(str(value)))
        attempt = getattr(record, "attempt", None)
        if type(attempt) is int and 1 <= attempt <= 3:
            result["attempt"] = attempt
        duration = getattr(record, "duration", None)
        if (
            isinstance(duration, (float, int))
            and not isinstance(duration, bool)
            and 0 <= duration <= 1e12
            and math.isfinite(duration)
        ):
            result["duration"] = duration
        category = getattr(record, "errorcategory", None)
        if isinstance(category, str) and category in CATEGORIES:
            result["errorcategory"] = category
        # Deliberately never call getMessage/formatException/formatStack or super.
        return json.dumps(result, ensure_ascii=True, allow_nan=False)


def configure_logging() -> None:
    handler = logging.StreamHandler(sys.stderr)
    handler.setFormatter(JsonFormatter())
    root = logging.getLogger()
    root.handlers = [handler]
    root.setLevel(logging.INFO)
    # Libraries may otherwise install their own formatter containing a DSN.
    for logger in logging.Logger.manager.loggerDict.values():
        if isinstance(logger, logging.Logger):
            logger.handlers.clear()
            logger.propagate = True
