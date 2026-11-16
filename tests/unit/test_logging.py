import io
import json
import logging
from uuid import uuid4

from payments.logging import JsonFormatter


def test_log_redacts_secrets() -> None:
    stream = io.StringIO()
    handler = logging.StreamHandler(stream)
    handler.setFormatter(JsonFormatter())
    logger = logging.Logger("test")
    logger.addHandler(handler)
    identifier = uuid4()
    try:
        raise RuntimeError("postgresql://user:private-password@private-host/db")
    except RuntimeError:
        logger.exception(
            "payment_processed",
            extra={
                "event_id": identifier,
                "payment_id": str(identifier),
                "attempt": 1,
                "duration": 0.25,
                "errorcategory": "infrastructure_error",
                "metadata": {"secret": "private-password"},
                "url": "https://private-host/hook?secret=private-password",
            },
            stack_info=True,
        )
    line = stream.getvalue()
    assert "private-" not in line
    record = json.loads(line)
    assert record["event_id"] == record["payment_id"] == str(identifier)
    assert record["attempt"] == 1 and record["duration"] == 0.25
    assert set(record) == {
        "timestamp",
        "level",
        "event",
        "event_id",
        "payment_id",
        "attempt",
        "duration",
        "errorcategory",
    }


def test_untrusted_message_and_allowlisted_field_values_are_not_logged() -> None:
    record = logging.LogRecord(
        "private-name", 40, "private-path", 1, "private-%s", ("secret",), None
    )
    record.event_id = "private-uuid"
    record.errorcategory = "private-category"
    record.attempt = "private-attempt"
    record.duration = float("inf")
    output = JsonFormatter().format(record)
    assert "private" not in output
    assert json.loads(output)["event"] == "log_redacted"


def test_huge_numeric_diagnostic_does_not_crash_formatter() -> None:
    record = logging.LogRecord("test", 20, "", 1, "payment_processed", (), None)
    record.duration = 10**1000
    assert "duration" not in json.loads(JsonFormatter().format(record))
