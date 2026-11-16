import json
import os
import time
from pathlib import Path
from runpy import run_path
from unittest.mock import patch

import pytest

from payments import api, cli


def test_api_entrypoint_safe_logging() -> None:
    with patch("payments.api.configure_logging") as configure, patch("uvicorn.run") as run:
        api.main()
    configure.assert_called_once_with()
    assert run.call_args.kwargs["access_log"] is False
    assert run.call_args.kwargs["log_config"] is None
    assert run.call_args.kwargs["factory"] is True


@pytest.mark.parametrize("host", [None, "0.0.0.0"])
def test_receiver_explicit_dev_bind(monkeypatch: pytest.MonkeyPatch, host: str | None) -> None:
    receiver = run_path("examples/webhook_receiver.py", run_name="receiver_test")
    monkeypatch.setenv("ENVIRONMENT", "development")
    monkeypatch.setenv("WEBHOOK_SECRET", "test-only")
    monkeypatch.delenv("WEBHOOK_RECEIVER_HOST", raising=False)
    if host:
        monkeypatch.setenv("WEBHOOK_RECEIVER_HOST", host)
    with patch("aiohttp.web.run_app") as run:
        receiver["main"]()
    assert run.call_args.kwargs["host"] == (host or "127.0.0.1")


def test_receiver_production_guard_even_with_bind(monkeypatch: pytest.MonkeyPatch) -> None:
    receiver = run_path("examples/webhook_receiver.py", run_name="receiver_test")
    monkeypatch.setenv("ENVIRONMENT", "production")
    monkeypatch.setenv("WEBHOOK_RECEIVER_HOST", "0.0.0.0")
    with pytest.raises(RuntimeError, match="requires development"):
        receiver["main"]()


def test_health_cli_no_settings_or_logging(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    from payments.health import process_identity

    path = tmp_path / "health.json"
    payload = {
        "state": "running",
        "consumer_alive": True,
        "relay_alive": True,
        "timestamp": time.time(),
        "pid": os.getpid(),
        "process_start": process_identity(os.getpid()),
    }
    with (
        patch("payments.cli.Settings", side_effect=AssertionError),
        patch("payments.cli.configure_logging", side_effect=AssertionError),
    ):
        assert cli.main(["worker-health", "--path", str(path)]) == 1
        path.write_text(json.dumps(payload))
        assert cli.main(["worker-health", "--path", str(path)]) == 0
        for field, bad in [
            ("state", "starting"),
            ("state", "stopping"),
            ("state", "stopped"),
            ("state", "fatal"),
            ("consumer_alive", False),
            ("consumer_alive", 1),
            ("relay_alive", False),
            ("timestamp", time.time() - 31),
            ("timestamp", time.time() + 30),
            ("timestamp", float("nan")),
            ("timestamp", 10**500),
            ("process_start", "stale-previous-process"),
            ("pid", 999999999),
        ]:
            path.write_text(json.dumps({**payload, field: bad}))
            assert cli.main(["worker-health", "--path", str(path)]) == 1
        path.write_text("broken")
        assert cli.main(["worker-health", "--path", str(path)]) == 1
    captured = capsys.readouterr()
    assert captured.out == captured.err == ""
