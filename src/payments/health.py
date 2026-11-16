"""Secret-free Linux process-bound heartbeat, atomically replaced by its owner."""

import json
import math
import os
import tempfile
import time
from pathlib import Path

DEFAULT_HEARTBEAT = Path("/tmp/payments-worker-health.json")


def process_identity(pid: int) -> str:
    # comm may contain spaces/parentheses. Field 22 follows the last ')'.
    return Path(f"/proc/{pid}/stat").read_text().rsplit(")", 1)[1].split()[19]


def write_heartbeat(path: Path, health: dict[str, str | bool]) -> None:
    payload = {
        **health,
        "timestamp": time.time(),
        "pid": os.getpid(),
        "process_start": process_identity(os.getpid()),
    }
    descriptor, temporary = tempfile.mkstemp(dir=path.parent, suffix=".tmp")
    try:
        with os.fdopen(descriptor, "w") as stream:
            json.dump(payload, stream, allow_nan=False)
        os.replace(temporary, path)
    finally:
        Path(temporary).unlink(missing_ok=True)


def healthy(path: Path = DEFAULT_HEARTBEAT) -> bool:
    try:
        payload = json.loads(path.read_text())
        if not isinstance(payload, dict):
            return False
        timestamp = payload.get("timestamp")
        pid = payload.get("pid")
        if (
            not isinstance(timestamp, int | float)
            or isinstance(timestamp, bool)
            or type(pid) is not int
            or pid <= 0
        ):
            return False
        age = time.time() - timestamp
        return bool(
            math.isfinite(age)
            and 0 <= age <= 30
            and payload.get("state") == "running"
            and payload.get("consumer_alive") is True
            and payload.get("relay_alive") is True
            and payload.get("process_start") == process_identity(pid)
        )
    except (OSError, ValueError, IndexError, TypeError, OverflowError):
        return False
