"""Development-only HMAC receiver; never use its in-memory dedupe in production.

Run with ENVIRONMENT=development, WEBHOOK_SECRET and optionally
WEBHOOK_FAIL_FIRST=1 using ``uv run python examples/webhook_receiver.py``.
List localhost in WEBHOOK_ALLOWED_DOMAINS and explicitly enable
ALLOW_UNSAFE_WEBHOOKS on the development sender. POST target:
http://localhost:8081/webhook. Access logs are disabled to avoid query leaks.
"""

import hmac
import json
import os
import re
import time
from collections import deque

from aiohttp import web

from payments.webhooks import sign_payload


def create_app(secret: str, *, fail_first: int = 0) -> web.Application:
    """Verify raw bytes/freshness, emulate failures, dedupe successful event IDs."""
    if not secret.strip() or fail_first < 0:
        raise ValueError("invalid development receiver configuration")
    remaining_failures = fail_first
    delivered: set[str] = set()
    order: deque[str] = deque()

    async def receive(request: web.Request) -> web.Response:
        nonlocal remaining_failures
        timestamps = request.headers.getall("X-Webhook-Timestamp", [])
        signatures = request.headers.getall("X-Webhook-Signature", [])
        if len(timestamps) != 1 or len(signatures) != 1:
            return web.Response(status=401)
        timestamp, signature = timestamps[0], signatures[0]
        if (
            re.fullmatch(r"[0-9]{1,12}", timestamp) is None
            or re.fullmatch(r"v1=[0-9a-f]{64}", signature) is None
            or abs(time.time() - int(timestamp)) > 300
        ):
            return web.Response(status=401)
        raw_body = await request.read()
        if not hmac.compare_digest(signature, sign_payload(secret, timestamp, raw_body)):
            return web.Response(status=401)
        try:
            body = json.loads(raw_body)
        except (ValueError, RecursionError):
            return web.Response(status=400)
        event_id = body.get("event_id") if isinstance(body, dict) else None
        if (
            not isinstance(event_id, str)
            or not 1 <= len(event_id) <= 128
            or request.headers.getall("Idempotency-Key", []) != [event_id]
        ):
            return web.Response(status=400)
        if event_id in delivered:
            return web.Response(status=204)
        if remaining_failures:
            remaining_failures -= 1
            return web.Response(status=500)
        if len(order) == 4096:
            delivered.remove(order.popleft())
        order.append(event_id)
        delivered.add(event_id)
        return web.Response(status=204)

    app = web.Application(client_max_size=32 * 1024)
    app.router.add_post("/webhook", receive)
    return app


def main() -> None:
    if os.environ.get("ENVIRONMENT") not in {"development", "test"}:
        raise RuntimeError("receiver requires development or test environment")
    app = create_app(
        os.environ.get("WEBHOOK_SECRET", ""),
        fail_first=int(os.environ.get("WEBHOOK_FAIL_FIRST", "0")),
    )
    host = os.environ.get("WEBHOOK_RECEIVER_HOST", "127.0.0.1")
    web.run_app(app, host=host, port=8081, access_log=None, print=None)


if __name__ == "__main__":
    main()
