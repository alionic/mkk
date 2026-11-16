import asyncio
import hashlib
import hmac
import json
import socket
import ssl
import subprocess
import time
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from runpy import run_path
from typing import Any
from uuid import uuid4

import pytest
from aiohttp import ClientSession, web

from payments.config import Settings
from payments.models import Payment
from payments.processor import DeliveryError, WebhookSender
from payments.webhooks import WebhookClient

from .http_helpers import capped_response_socket

Handler = Callable[[web.Request], Awaitable[web.StreamResponse]]


@pytest.fixture
def dev_settings(settings: Settings) -> Settings:
    return settings.model_copy(
        update={"allow_unsafe_webhooks": True, "webhook_allowed_domains": ["webhook.test"]}
    )


@pytest.fixture
def payment() -> Payment:
    return Payment(
        id=uuid4(),
        webhook_event_id=uuid4(),
        generation=uuid4(),
        amount=Decimal("42.50"),
        currency="EUR",
        status="failed",
        description="Тест",
        metadata_={"nested": {"ok": True}},
        created_at=datetime(2026, 11, 15, tzinfo=UTC),
        processed_at=datetime(2026, 11, 15, 0, 0, 3, tzinfo=UTC),
    )


@pytest.fixture
async def dns(monkeypatch: pytest.MonkeyPatch) -> list[str]:
    calls: list[str] = []

    async def lookup(host: str, port: int, *args: Any, **kwargs: Any) -> list[Any]:
        calls.append(host)
        assert host == "webhook.test", "Unexpected second hostname/DNS lookup"
        return [(socket.AF_INET, socket.SOCK_STREAM, 6, "", ("127.0.0.1", port))]

    monkeypatch.setattr(asyncio.get_running_loop(), "getaddrinfo", lookup)
    return calls


@asynccontextmanager
async def server(handler: Handler, *, tls: ssl.SSLContext | None = None) -> AsyncIterator[str]:
    app = web.Application()
    app.router.add_route("*", "/{path:.*}", handler)
    runner = web.AppRunner(app, access_log=None, shutdown_timeout=0.1)
    await runner.setup()
    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    site = web.SockSite(runner, sock, ssl_context=tls)
    try:
        await site.start()
        scheme = "https" if tls is not None else "http"
        yield f"{scheme}://webhook.test:{sock.getsockname()[1]}"
    finally:
        await runner.cleanup()


async def test_connection_uses_validated_ip(
    dev_settings: Settings, payment: Payment, dns: list[str]
) -> None:
    received: list[tuple[bytes, dict[str, str]]] = []

    async def handler(request: web.Request) -> web.Response:
        received.append((await request.read(), dict(request.headers)))
        return web.Response(status=204)

    async with server(handler) as url, WebhookClient(dev_settings) as client:
        sender: WebhookSender = client
        payment.webhook_url = url + "/hook?token=SECRET"
        await sender.send(payment)
        payment.generation = uuid4()
        payment.expected_attempt = 3
        await sender.send(payment)
    assert dns == ["webhook.test", "webhook.test"]  # No second lookup, no cache/reused socket.
    assert len(received) == 2
    assert received[0][0] == received[1][0]
    body, headers = received[0]
    assert headers["Idempotency-Key"] == str(payment.webhook_event_id)
    assert headers["Content-Type"] == "application/json"
    assert int(headers["X-Webhook-Timestamp"]) > 0
    expected = hmac.new(
        b"test-webhook-secret",
        headers["X-Webhook-Timestamp"].encode() + b"." + body,
        hashlib.sha256,
    ).hexdigest()
    assert headers["X-Webhook-Signature"] == "v1=" + expected
    assert json.loads(body)["status"] == "failed"


async def test_no_redirect_follow(dev_settings: Settings, payment: Payment, dns: list[str]) -> None:
    paths: list[str] = []

    async def handler(request: web.Request) -> web.Response:
        paths.append(request.path)
        return web.Response(status=302, headers={"Location": "/stolen?token=SECRET"})

    async with server(handler) as url, WebhookClient(dev_settings) as client:
        payment.webhook_url = url + "/hook"
        with pytest.raises(DeliveryError, match="^webhook_http_error$"):
            await client.send(payment)
    assert paths == ["/hook"]


async def test_proxy_env_ignored(
    dev_settings: Settings, payment: Payment, dns: list[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    for key in ["HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "http_proxy", "https_proxy", "all_proxy"]:
        monkeypatch.setenv(key, "http://proxy.invalid:1/?token=SECRET")
    monkeypatch.setenv("NO_PROXY", "")
    monkeypatch.setenv("no_proxy", "")
    hits = 0

    async def handler(request: web.Request) -> web.Response:
        nonlocal hits
        hits += 1
        return web.Response()

    async with server(handler) as url, WebhookClient(dev_settings) as client:
        payment.webhook_url = url
        await client.send(payment)
    assert hits == 1


@pytest.mark.parametrize("status", [200, 204, 299, 300, 400, 500])
async def test_http_status_policy(
    dev_settings: Settings,
    payment: Payment,
    dns: list[str],
    status: int,
    caplog: pytest.LogCaptureFixture,
) -> None:
    async def handler(request: web.Request) -> web.Response:
        return web.Response(status=status, text="SECRET response text")

    async with server(handler) as url, WebhookClient(dev_settings) as client:
        payment.webhook_url = url + "/?token=SECRET"
        if 200 <= status < 300:
            await client.send(payment)
        else:
            with pytest.raises(DeliveryError, match="^webhook_http_error$") as error:
                await client.send(payment)
            assert error.value.__suppress_context__
    assert "SECRET" not in caplog.text
    assert "test-webhook-secret" not in caplog.text


@pytest.mark.parametrize("streamed", [False, True])
async def test_timeout_and_bounded_response(
    dev_settings: Settings, payment: Payment, dns: list[str], streamed: bool
) -> None:
    async def handler(request: web.Request) -> web.StreamResponse:
        if not streamed:
            return web.Response(body=b"x" * (65536 + 1))
        response = web.StreamResponse()
        await response.prepare(request)
        await response.write(b"x" * (65536 + 1))
        await asyncio.sleep(1)  # Sender must reject cap+1 without waiting for EOF.
        return response

    async with server(handler) as url, WebhookClient(dev_settings) as client:
        payment.webhook_url = url
        with pytest.raises(DeliveryError, match="^webhook_http_error$"):
            await asyncio.wait_for(client.send(payment), 0.5)


@pytest.mark.parametrize("streamed", [False, True])
async def test_slow_headers_and_stream_timeout(
    dev_settings: Settings, payment: Payment, dns: list[str], streamed: bool
) -> None:
    async def handler(request: web.Request) -> web.StreamResponse:
        response = web.StreamResponse()
        if streamed:
            await response.prepare(request)
            await response.write(b"x")
        await asyncio.sleep(1)
        return response

    async with server(handler) as url, WebhookClient(dev_settings, total_timeout=0.05) as client:
        payment.webhook_url = url
        with pytest.raises(DeliveryError, match="^webhook_timeout$"):
            await client.send(payment)


@pytest.fixture
def certificate(tmp_path: Path, request: pytest.FixtureRequest) -> tuple[ssl.SSLContext, Path]:
    cert, key = tmp_path / "cert.pem", tmp_path / "key.pem"
    hostname = getattr(request, "param", "webhook.test")
    subprocess.run(
        [
            "openssl",
            "req",
            "-x509",
            "-newkey",
            "rsa:2048",
            "-nodes",
            "-days",
            "1",
            "-subj",
            f"/CN={hostname}",
            "-addext",
            f"subjectAltName=DNS:{hostname}",
            "-keyout",
            str(key),
            "-out",
            str(cert),
        ],
        check=True,
        capture_output=True,
    )
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.load_cert_chain(cert, key)
    return context, cert


async def test_bad_tls_is_not_accepted(
    dev_settings: Settings,
    payment: Payment,
    dns: list[str],
    certificate: tuple[ssl.SSLContext, Path],
) -> None:
    hits = 0

    async def handler(request: web.Request) -> web.Response:
        nonlocal hits
        hits += 1
        return web.Response()

    async with server(handler, tls=certificate[0]) as url, WebhookClient(dev_settings) as client:
        payment.webhook_url = url
        with pytest.raises(DeliveryError, match="^webhook_network_error$"):
            await client.send(payment)
    assert hits == 0


async def test_tls_sni_and_certificate_use_original_hostname(
    dev_settings: Settings,
    payment: Payment,
    dns: list[str],
    certificate: tuple[ssl.SSLContext, Path],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    names: list[str | None] = []
    certificate[0].set_servername_callback(lambda sock, name, ctx: names.append(name))
    factory = ssl.create_default_context

    def trusted_context() -> ssl.SSLContext:
        context = factory(cafile=str(certificate[1]))
        assert context.check_hostname and context.verify_mode == ssl.CERT_REQUIRED
        return context

    monkeypatch.setattr("payments.webhooks.ssl.create_default_context", trusted_context)

    async def handler(request: web.Request) -> web.Response:
        assert request.host.startswith("webhook.test:")
        return web.Response()

    async with server(handler, tls=certificate[0]) as url, WebhookClient(dev_settings) as client:
        payment.webhook_url = url
        await client.send(payment)
    assert names == ["webhook.test"]


@pytest.mark.parametrize("certificate", ["wrong.test"], indirect=True)
async def test_trusted_certificate_with_wrong_hostname_is_rejected(
    dev_settings: Settings,
    payment: Payment,
    dns: list[str],
    certificate: tuple[ssl.SSLContext, Path],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    factory = ssl.create_default_context
    monkeypatch.setattr(
        "payments.webhooks.ssl.create_default_context", lambda: factory(cafile=str(certificate[1]))
    )
    hits = 0

    async def handler(request: web.Request) -> web.Response:
        nonlocal hits
        hits += 1
        return web.Response()

    async with server(handler, tls=certificate[0]) as url, WebhookClient(dev_settings) as client:
        payment.webhook_url = url
        with pytest.raises(DeliveryError, match="^webhook_network_error$"):
            await client.send(payment)
    assert hits == 0


async def test_connect_deadline_bounds_dns(
    dev_settings: Settings, payment: Payment, monkeypatch: pytest.MonkeyPatch
) -> None:
    cancelled = asyncio.Event()

    async def lookup(*args: Any, **kwargs: Any) -> list[Any]:
        try:
            await asyncio.sleep(1)
            return []
        finally:
            cancelled.set()

    monkeypatch.setattr(asyncio.get_running_loop(), "getaddrinfo", lookup)
    async with WebhookClient(dev_settings, connect_timeout=0.03) as client:
        payment.webhook_url = "http://webhook.test:8080/"
        with pytest.raises(DeliveryError, match="^webhook_timeout$"):
            await asyncio.wait_for(client.send(payment), 0.5)
    assert cancelled.is_set()


async def test_cancellation_releases_concurrency_slot(
    dev_settings: Settings, payment: Payment, dns: list[str]
) -> None:
    entered = asyncio.Event()
    release = asyncio.Event()
    hits = 0

    async def handler(request: web.Request) -> web.Response:
        nonlocal hits
        hits += 1
        if hits == 1:
            entered.set()
            await release.wait()
        return web.Response()

    async with server(handler) as url, WebhookClient(dev_settings, max_concurrency=1) as client:
        payment.webhook_url = url
        task = asyncio.create_task(client.send(payment))
        try:
            await asyncio.wait_for(entered.wait(), 1)
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
            await asyncio.wait_for(client.send(payment), 1)
        finally:
            release.set()
    assert hits == 2


@pytest.mark.parametrize("size", [0, 65535, 65536])
async def test_response_within_limit_succeeds(
    dev_settings: Settings, payment: Payment, dns: list[str], size: int
) -> None:
    async def handler(request: web.Request) -> web.Response:
        return web.Response(body=b"x" * size)

    async with server(handler) as url, WebhookClient(dev_settings) as client:
        payment.webhook_url = url
        await client.send(payment)


@pytest.mark.parametrize("framing", ["chunked", "close"])
@pytest.mark.parametrize("ending", ["eof", "excess", "stalled"])
async def test_exact_cap_waits_for_bounded_eof_or_one_byte(
    dev_settings: Settings, payment: Payment, dns: list[str], framing: str, ending: str
) -> None:
    async with (
        capped_response_socket(framing, ending) as port,
        WebhookClient(dev_settings, total_timeout=0.25) as client,
    ):
        payment.webhook_url = f"http://webhook.test:{port}/hook"
        if ending == "eof":
            await client.send(payment)
        else:
            code = "webhook_timeout" if ending == "stalled" else "webhook_http_error"
            with pytest.raises(DeliveryError, match=f"^{code}$"):
                # Outer guard is not the transport deadline/error classification.
                await asyncio.wait_for(client.send(payment), 0.7)


async def test_concurrency_is_bounded_and_waiter_can_be_cancelled(
    dev_settings: Settings, payment: Payment, dns: list[str]
) -> None:
    entered = asyncio.Event()
    release = asyncio.Event()
    active = maximum = hits = 0

    async def handler(request: web.Request) -> web.Response:
        nonlocal active, maximum, hits
        active += 1
        hits += 1
        maximum = max(maximum, active)
        entered.set()
        try:
            await release.wait()
            return web.Response()
        finally:
            active -= 1

    async with server(handler) as url, WebhookClient(dev_settings, max_concurrency=1) as client:
        payment.webhook_url = url
        first = asyncio.create_task(client.send(payment))
        await asyncio.wait_for(entered.wait(), 1)
        waiter = asyncio.create_task(client.send(payment))
        try:
            await asyncio.sleep(0.03)
            assert hits == maximum == 1
            waiter.cancel()
            with pytest.raises(asyncio.CancelledError):
                await waiter
            release.set()
            await asyncio.wait_for(first, 1)
            await asyncio.wait_for(client.send(payment), 1)
        finally:
            release.set()
            first.cancel()
            waiter.cancel()
            await asyncio.gather(first, waiter, return_exceptions=True)
    assert hits == 2 and maximum == 1


async def test_connection_failure_is_delivery_error(
    dev_settings: Settings, payment: Payment, dns: list[str]
) -> None:
    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    port = sock.getsockname()[1]  # Bound but not listening: deterministic connection refusal.
    try:
        async with WebhookClient(dev_settings) as client:
            payment.webhook_url = f"http://webhook.test:{port}/?token=SECRET"
            with pytest.raises(DeliveryError, match="^webhook_network_error$") as error:
                await client.send(payment)
            assert error.value.__suppress_context__
    finally:
        sock.close()


async def test_explicit_lifecycle_is_idempotent_and_terminal(dev_settings: Settings) -> None:
    client = WebhookClient(dev_settings)
    assert await client.open() is client
    assert await client.open() is client
    await client.close()
    await client.close()
    with pytest.raises(RuntimeError, match="closed"):
        await client.open()
    unopened = WebhookClient(dev_settings)
    await unopened.close()
    with pytest.raises(RuntimeError, match="closed"):
        await unopened.open()


async def test_receiver_verifies_signature_and_emulates_500(
    dev_settings: Settings, payment: Payment, dns: list[str]
) -> None:
    module = run_path("examples/webhook_receiver.py", run_name="receiver_test")
    app = module["create_app"]("test-webhook-secret", fail_first=1)
    runner = web.AppRunner(app, access_log=None)
    await runner.setup()
    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    try:
        await web.SockSite(runner, sock).start()
        port = sock.getsockname()[1]
        payment.webhook_url = f"http://webhook.test:{port}/webhook"
        async with WebhookClient(dev_settings) as client:
            with pytest.raises(DeliveryError, match="^webhook_http_error$"):
                await client.send(payment)
            await client.send(payment)
            # Successful duplicate is acknowledged; failed delivery was not deduped.
            await client.send(payment)
        async with ClientSession(trust_env=False) as http:
            local = f"http://127.0.0.1:{port}/webhook"
            for headers in [{}, {"X-Webhook-Timestamp": "1", "X-Webhook-Signature": "v1=bad"}]:
                async with http.post(local, data=b"{}", headers=headers) as response:
                    assert response.status == 401
    finally:
        await runner.cleanup()


async def test_receiver_rejects_tampered_raw_bytes_and_stale_timestamp() -> None:
    module = run_path("examples/webhook_receiver.py", run_name="receiver_test")
    app = module["create_app"]("test-secret")
    runner = web.AppRunner(app, access_log=None)
    await runner.setup()
    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    try:
        await web.SockSite(runner, sock).start()
        local = f"http://127.0.0.1:{sock.getsockname()[1]}/webhook"
        async with ClientSession(trust_env=False) as http:
            # Valid HMAC for the original bytes, but body changed after signing.
            for timestamp, original, body in [
                (str(int(time.time())), b"{}", b"{ }"),
                ("1", b"{}", b"{}"),
            ]:
                digest = hmac.new(
                    b"test-secret", timestamp.encode() + b"." + original, hashlib.sha256
                ).hexdigest()
                async with http.post(
                    local,
                    data=body,
                    headers={
                        "X-Webhook-Timestamp": timestamp,
                        "X-Webhook-Signature": "v1=" + digest,
                    },
                ) as response:
                    assert response.status == 401
    finally:
        await runner.cleanup()
