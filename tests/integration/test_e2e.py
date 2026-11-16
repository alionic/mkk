import asyncio
import hashlib
import hmac
import json
import logging
import os
import signal
import socket
import subprocess
import sys
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager
from urllib.parse import urlsplit, urlunsplit
from uuid import UUID

import pytest
from aiohttp import web
from httpx import ASGITransport, AsyncClient
from sqlalchemy import select

from payments.api import create_app
from payments.cli import export_schema
from payments.config import Settings
from payments.db import Database
from payments.messaging import Messaging
from payments.models import Outbox, Payment
from payments.worker import Worker

from .http_helpers import capped_response_socket
from .rabbit_helpers import receive
from .test_processor import fail_commit, read, stop_failing_commit


@asynccontextmanager
async def receiver(handler: Callable[[web.Request], Awaitable[web.Response]]) -> AsyncIterator[str]:
    app = web.Application()
    app.router.add_post("/hook", handler)
    runner = web.AppRunner(app, access_log=None, shutdown_timeout=0.1)
    await runner.setup()
    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    try:
        await web.SockSite(runner, sock).start()
        yield f"http://localhost:{sock.getsockname()[1]}/hook"
    finally:
        await runner.cleanup()


def configuration(database: Database, rabbit_url: str, settings: Settings) -> Settings:
    return settings.model_copy(
        update={
            "database_url": database.engine.url.render_as_string(hide_password=False),
            "rabbitmq_url": rabbit_url,
            "allow_unsafe_webhooks": True,
            "webhook_allowed_domains": ["localhost"],
            "retry_base_seconds": 0.03,
        }
    )


def process_environment(config: Settings) -> dict[str, str]:
    environment = dict(os.environ)
    environment.update(
        {
            "DATABASE_URL": config.database_url,
            "RABBITMQ_URL": config.rabbitmq_url,
            "API_KEY": "test-api-key",
            "WEBHOOK_SECRET": "test-webhook-secret",
            "WEBHOOK_ALLOWED_DOMAINS": '["localhost"]',
            "ENVIRONMENT": "test",
            "ALLOW_UNSAFE_WEBHOOKS": "true",
            "RETRY_BASE_SECONDS": "0.03",
        }
    )
    return environment


@asynccontextmanager
async def runtime(worker: Worker) -> AsyncIterator[None]:
    worker.relay.poll_interval = 0.03
    worker.relay.retry_base = 0.03
    worker.relay.retry_cap = 0.1
    running = asyncio.create_task(worker.run())
    try:
        await asyncio.wait_for(worker.ready.wait(), 5)
        yield
    finally:
        worker.stop.set()
        await asyncio.wait_for(running, 15)


async def create(config: Settings, url: str) -> UUID:
    app = create_app(config)
    async with (
        app.router.lifespan_context(app),
        AsyncClient(
            transport=ASGITransport(app=app),
            base_url="http://test",
            headers={"X-API-Key": "test-api-key", "Idempotency-Key": "e2e-key"},
        ) as client,
    ):
        response = await client.post(
            "/api/v1/payments",
            json={
                "amount": "42.50",
                "currency": "EUR",
                "webhook_url": url,
                "metadata": {"private-metadata": "private-value"},
            },
        )
        assert response.status_code == 202
        return UUID(response.json()["payment_id"])


async def wait_state(database: Database, payment_id: UUID, state: str) -> Payment:
    async with asyncio.timeout(15):
        while True:
            current = await read(database, payment_id)
            if current.webhook_status == state:
                return current
            await asyncio.sleep(0.03)


async def test_actual_success_create_to_terminal_delivered(
    database: Database, rabbit_url: str, settings: Settings
) -> None:
    received: list[tuple[bytes, dict[str, str]]] = []

    async def handler(request: web.Request) -> web.Response:
        body = await request.read()
        payload = json.loads(body)
        current = await read(database, UUID(payload["payment_id"]))
        assert current.status in {"succeeded", "failed"} and current.processed_at is not None
        received.append((body, dict(request.headers)))
        return web.Response(status=204)

    config = configuration(database, rabbit_url, settings)
    async with receiver(handler) as url, runtime(Worker(config, database=database)):
        payment_id = await create(config, url)
        current = await wait_state(database, payment_id, "delivered")
        assert len(received) == 1
        body, headers = received[0]
        assert headers["Idempotency-Key"] == str(current.webhook_event_id)
        signature = hmac.new(
            b"test-webhook-secret",
            headers["X-Webhook-Timestamp"].encode() + b"." + body,
            hashlib.sha256,
        ).hexdigest()
        assert headers["X-Webhook-Signature"] == "v1=" + signature


async def test_exact_cap_chunked_response_delivers_without_retry(
    database: Database, rabbit_url: str, settings: Settings
) -> None:
    config = configuration(database, rabbit_url, settings)
    async with (
        capped_response_socket("chunked", "eof") as port,
        runtime(Worker(config, database=database)),
    ):
        payment_id = await create(config, f"http://localhost:{port}/hook")
        current = await wait_state(database, payment_id, "delivered")
        assert current.expected_attempt == 1
        assert current.last_error is None
        async with database.session_factory() as session:
            events = (await session.scalars(select(Outbox))).all()
            assert [(event.destination, event.attempt) for event in events] == [("payments.new", 1)]


async def test_actual_retry_three_failures_to_dlq(
    database: Database, rabbit_url: str, settings: Settings
) -> None:
    bodies: list[bytes] = []
    failing = True

    async def handler(request: web.Request) -> web.Response:
        bodies.append(await request.read())
        return web.Response(status=503 if failing else 204)

    config = configuration(database, rabbit_url, settings)
    worker = Worker(config, database=database)
    async with receiver(handler) as url, runtime(worker):
        payment_id = await create(config, url)
        current = await wait_state(database, payment_id, "exhausted")
        dead_queue = await worker.messaging.broker.declare_queue(
            worker.messaging.topology.dead_queue
        )
        dead = await receive(dead_queue)
        payload = json.loads(dead.body)
        assert payload["attempt"] == 3
        assert payload["event_id"] == str(current.webhook_event_id)
        assert payload["diagnostics"] == {
            "reason": "webhook_attempts_exhausted",
            "delivery_error": "webhook_http_error",
        }
        assert len(bodies) == 3 and len(set(bodies)) == 1
        assert current.expected_attempt == 3 and current.status != "pending"
        await dead.ack()
        failing = False
        command = await asyncio.create_subprocess_exec(
            sys.executable,
            "-m",
            "payments.cli",
            "replay-webhook",
            str(payment_id),
            env=process_environment(config),
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await asyncio.wait_for(command.communicate(), 10)
        assert command.returncode == 0 and not stdout
        assert json.loads(stderr)["event"] == "replay_created"
        replayed = await wait_state(database, payment_id, "delivered")
        assert replayed.generation != current.generation
        assert replayed.webhook_event_id == current.webhook_event_id
        assert len(bodies) == 4 and len(set(bodies)) == 1


async def test_restart_after_status_commit(
    database: Database, rabbit_url: str, settings: Settings
) -> None:
    entered, release = asyncio.Event(), asyncio.Event()
    bodies: list[bytes] = []

    async def handler(request: web.Request) -> web.Response:
        bodies.append(await request.read())
        if len(bodies) == 1:
            entered.set()
            await release.wait()
        return web.Response(status=204)

    config = configuration(database, rabbit_url, settings)
    first = Worker(config, database=database, drain_timeout=0.05)
    async with receiver(handler) as url:
        async with runtime(first):
            payment_id = await create(config, url)
            await asyncio.wait_for(entered.wait(), 10)
            committed = await read(database, payment_id)
            assert committed.status != "pending" and committed.webhook_status == "pending"
        assert first.sender._closed and not first.active
        release.set()
        async with runtime(Worker(config, database=database)):
            current = await wait_state(database, payment_id, "delivered")
            assert current.status == committed.status
            assert current.processed_at == committed.processed_at
            assert current.webhook_event_id == committed.webhook_event_id
            assert current.expected_attempt == 1
            assert len(bodies) == 2 and bodies[0] == bodies[1]


async def test_confirm_before_commit_duplicate_does_not_resend(
    database: Database, rabbit_url: str, settings: Settings
) -> None:
    bodies: list[bytes] = []

    async def handler(request: web.Request) -> web.Response:
        bodies.append(await request.read())
        return web.Response(status=204)

    config = configuration(database, rabbit_url, settings)
    worker = Worker(config, database=database)
    worker.relay.poll_interval = 0.05
    async with receiver(handler) as url:
        payment_id = await create(config, url)
        await fail_commit(database, "outbox")
        async with runtime(worker):
            await wait_state(database, payment_id, "delivered")
            async with database.session_factory() as session:
                row = await session.scalar(select(Outbox).where(Outbox.payment_id == payment_id))
                assert row is not None and row.published_at is None
            await stop_failing_commit(database, "outbox")
            async with asyncio.timeout(5):
                while True:
                    async with database.session_factory() as session:
                        row = await session.scalar(
                            select(Outbox).where(Outbox.payment_id == payment_id)
                        )
                        assert row is not None
                        if row.published_at is not None:
                            break
                    await asyncio.sleep(0.03)
            await asyncio.sleep(0.15)
            assert len(bodies) == 1


class BrokerProxy:
    """Cut only test-owned TCP sockets; never stop or reconfigure the shared broker."""

    def __init__(self, target: str) -> None:
        self.target = urlsplit(target)
        self.enabled = True
        self.writers: set[asyncio.StreamWriter] = set()
        self.tasks: set[asyncio.Task[None]] = set()
        self.server: asyncio.Server | None = None
        self.port = 0

    async def recover(self) -> None:
        self.enabled = True
        self.server = await asyncio.start_server(self.forward, "127.0.0.1", self.port)
        self.port = self.server.sockets[0].getsockname()[1]

    async def forward(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        task = asyncio.current_task()
        assert task is not None
        self.tasks.add(task)
        self.writers.add(writer)
        upstream: asyncio.StreamWriter | None = None
        try:
            if not self.enabled:
                return
            remote, upstream = await asyncio.open_connection(
                self.target.hostname, self.target.port or 5672
            )
            self.writers.add(upstream)

            async def copy(source: asyncio.StreamReader, destination: asyncio.StreamWriter) -> None:
                while chunk := await source.read(65536):
                    destination.write(chunk)
                    await destination.drain()

            sending = asyncio.create_task(copy(reader, upstream))
            receiving = asyncio.create_task(copy(remote, writer))
            try:
                await asyncio.wait((sending, receiving), return_when=asyncio.FIRST_COMPLETED)
            finally:
                sending.cancel()
                receiving.cancel()
                await asyncio.gather(sending, receiving, return_exceptions=True)
        except (OSError, asyncio.CancelledError):
            pass
        finally:
            for connection in (writer, upstream):
                if connection is not None:
                    connection.close()
                    await connection.wait_closed()
                    self.writers.discard(connection)
            self.tasks.discard(task)

    async def cut(self) -> None:
        self.enabled = False
        # A broker outage refuses reconnects; accepting TCP then immediately
        # EOF-ing instead tests aiormq's pre-AMQP-handshake error path.
        if self.server is not None:
            self.server.close()
        for writer in tuple(self.writers):
            writer.close()
        if self.server is not None:
            await self.server.wait_closed()
            self.server = None


@asynccontextmanager
async def broker_proxy(target: str) -> AsyncIterator[tuple[BrokerProxy, str]]:
    proxy = BrokerProxy(target)
    await proxy.recover()
    netloc = f"{proxy.target.username}:{proxy.target.password}@127.0.0.1:{proxy.port}"
    try:
        yield proxy, urlunsplit(proxy.target._replace(netloc=netloc))
    finally:
        await proxy.cut()
        await asyncio.gather(*proxy.tasks, return_exceptions=True)


async def test_broker_outage_outbox_recovery(
    database: Database, rabbit_url: str, settings: Settings, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.CRITICAL, logger="aio_pika")
    caplog.set_level(logging.CRITICAL, logger="aiormq")
    hits = 0

    async def handler(request: web.Request) -> web.Response:
        nonlocal hits
        await request.read()
        hits += 1
        return web.Response(status=204)

    async with broker_proxy(rabbit_url) as (proxy, url), receiver(handler) as hook:
        config = configuration(database, url, settings)
        worker = Worker(config, database=database, messaging=Messaging(url))
        worker.relay.publish_timeout = 0.4
        async with runtime(worker):
            worker.messaging.timeout_seconds = 0.3
            assert worker.health()["consumer_alive"]
            await proxy.cut()
            async with asyncio.timeout(3):
                while worker.health()["consumer_alive"]:
                    await asyncio.sleep(0.01)
            payment_id = await create(config, hook)
            async with asyncio.timeout(5):
                while True:
                    async with database.session_factory() as session:
                        row = await session.scalar(
                            select(Outbox).where(Outbox.payment_id == payment_id)
                        )
                        assert row is not None and row.published_at is None
                        if row.publish_attempts >= 1:
                            break
                    await asyncio.sleep(0.03)
            current = await read(database, payment_id)
            assert current.status == "pending" and current.expected_attempt == 1
            await proxy.recover()
            await wait_state(database, payment_id, "delivered")
            assert hits == 1
            assert worker.health()["consumer_alive"]


async def test_sigterm_drains_real_http_and_exits_cleanly(
    database: Database, rabbit_url: str, settings: Settings
) -> None:
    entered, release = asyncio.Event(), asyncio.Event()

    async def handler(request: web.Request) -> web.Response:
        await request.read()
        entered.set()
        await release.wait()
        return web.Response(status=204)

    config = configuration(database, rabbit_url, settings)
    async with receiver(handler) as url:
        payment_id = await create(config, url)
        process = await asyncio.create_subprocess_exec(
            sys.executable,
            "-m",
            "payments.worker",
            env=process_environment(config),
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        try:
            await asyncio.wait_for(entered.wait(), 15)
            process.send_signal(signal.SIGTERM)
            await asyncio.sleep(0.1)
            assert process.returncode is None  # Drain, not abrupt transport close.
            release.set()
            stdout, stderr = await asyncio.wait_for(process.communicate(), 15)
            assert process.returncode == 0
            assert not stdout
            logs = [json.loads(line) for line in stderr.splitlines()]
            assert any(item["event"] == "worker_started" for item in logs)
            assert any(item["event"] == "worker_stopped" for item in logs)
            for secret in (
                config.database_url,
                config.rabbitmq_url,
                "test-api-key",
                "test-webhook-secret",
            ):
                assert secret.encode() not in stderr
            assert (await read(database, payment_id)).webhook_status == "delivered"
        finally:
            release.set()
            if process.returncode is None:
                process.terminate()
                await asyncio.wait_for(process.communicate(), 15)


def test_export_schema_offline_without_secrets() -> None:
    environment = {
        key: value
        for key, value in os.environ.items()
        if key
        not in {
            "DATABASE_URL",
            "RABBITMQ_URL",
            "API_KEY",
            "WEBHOOK_SECRET",
            "WEBHOOK_ALLOWED_DOMAINS",
        }
    }
    environment.update({"DATABASE_URL": "private-invalid-url", "API_KEY": "private-api-key"})
    result = subprocess.run(
        [sys.executable, "-m", "payments.cli", "export-schema"],
        env=environment,
        text=True,
        capture_output=True,
        timeout=10,
    )
    assert result.returncode == 0
    assert "private-" not in result.stdout + result.stderr
    schema = json.loads(result.stdout)
    assert "/api/v1/payments" in schema["paths"]
    assert schema["components"]["securitySchemes"]["APIKeyHeader"]["name"] == "X-API-Key"


async def test_export_is_app_openapi_without_connections(
    settings: Settings, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def forbidden(*args: object, **kwargs: object) -> None:
        raise AssertionError("Schema export attempted a connection")

    monkeypatch.setattr(asyncio.get_running_loop(), "create_connection", forbidden)
    app = create_app(settings)
    try:
        assert await export_schema() == app.openapi()
    finally:
        await app.state.database.dispose()


@pytest.mark.parametrize(
    "arguments",
    [
        ["replay-webhook", "private-invalid-uuid"],
        ["private-invalid-command"],
        ["export-schema", "private-unexpected-argument"],
    ],
)
def test_cli_errors_do_not_echo_untrusted_arguments(arguments: list[str]) -> None:
    result = subprocess.run(
        [sys.executable, "-m", "payments.cli", *arguments],
        text=True,
        capture_output=True,
        timeout=10,
    )
    assert result.returncode != 0
    assert "private-" not in result.stdout + result.stderr
