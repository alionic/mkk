import asyncio
import json
from pathlib import Path
from unittest.mock import AsyncMock
from urllib.parse import urlsplit
from uuid import uuid4

import pytest
from faststream.rabbit.message import RabbitMessage as RawRabbitMessage
from httpx import AsyncClient

from payments.config import Settings
from payments.db import Database
from payments.messaging import Messaging
from payments.models import Payment
from payments.outbox import OutboxRelay
from payments.processor import PaymentProcessor
from payments.worker import Worker

from .rabbit_helpers import receive
from .test_processor import GatewaySpy, Sender, fail_commit, read, seed


async def test_external_heartbeat_lifecycle(
    database: Database, rabbit_url: str, settings: Settings, tmp_path: Path
) -> None:
    from payments.cli import main

    path = tmp_path / "health.json"
    # Simulate an old successful heartbeat retained in a container's /tmp.
    path.write_text('{"state":"running","consumer_alive":true,"relay_alive":true}')
    worker = Worker(settings.model_copy(update={"rabbitmq_url": rabbit_url}), database=database)
    worker.heartbeat_path = path
    worker.heartbeat_interval = 0.02
    running = asyncio.create_task(worker.run())
    try:
        await asyncio.wait_for(worker.ready.wait(), 5)
        async with asyncio.timeout(3):
            while main(["worker-health", "--path", str(path)]) != 0:
                await asyncio.sleep(0.01)
        before = json.loads(path.read_text())["timestamp"]
        async with asyncio.timeout(3):
            while json.loads(path.read_text())["timestamp"] == before:
                await asyncio.sleep(0.01)
        assert not list(tmp_path.glob("*.tmp"))
    finally:
        worker.stop.set()
        await asyncio.wait_for(running, 5)
    assert json.loads(path.read_text())["state"] == "stopped"
    assert main(["worker-health", "--path", str(path)]) == 1


async def test_starting_heartbeat_invalidates_old_ready(
    settings: Settings, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / "health.json"
    path.write_text('{"state":"running","consumer_alive":true,"relay_alive":true}')
    worker = Worker(settings)
    worker.heartbeat_path = path
    observed: list[dict[str, object]] = []

    async def unavailable() -> None:
        observed.append(json.loads(path.read_text()))
        raise OSError("private-connect-error")

    monkeypatch.setattr(worker.messaging, "start", unavailable)
    with pytest.raises(RuntimeError, match="worker_failed"):
        await worker.run()
    # Assert outside run(): the supervisor intentionally sanitizes ALL exceptions,
    # so assertions inside unavailable() would be swallowed as worker_failed too.
    assert observed[0]["state"] == "starting"
    assert observed[0]["consumer_alive"] is False
    assert json.loads(path.read_text())["state"] == "fatal"


async def test_explicit_ack_after_commit(database: Database, settings: Settings) -> None:
    event = await seed(database)
    processor = PaymentProcessor(database, GatewaySpy(), Sender())
    worker = Worker(settings, database=database, processor=processor)
    message = AsyncMock()
    message.body = event.model_dump_json().encode()

    async def ack() -> None:
        assert (await read(database, event.payment_id)).webhook_status == "delivered"

    message.ack.side_effect = ack
    await worker.handle(message)
    message.ack.assert_awaited_once()
    message.nack.assert_not_awaited()
    message.reject.assert_not_awaited()


async def test_db_outage_not_acked(database: Database, settings: "Settings") -> None:
    event = await seed(database)
    await fail_commit(database, "payments")
    worker = Worker(
        settings,
        database=database,
        processor=PaymentProcessor(database, GatewaySpy(), Sender()),
        backoff_base=0.02,
        backoff_cap=0.02,
    )
    message = AsyncMock()
    message.body = event.model_dump_json().encode()
    before = asyncio.get_running_loop().time()
    await worker.handle(message)
    assert asyncio.get_running_loop().time() - before >= 0.02
    message.nack.assert_awaited_once_with(requeue=True)
    message.ack.assert_not_awaited()
    current = await read(database, event.payment_id)
    assert current.status == "pending" and current.expected_attempt == 1


@pytest.mark.parametrize(
    "body",
    [
        b'{"broken":',
        b"{}",
        b"[]",
        b"null",
        b"\xff",
        pytest.param(b"x" * 32769, id="oversize"),
        pytest.param(
            b'{"schema_version":1,"event_id":"00000000-0000-0000-0000-000000000001",'
            b'"payment_id":"00000000-0000-0000-0000-000000000002",'
            b'"generation":"00000000-0000-0000-0000-000000000003","attempt":1,"diagnostics":{}}',
            id="extended-dlq-envelope",
        ),
    ],
)
async def test_invalid_message_reject_dlq(
    rabbit_url: str, settings: "Settings", body: bytes
) -> None:
    worker = Worker(settings.model_copy(update={"rabbitmq_url": rabbit_url}))
    publisher = Messaging(rabbit_url)
    try:
        await worker.messaging.start()
        await publisher.start()
        await publisher.broker.publish(
            body,
            exchange=publisher.topology.exchange,
            routing_key="payments.new",
            content_type="application/json",
            persist=True,
        )
        queue = await publisher.broker.declare_queue(publisher.topology.dead_queue)
        dead = await receive(queue)
        assert dead.body == body
        assert dead.headers["x-first-death-reason"] == "rejected"
        await dead.ack()
    finally:
        await worker.messaging.close()
        await worker.sender.close()
        await worker.database.dispose()
        await publisher.close()


async def test_missing_payment_rejected(database: Database, settings: "Settings") -> None:
    event = await seed(database)
    worker = Worker(
        settings, database=database, processor=PaymentProcessor(database, GatewaySpy(), Sender())
    )
    message = AsyncMock()
    message.body = event.model_copy(update={"payment_id": uuid4()}).model_dump_json().encode()
    await worker.handle(message)
    message.reject.assert_awaited_once_with(requeue=False)
    message.ack.assert_not_awaited()


async def test_cancellation_never_acks(database: Database, settings: "Settings") -> None:
    event = await seed(database)
    gateway = GatewaySpy()
    gateway.release.clear()
    worker = Worker(
        settings, database=database, processor=PaymentProcessor(database, gateway, Sender())
    )
    message = AsyncMock()
    message.body = event.model_dump_json().encode()
    task = asyncio.create_task(worker.handle(message))
    await asyncio.wait_for(gateway.entered.wait(), 2)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    message.ack.assert_not_awaited()
    assert (await read(database, event.payment_id)).status == "pending"


@pytest.mark.parametrize("cancel", [False, True])
async def test_shutdown_drains_and_closes(
    database: Database, rabbit_url: str, settings: Settings, cancel: bool
) -> None:
    event = await seed(database)
    entered, release, finished = asyncio.Event(), asyncio.Event(), asyncio.Event()

    async def check(payment: object) -> None:
        entered.set()
        try:
            await release.wait()
        finally:
            finished.set()

    worker = Worker(
        settings.model_copy(update={"rabbitmq_url": rabbit_url}),
        database=database,
        processor=PaymentProcessor(database, GatewaySpy(), Sender(check=check)),
        drain_timeout=0.1,
    )
    stop = asyncio.Event()
    running = asyncio.create_task(worker.run(stop))
    try:
        await asyncio.wait_for(worker.ready.wait(), 5)
        connection = await worker.messaging.broker.connect()
        await asyncio.wait_for(entered.wait(), 5)
        if cancel:
            running.cancel()
            with pytest.raises(asyncio.CancelledError):
                await asyncio.wait_for(running, 5)
        else:
            stop.set()
            release.set()
            await asyncio.wait_for(running, 5)
        assert finished.is_set()
        closed = connection.is_closed
        assert closed
        assert worker.sender._closed
        assert not worker.health()["consumer_alive"]
        assert not worker.active
        current = await read(database, event.payment_id)
        assert current.webhook_status == ("pending" if cancel else "delivered")
    finally:
        release.set()
        stop.set()
        if not running.done():
            await asyncio.wait_for(running, 5)


async def test_fatal_relay_stops_consumer(
    database: Database, rabbit_url: str, settings: Settings, monkeypatch: pytest.MonkeyPatch
) -> None:
    worker = Worker(settings.model_copy(update={"rabbitmq_url": rabbit_url}), database=database)
    fail = asyncio.Event()

    async def broken(stop: asyncio.Event, *, manage_lifecycle: bool = True) -> None:
        assert not manage_lifecycle
        await fail.wait()
        raise RuntimeError("private-relay-failure")

    monkeypatch.setattr(worker.relay, "run", broken)
    running = asyncio.create_task(worker.run())
    await asyncio.wait_for(worker.ready.wait(), 5)
    assert worker.health()["consumer_alive"]
    fail.set()
    with pytest.raises(RuntimeError, match="worker_failed"):
        await asyncio.wait_for(running, 5)
    assert worker.health()["state"] == "fatal"
    assert not worker.health()["consumer_alive"]
    assert worker.messaging.broker._connection is None
    assert worker.sender._closed


async def test_backoff_interrupted_by_stop(database: Database, settings: Settings) -> None:
    event = await seed(database)
    await fail_commit(database, "payments")
    worker = Worker(
        settings,
        database=database,
        processor=PaymentProcessor(database, GatewaySpy(), Sender()),
        backoff_base=30,
        backoff_cap=30,
    )
    message = AsyncMock()
    message.body = event.model_dump_json().encode()
    task = asyncio.create_task(worker.handle(message))
    async with asyncio.timeout(3):
        while worker.infrastructure_failures == 0:
            await asyncio.sleep(0.01)
    worker.stop.set()
    await asyncio.wait_for(task, 0.2)
    message.ack.assert_not_awaited()
    message.nack.assert_not_awaited()


async def test_relay_external_lifecycle(settings: Settings) -> None:
    database = Database(settings.database_url)
    publisher = AsyncMock()
    stop = asyncio.Event()
    stop.set()
    await OutboxRelay(database, publisher).run(stop, manage_lifecycle=False)
    publisher.start.assert_not_awaited()
    publisher.close.assert_not_awaited()
    await database.dispose()


@pytest.mark.parametrize("failed_commit", [False, True])
async def test_real_broker_settlement_after_database_commit(
    database: Database,
    rabbit_url: str,
    settings: Settings,
    monkeypatch: pytest.MonkeyPatch,
    failed_commit: bool,
) -> None:
    event = await seed(database)
    if failed_commit:
        await fail_commit(database, "payments", "NEW.webhook_status = 'delivered'")
    worker = Worker(
        settings.model_copy(update={"rabbitmq_url": rabbit_url}),
        database=database,
        processor=PaymentProcessor(database, GatewaySpy(), Sender()),
        backoff_base=0.1,
        backoff_cap=0.1,
    )
    settled = asyncio.Event()
    acknowledgements: list[str] = []
    original_ack, original_nack = RawRabbitMessage.ack, RawRabbitMessage.nack

    async def ack(message: RawRabbitMessage, multiple: bool = False) -> None:
        current = await read(database, event.payment_id)
        acknowledgements.append(current.webhook_status)
        assert current.webhook_status == "delivered"
        await original_ack(message, multiple=multiple)
        settled.set()

    async def nack(message: RawRabbitMessage, multiple: bool = False, requeue: bool = True) -> None:
        current = await read(database, event.payment_id)
        assert current.webhook_status == "pending" and current.expected_attempt == 1
        worker.stop.set()
        await original_nack(message, multiple=multiple, requeue=requeue)
        settled.set()

    monkeypatch.setattr(RawRabbitMessage, "ack", ack)
    monkeypatch.setattr(RawRabbitMessage, "nack", nack)
    running = asyncio.create_task(worker.run())
    try:
        await asyncio.wait_for(settled.wait(), 10)
        assert acknowledgements == ([] if failed_commit else ["delivered"])
    finally:
        worker.stop.set()
        await asyncio.wait_for(running, 10)


async def test_cleanup_failure_marks_health_fatal(
    database: Database, rabbit_url: str, settings: Settings, monkeypatch: pytest.MonkeyPatch
) -> None:
    worker = Worker(settings.model_copy(update={"rabbitmq_url": rabbit_url}), database=database)
    original_close = worker.messaging.close

    async def close() -> None:
        await original_close()
        raise RuntimeError("private-cleanup-failure")

    monkeypatch.setattr(worker.messaging, "close", close)
    running = asyncio.create_task(worker.run())
    await asyncio.wait_for(worker.ready.wait(), 5)
    worker.stop.set()
    with pytest.raises(RuntimeError, match="worker_failed"):
        await running
    assert worker.health()["state"] == "fatal"
    assert worker.sender._closed


async def test_shutdown_cancels_broker_consumer_before_drain(
    database: Database, rabbit_url: str, rabbit_admin: AsyncClient, settings: Settings
) -> None:
    await seed(database)
    entered, release = asyncio.Event(), asyncio.Event()

    async def sending(payment: Payment) -> None:
        entered.set()
        await release.wait()

    worker = Worker(
        settings.model_copy(update={"rabbitmq_url": rabbit_url}),
        database=database,
        processor=PaymentProcessor(database, GatewaySpy(), Sender(check=sending)),
    )
    running = asyncio.create_task(worker.run())
    try:
        await asyncio.wait_for(entered.wait(), 5)
        vhost = urlsplit(rabbit_url).path[1:]
        async with asyncio.timeout(8):
            while not (await rabbit_admin.get(f"/api/consumers/{vhost}")).json():
                await asyncio.sleep(0.05)
        worker.stop.set()
        async with asyncio.timeout(8):
            while True:
                response = await rabbit_admin.get(f"/api/consumers/{vhost}")
                response.raise_for_status()
                if not response.json():
                    break
                await asyncio.sleep(0.05)
        assert not running.done()  # HTTP still in flight, transport remains open.
        assert worker.messaging.broker._connection is not None
    finally:
        release.set()
        worker.stop.set()
        await asyncio.wait_for(running, 10)


async def test_unexpected_processor_failure_is_fatal_not_acknowledged(
    database: Database, settings: Settings
) -> None:
    event = await seed(database)
    worker = Worker(
        settings,
        database=database,
        processor=PaymentProcessor(database, GatewaySpy(), Sender(RuntimeError("private-error"))),
    )
    message = AsyncMock()
    message.body = event.model_dump_json().encode()
    await worker.handle(message)
    assert worker.health()["state"] == "fatal" and worker.stop.is_set()
    message.ack.assert_not_awaited()
    message.nack.assert_not_awaited()
    message.reject.assert_not_awaited()
    assert (await read(database, event.payment_id)).webhook_status == "pending"


async def test_repeated_cancellation_does_not_interrupt_owned_cleanup(
    database: Database, rabbit_url: str, settings: Settings, monkeypatch: pytest.MonkeyPatch
) -> None:
    worker = Worker(settings.model_copy(update={"rabbitmq_url": rabbit_url}), database=database)
    entered, release = asyncio.Event(), asyncio.Event()
    original_close = worker.messaging.close

    async def close() -> None:
        entered.set()
        await release.wait()
        await original_close()

    monkeypatch.setattr(worker.messaging, "close", close)
    running = asyncio.create_task(worker.run())
    try:
        await asyncio.wait_for(worker.ready.wait(), 5)
        running.cancel()
        await asyncio.wait_for(entered.wait(), 3)
        running.cancel()
        await asyncio.sleep(0)
        running.cancel()
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(running, 5)
        assert worker.messaging.broker._connection is None
        assert worker.sender._closed
        assert not worker.active
    finally:
        release.set()
        worker.stop.set()
        await asyncio.gather(running, return_exceptions=True)


async def test_real_database_connection_outage_does_not_consume_attempt(
    database: Database, settings: Settings
) -> None:
    event = await seed(database)
    unavailable = Database(settings.database_url)  # Explicitly unused loopback endpoint.
    worker = Worker(settings, database=unavailable, backoff_base=0.01, backoff_cap=0.01)
    message = AsyncMock()
    message.body = event.model_dump_json().encode()
    try:
        await worker.handle(message)
        message.nack.assert_awaited_once_with(requeue=True)
        message.ack.assert_not_awaited()
        current = await read(database, event.payment_id)
        assert current.status == "pending" and current.expected_attempt == 1
        assert not worker.fatal
    finally:
        await unavailable.dispose()


@pytest.mark.parametrize(
    "phase", ["relay", "handler", "stop_consuming", "messaging_close", "sender_close", "dispose"]
)
async def test_shutdown_phase_deadlines_attempt_later_cleanup_and_join_tasks(
    database: Database, settings: Settings, monkeypatch: pytest.MonkeyPatch, phase: str
) -> None:
    """A cooperative stuck phase must not starve later resource cleanup."""
    release = asyncio.Event()
    relay_entered, relay_finished = asyncio.Event(), asyncio.Event()
    send_entered, send_finished = asyncio.Event(), asyncio.Event()
    phases: list[str] = []

    async def send(payment: Payment) -> None:
        send_entered.set()
        try:
            await asyncio.Event().wait()
        finally:
            try:
                if phase == "handler":
                    # First cancellation starts sender cleanup; its next async
                    # wait remains cooperative with the cancellation deadline.
                    await release.wait()
            finally:
                send_finished.set()

    worker = Worker(
        settings,
        database=database,
        processor=PaymentProcessor(database, GatewaySpy(), Sender(check=send)),
        drain_timeout=0.01,
        cleanup_timeout=0.03,
    )
    worker.messaging.timeout_seconds = 0.01
    original_sender_close = worker.sender.close
    original_dispose = database.dispose

    async def relay(stop: asyncio.Event, *, manage_lifecycle: bool = True) -> None:
        assert not manage_lifecycle
        relay_entered.set()
        try:
            await asyncio.Event().wait()
        finally:
            try:
                if phase == "relay":
                    await release.wait()
            finally:
                relay_finished.set()

    async def stop_consuming() -> None:
        phases.append("stop_consuming")
        if phase == "stop_consuming":
            await release.wait()

    async def messaging_close() -> None:
        phases.append("messaging_close")
        assert send_finished.is_set()  # Never abandon an active send task.
        if phase == "messaging_close":
            await release.wait()

    async def sender_close() -> None:
        phases.append("sender_close")
        assert send_finished.is_set()
        try:
            if phase == "sender_close":
                await release.wait()
        finally:
            await original_sender_close()

    async def dispose() -> None:
        phases.append("dispose")
        try:
            if phase == "dispose":
                await release.wait()
        finally:
            await original_dispose()

    monkeypatch.setattr(worker.messaging, "start", AsyncMock())
    monkeypatch.setattr(worker.messaging, "stop_consuming", stop_consuming)
    monkeypatch.setattr(worker.messaging, "close", messaging_close)
    monkeypatch.setattr(worker.relay, "run", relay)
    monkeypatch.setattr(worker.sender, "close", sender_close)
    monkeypatch.setattr(database, "dispose", dispose)
    event = await seed(database)
    message = AsyncMock()
    message.body = event.model_dump_json().encode()
    running = asyncio.create_task(worker.run())
    handling: asyncio.Task[None] | None = None
    try:
        await asyncio.wait_for(worker.ready.wait(), 2)
        await asyncio.wait_for(relay_entered.wait(), 2)
        handling = asyncio.create_task(worker.handle(message))
        await asyncio.wait_for(send_entered.wait(), 2)
        worker.stop.set()
        completed, _ = await asyncio.wait((running,), timeout=0.5)
        assert "dispose" in phases, f"{phase} blocked the later cleanup phases"
        assert running in completed, f"{phase} blocked worker shutdown"
        with pytest.raises(RuntimeError, match="worker_failed"):
            await running
        assert phases == ["stop_consuming", "messaging_close", "sender_close", "dispose"]
        assert not release.is_set()
        assert relay_finished.is_set() and send_finished.is_set()
        assert worker._relay_task is not None and worker._relay_task.done()
        assert handling.done() and not worker.active
        message.ack.assert_not_awaited()
        assert worker.health()["state"] == "fatal"
    finally:
        # RED safety: release the deliberately blocked phase, then JOIN every
        # owned task rather than cancelling the shielded supervisor and leaking.
        worker.stop.set()
        release.set()
        await asyncio.gather(running, return_exceptions=True)
        if handling is not None:
            if not handling.done():
                handling.cancel()
            await asyncio.gather(handling, return_exceptions=True)
        monkeypatch.setattr(database, "dispose", original_dispose)
        await original_dispose()
