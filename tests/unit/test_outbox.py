import asyncio
from unittest.mock import AsyncMock, Mock

import pytest
from faststream._internal.endpoint.subscriber.utils import MultiLock
from sqlalchemy.exc import OperationalError

from payments.db import Database
from payments.messaging import Messaging
from payments.outbox import OutboxRelay, retry_delay


@pytest.mark.parametrize(
    ("attempt", "expected"), [(1, 1.0), (2, 2.0), (3, 4.0), (7, 60.0), (10**9, 60.0)]
)
def test_bounded_exponential_delay(attempt: int, expected: float) -> None:
    assert retry_delay(attempt, base_seconds=1, max_seconds=60) == expected


@pytest.mark.parametrize("option", ["batch_size", "poll_interval", "publish_timeout"])
def test_invalid_relay_options(option: str) -> None:
    database = Database("postgresql+asyncpg://unused:unused@127.0.0.1:1/unused")
    with pytest.raises(ValueError):
        if option == "batch_size":
            OutboxRelay(database, AsyncMock(), batch_size=0)
        elif option == "poll_interval":
            OutboxRelay(database, AsyncMock(), poll_interval=0)
        else:
            OutboxRelay(database, AsyncMock(), publish_timeout=float("inf"))


async def test_run_idle_pause_is_interruptible_and_closes(monkeypatch: pytest.MonkeyPatch) -> None:
    database = Database("postgresql+asyncpg://unused:unused@127.0.0.1:1/unused")
    publisher = AsyncMock()
    relay = OutboxRelay(database, publisher, poll_interval=3600)
    entered = asyncio.Event()

    async def empty() -> int:
        entered.set()
        return 0

    monkeypatch.setattr(relay, "run_once", empty)
    stop = asyncio.Event()
    task = asyncio.create_task(relay.run(stop))
    await asyncio.wait_for(entered.wait(), 1)
    stop.set()
    await asyncio.wait_for(task, 1)
    publisher.close.assert_awaited_once()


async def test_run_cancellation_closes_publisher(monkeypatch: pytest.MonkeyPatch) -> None:
    database = Database("postgresql+asyncpg://unused:unused@127.0.0.1:1/unused")
    publisher = AsyncMock()
    relay = OutboxRelay(database, publisher)
    entered = asyncio.Event()

    async def blocked() -> int:
        entered.set()
        await asyncio.Event().wait()
        return 0

    monkeypatch.setattr(relay, "run_once", blocked)
    task = asyncio.create_task(relay.run(asyncio.Event()))
    await asyncio.wait_for(entered.wait(), 1)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    publisher.close.assert_awaited_once()


async def test_run_database_failure_wait_is_interruptible(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    database = Database("postgresql+asyncpg://unused:unused@127.0.0.1:1/unused")
    publisher = AsyncMock()
    relay = OutboxRelay(database, publisher, poll_interval=3600)
    entered = asyncio.Event()

    async def failure() -> int:
        entered.set()
        raise OperationalError("SECRET_STATEMENT", {}, OSError("SECRET_DSN"))

    monkeypatch.setattr(relay, "run_once", failure)
    stop = asyncio.Event()
    task = asyncio.create_task(relay.run(stop))
    await asyncio.wait_for(entered.wait(), 1)
    stop.set()
    await asyncio.wait_for(task, 1)
    assert "outbox_iteration_failed" in caplog.text
    assert "SECRET" not in caplog.text
    publisher.close.assert_awaited_once()


async def test_start_failure_still_closes_publisher() -> None:
    database = Database("postgresql+asyncpg://unused:unused@127.0.0.1:1/unused")
    publisher = AsyncMock()
    publisher.start.side_effect = TimeoutError
    with pytest.raises(TimeoutError):
        await OutboxRelay(database, publisher).run(asyncio.Event())
    publisher.close.assert_awaited_once()


@pytest.mark.parametrize("cancel", [False, True])
async def test_busy_subscriber_shutdown_closes_transport(
    monkeypatch: pytest.MonkeyPatch, cancel: bool
) -> None:
    publisher = Messaging("amqp://unused:unused@127.0.0.1:1/", timeout_seconds=0.05)
    subscriber = publisher.broker.subscriber("busy")
    lock = MultiLock()
    lock.acquire()  # Actual FastStream in-flight handler drain, never released by shutdown.
    subscriber.lock = lock
    channel = Mock(is_closed=False, close=AsyncMock())
    connection = Mock(close=AsyncMock())
    monkeypatch.setattr(publisher.broker, "_channel", channel)
    monkeypatch.setattr(publisher.broker, "_connection", connection)
    closing = asyncio.create_task(publisher.close())
    try:
        if cancel:
            # stop() has entered its real MultiLock graceful-drain wait.
            await asyncio.sleep(0.01)
            closing.cancel()
            with pytest.raises(asyncio.CancelledError):
                await closing
        else:
            await asyncio.wait_for(closing, 1)
        channel.close.assert_awaited_once()
        connection.close.assert_awaited_once()
        assert publisher.broker._connection is None
        assert publisher.broker._channel is None
    finally:
        lock.release()


async def test_subscriber_stop_timeout_still_closes_transport(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    publisher = Messaging("amqp://unused:unused@127.0.0.1:1/", timeout_seconds=0.05)
    subscriber = publisher.broker.subscriber("busy")
    monkeypatch.setattr(subscriber, "stop", AsyncMock(side_effect=TimeoutError))
    channel = Mock(is_closed=False, close=AsyncMock())
    connection = Mock(close=AsyncMock())
    monkeypatch.setattr(publisher.broker, "_channel", channel)
    monkeypatch.setattr(publisher.broker, "_connection", connection)
    with pytest.raises(TimeoutError):
        await publisher.close()
    channel.close.assert_awaited_once()
    connection.close.assert_awaited_once()
    assert publisher.broker._connection is None
