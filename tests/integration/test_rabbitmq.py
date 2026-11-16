import asyncio
import json
from urllib.parse import urlsplit
from uuid import uuid4

import pytest
from aio_pika import DeliveryMode
from aio_pika.abc import AbstractIncomingMessage
from faststream import AckPolicy
from faststream.rabbit import RabbitMessage
from httpx import AsyncClient

from payments.messaging import Messaging, PublishError

from .rabbit_helpers import receive


async def get_message(messaging: Messaging, *, dead: bool = False) -> AbstractIncomingMessage:
    queue = await messaging.broker.declare_queue(
        messaging.topology.dead_queue if dead else messaging.topology.queue
    )
    return await receive(queue)


@pytest.mark.parametrize("cancel", [False, True])
async def test_busy_subscriber_shutdown_closes_real_connection(
    rabbit_url: str, cancel: bool
) -> None:
    publisher = Messaging(rabbit_url)
    entered = asyncio.Event()
    release = asyncio.Event()
    finished = asyncio.Event()

    async def handle(body: dict[str, object]) -> None:
        entered.set()
        try:
            await release.wait()
        finally:
            finished.set()

    publisher.broker.subscriber(
        publisher.topology.queue, publisher.topology.exchange, ack_policy=AckPolicy.MANUAL
    )(handle)
    await publisher.start()
    connection = await publisher.broker.connect()
    queue = await publisher.broker.declare_queue(publisher.topology.queue)
    try:
        await publisher.publish_event({"schema_version": 1}, "payments.new", uuid4())
        await asyncio.wait_for(entered.wait(), 3)
        closing = asyncio.create_task(publisher.close())
        if cancel:
            await asyncio.sleep(0.05)
            closing.cancel()
            with pytest.raises(asyncio.CancelledError):
                await asyncio.wait_for(closing, 12)
        else:
            await asyncio.wait_for(closing, 12)
        # Keep pytest assertion reprs from including even a password-masked AMQP URL.
        connection_closed = connection.is_closed
        channel_closed = queue.channel.is_closed
        connection_released = publisher.broker._connection is None
        assert connection_closed
        assert channel_closed
        assert connection_released
    finally:
        release.set()
        await asyncio.wait_for(finished.wait(), 3)
        # Test owns this connection; explicit fallback also prevents a RED leak.
        await connection.close()
        await publisher.close()


async def test_topology_quorum_and_single_active(
    messaging: Messaging, rabbit_url: str, rabbit_admin: AsyncClient
) -> None:
    vhost = urlsplit(rabbit_url).path[1:]
    response = await rabbit_admin.get(f"/api/queues/{vhost}/payments.new")
    response.raise_for_status()
    queue = response.json()
    assert queue["durable"] is True
    assert queue["type"] == "quorum"
    assert queue["arguments"] == {
        "x-queue-type": "quorum",
        "x-single-active-consumer": True,
        "x-dead-letter-exchange": "payments.dlx",
        "x-dead-letter-routing-key": "payments.dlq",
        "x-dead-letter-strategy": "at-least-once",
        "x-overflow": "reject-publish",
        "x-delivery-limit": 3,
    }
    dead = (await rabbit_admin.get(f"/api/queues/{vhost}/payments.dlq")).json()
    assert dead["type"] == "quorum" and dead["durable"] is True
    for name in ("payments", "payments.dlx"):
        exchange = (await rabbit_admin.get(f"/api/exchanges/{vhost}/{name}")).json()
        assert exchange["type"] == "direct" and exchange["durable"] is True


async def test_persistent_confirmed_message(messaging: Messaging) -> None:
    message_id, event_id, generation, payment_id = uuid4(), uuid4(), uuid4(), uuid4()
    event: dict[str, object] = {
        "schema_version": 1,
        "event_id": str(event_id),
        "generation": str(generation),
        "payment_id": str(payment_id),
        "attempt": 2,
    }
    await messaging.publish_event(event, "payments.new", message_id)
    message = await get_message(messaging)
    assert json.loads(message.body) == event
    assert message.delivery_mode == DeliveryMode.PERSISTENT
    assert message.message_id == str(message_id)
    assert message.exchange == "payments"
    assert message.routing_key == "payments.new"
    await message.ack()


async def test_dlq_destination_uses_dead_exchange(messaging: Messaging) -> None:
    message_id = uuid4()
    event: dict[str, object] = {
        "schema_version": 1,
        "event_id": str(uuid4()),
        "payment_id": str(uuid4()),
        "generation": str(uuid4()),
        "attempt": 3,
        "diagnostics": {
            "reason": "webhook_attempts_exhausted",
            "delivery_error": "webhook_timeout",
        },
    }
    await messaging.publish_event(event, "payments.dlq", message_id)
    message = await get_message(messaging, dead=True)
    assert json.loads(message.body) == event
    assert message.delivery_mode == DeliveryMode.PERSISTENT
    assert message.message_id == str(message_id)
    assert message.exchange == "payments.dlx"
    assert message.routing_key == "payments.dlq"
    await message.ack()


@pytest.mark.parametrize("destination", ["not.bound", "payments.dlq.unknown"])
async def test_unroutable_is_failure(messaging: Messaging, destination: str) -> None:
    connection = await messaging.broker.connect()
    async with connection.channel() as channel:
        # A default-exchange fallback would incorrectly succeed by queue name.
        await channel.declare_queue(destination, exclusive=True, auto_delete=True, timeout=3)
        with pytest.raises(PublishError, match="publish_unroutable"):
            await messaging.publish_event({"schema_version": 1}, destination, uuid4())


async def test_quorum_overflow_rejects_publish(
    messaging: Messaging, rabbit_url: str, rabbit_admin: AsyncClient
) -> None:
    vhost = urlsplit(rabbit_url).path[1:]
    response = await rabbit_admin.put(
        f"/api/policies/{vhost}/test-capacity",
        json={
            "pattern": "^payments[.]new$",
            "apply-to": "queues",
            "priority": 1,
            "definition": {"max-length": 1},
        },
    )
    response.raise_for_status()
    # Quorum length enforcement is asynchronous and may overshoot briefly.
    rejected = False
    for _ in range(20):
        try:
            await messaging.publish_event({"schema_version": 1}, "payments.new", uuid4())
        except PublishError as error:
            assert str(error) == "publish_failed"
            rejected = True
            break
        await asyncio.sleep(0.02)
    assert rejected, "quorum overflow must nack, not silently drop confirmed messages"


async def test_reject_dead_letters(messaging: Messaging) -> None:
    message_id = uuid4()
    await messaging.publish_event({"schema_version": 1}, "payments.new", message_id)
    message = await get_message(messaging)
    await message.reject(requeue=False)
    dead = await get_message(messaging, dead=True)
    assert dead.message_id == str(message_id)
    deaths = dead.headers["x-death"]
    assert isinstance(deaths, list)
    death = deaths[0]
    assert isinstance(death, dict)
    assert death["reason"] == "rejected"
    assert death["queue"] == "payments.new"
    await dead.ack()


async def test_delivery_limit_three_dead_letters(messaging: Messaging) -> None:
    await messaging.publish_event({"schema_version": 1}, "payments.new", uuid4())
    counts = []
    for _ in range(4):
        message = await get_message(messaging)
        counts.append(message.headers.get("x-delivery-count", 0))
        await message.reject(requeue=True)
    assert counts == [0, 1, 2, 3]
    dead = await get_message(messaging, dead=True)
    deaths = dead.headers["x-death"]
    assert isinstance(deaths, list)
    death = deaths[0]
    assert isinstance(death, dict)
    assert death["reason"] == "delivery_limit"
    await dead.ack()


async def test_crashed_consumer_delivery_limit_three_dead_letters(messaging: Messaging) -> None:
    await messaging.publish_event({"schema_version": 1}, "payments.new", uuid4())
    connection = await messaging.broker.connect()
    counts = []
    for _ in range(4):
        async with connection.channel() as channel:
            await channel.set_qos(prefetch_count=1, timeout=3)
            queue = await channel.declare_queue("payments.new", passive=True, timeout=3)
            message = await receive(queue)
            counts.append(message.headers.get("x-delivery-count", 0))
            # Closing a channel without ack simulates worker connection loss.
    assert counts == [0, 1, 2, 3]
    dead = await get_message(messaging, dead=True)
    deaths = dead.headers["x-death"]
    assert isinstance(deaths, list)
    death = deaths[0]
    assert isinstance(death, dict) and death["reason"] == "delivery_limit"
    await dead.ack()


async def test_dlx_outage_retains_rejected_message(messaging: Messaging) -> None:
    dead_queue = await messaging.broker.declare_queue(messaging.topology.dead_queue)
    # Delete/redeclare only our fixture vhost's queue, not broker infrastructure.
    await dead_queue.delete(if_unused=False, if_empty=False, timeout=3)
    message_id = uuid4()
    await messaging.publish_event({"schema_version": 1}, "payments.new", message_id)
    message = await get_message(messaging)
    await message.reject(requeue=False)
    await asyncio.sleep(0.2)
    connection = await messaging.broker.connect()
    async with connection.channel() as channel:
        restored = await channel.declare_queue(
            "payments.dlq", durable=True, arguments={"x-queue-type": "quorum"}, timeout=3
        )
        await restored.bind("payments.dlx", "payments.dlq", timeout=3)
        # RabbitMQ 4.2.1 retries unroutable dead letters every 180 seconds by
        # default. Wait for that real timer; do not alter broker-wide settings.
        dead = await receive(restored, timeout=200)
        assert dead.message_id == str(message_id)
        await dead.ack()


async def test_faststream_subscriber_single_active_prefetch_one(
    rabbit_url: str, rabbit_admin: AsyncClient
) -> None:
    first, second = Messaging(rabbit_url), Messaging(rabbit_url)
    entered = asyncio.Event()
    release = asyncio.Event()
    received: list[str] = []

    async def handle(body: dict[str, object], message: RabbitMessage) -> None:
        received.append(str(message.message_id))
        entered.set()
        await release.wait()

    first.broker.subscriber(first.topology.queue, first.topology.exchange)(handle)
    second.broker.subscriber(second.topology.queue, second.topology.exchange)(handle)
    try:
        await first.start()
        await second.start()
        await first.publish_event({"schema_version": 1}, "payments.new", uuid4())
        await first.publish_event({"schema_version": 1}, "payments.new", uuid4())
        await asyncio.wait_for(entered.wait(), 3)
        await asyncio.sleep(0.2)
        assert len(received) == 1  # SAC + prefetch=1, even across two worker connections.
        vhost = urlsplit(rabbit_url).path[1:]
        # Management consumer stats are sampled (default interval: five seconds).
        async with asyncio.timeout(12):
            while True:
                response = await rabbit_admin.get(f"/api/consumers/{vhost}")
                response.raise_for_status()
                consumers = [c for c in response.json() if c["queue"]["name"] == "payments.new"]
                if len(consumers) == 2:
                    break
                await asyncio.sleep(0.1)
        assert {c["prefetch_count"] for c in consumers} == {1}
        assert sorted(c["active"] for c in consumers) == [False, True]
        release.set()
        async with asyncio.timeout(3):
            while len(received) < 2:
                await asyncio.sleep(0.01)
    finally:
        release.set()
        await first.close()
        await second.close()
