import asyncio
import json
from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock
from uuid import UUID, uuid4

import pytest
from aio_pika import DeliveryMode
from sqlalchemy import select, text
from sqlalchemy.exc import DBAPIError

from payments.db import Database
from payments.gateway import Gateway
from payments.messaging import Messaging
from payments.models import Outbox, Payment
from payments.outbox import OutboxRelay
from payments.processor import DeliveryError, PaymentProcessor
from payments.schemas import PaymentCreate, PaymentEvent
from payments.service import create_payment

from .rabbit_helpers import receive


class Publisher:
    """Failure injection only; real DB and separate real-broker tests remain mandatory."""

    def __init__(self, error: Exception | None = None, *, block: bool = False) -> None:
        self.error = error
        self.entered = asyncio.Event()
        self.release = asyncio.Event()
        if not block:
            self.release.set()
        self.calls: list[tuple[dict[str, object], str, UUID]] = []

    async def start(self) -> None:
        pass

    async def close(self) -> None:
        pass

    async def publish_event(
        self, event: dict[str, object], destination: str, message_id: UUID
    ) -> None:
        self.calls.append((event, destination, message_id))
        self.entered.set()
        await self.release.wait()
        if self.error:
            raise self.error


async def seed(database: Database) -> Outbox:
    async with database.session_factory.begin() as session:
        await create_payment(
            session,
            PaymentCreate.model_validate(
                {"amount": "10.00", "currency": "USD", "webhook_url": "https://example.com/hook"}
            ),
            str(uuid4()),
        )
        row = (await session.scalars(select(Outbox).order_by(Outbox.created_at.desc()))).first()
        assert row is not None
        return row


async def read(database: Database, row_id: UUID) -> Outbox:
    async with database.session_factory() as session:
        row = await session.get(Outbox, row_id)
        assert row is not None
        return row


async def test_confirm_before_published_at(database: Database) -> None:
    row = await seed(database)
    publisher = Publisher(block=True)
    task = asyncio.create_task(OutboxRelay(database, publisher).run_once())
    try:
        await asyncio.wait_for(publisher.entered.wait(), 2)
        assert (await read(database, row.id)).published_at is None
    finally:
        publisher.release.set()
    assert await task == 1
    assert (await read(database, row.id)).published_at is not None
    event, destination, message_id = publisher.calls[0]
    assert event == row.payload
    assert destination == "payments.new"
    assert message_id == row.id
    async with database.session_factory() as session:
        payment = await session.get(Payment, row.payment_id)
        assert payment is not None
        assert event["event_id"] == str(payment.webhook_event_id)
        assert event["generation"] == str(payment.generation)
        assert event["attempt"] == payment.expected_attempt == 1
        assert payment.status == "pending"  # No payment business in relay.


async def test_publish_failure_reschedules(database: Database) -> None:
    row = await seed(database)
    publisher = Publisher(ConnectionError("secret URL must not be stored"))
    relay = OutboxRelay(database, publisher, retry_base=2, retry_cap=4)
    before = datetime.now(UTC)
    assert await relay.run_once() == 0
    failed = await read(database, row.id)
    assert failed.published_at is None
    assert failed.publish_attempts == 1
    assert failed.last_error == "publish_failed"
    assert before + timedelta(seconds=2) <= failed.available_at
    assert await relay.run_once() == 0
    assert len(publisher.calls) == 1  # No fast retry loop during broker outage.
    # Very old failure is still retried; there is no max-attempts discard policy.
    async with database.session_factory.begin() as session:
        current = await session.get(Outbox, row.id)
        assert current is not None
        current.publish_attempts = 1_000_000
        current.available_at = before - timedelta(seconds=1)
    assert await relay.run_once() == 0
    failed = await read(database, row.id)
    assert failed.publish_attempts == 1_000_001
    assert datetime.now(UTC) + timedelta(seconds=3) < failed.available_at
    publisher.error = None
    async with database.session_factory.begin() as session:
        current = await session.get(Outbox, row.id)
        assert current is not None
        current.available_at = before
    assert await relay.run_once() == 1
    assert (await read(database, row.id)).last_error is None


async def test_publish_timeout_is_durable(database: Database) -> None:
    row = await seed(database)
    publisher = Publisher(block=True)
    relay = OutboxRelay(database, publisher, publish_timeout=0.05)
    assert await asyncio.wait_for(relay.run_once(), 2) == 0
    failed = await read(database, row.id)
    assert failed.last_error == "publish_timeout"
    assert failed.publish_attempts == 1
    assert failed.available_at > datetime.now(UTC)


async def test_two_relays_skip_locked(database: Database) -> None:
    one, two = await seed(database), await seed(database)
    publisher = Publisher(block=True)
    other = Publisher()
    first = asyncio.create_task(OutboxRelay(database, publisher, batch_size=1).run_once())
    try:
        await asyncio.wait_for(publisher.entered.wait(), 2)
        assert await asyncio.wait_for(OutboxRelay(database, other).run_once(), 2) == 1
        assert other.calls[0][2] == two.id
    finally:
        publisher.release.set()
    assert await first == 1
    assert publisher.calls[0][2] == one.id


async def test_batch_is_bounded_and_skips_future_and_published(database: Database) -> None:
    rows = [await seed(database) for _ in range(4)]
    async with database.session_factory.begin() as session:
        future = await session.get(Outbox, rows[0].id)
        done = await session.get(Outbox, rows[1].id)
        assert future is not None and done is not None
        future.available_at = datetime.now(UTC) + timedelta(days=1)
        done.published_at = datetime.now(UTC)
    publisher = Publisher()
    relay = OutboxRelay(database, publisher, batch_size=1)
    assert await relay.run_once() == 1
    assert len(publisher.calls) == 1
    assert await relay.run_once() == 1
    assert await relay.run_once() == 0
    assert len({call[2] for call in publisher.calls}) == 2


async def test_confirm_then_commit_failure_republishes(
    database: Database, messaging: Messaging
) -> None:
    row = await seed(database)
    # Deferred constraint trigger fails at actual COMMIT, after publisher confirm.
    async with database.engine.begin() as connection:
        await connection.execute(
            text("""
            CREATE FUNCTION reject_commit() RETURNS trigger LANGUAGE plpgsql AS $$
            BEGIN RAISE EXCEPTION 'injected commit failure'; END $$
        """)
        )
        await connection.execute(
            text("""
            CREATE CONSTRAINT TRIGGER fail_commit AFTER UPDATE ON outbox
            DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION reject_commit()
        """)
        )
    relay = OutboxRelay(database, messaging)
    with pytest.raises(DBAPIError, match="injected commit failure"):
        await relay.run_once()
    assert (await read(database, row.id)).published_at is None
    queue = await messaging.broker.declare_queue(messaging.topology.queue)
    first = await receive(queue)
    assert first is not None and first.message_id == str(row.id)
    await first.ack()
    async with database.engine.begin() as connection:
        await connection.execute(text("DROP TRIGGER fail_commit ON outbox"))
    assert await relay.run_once() == 1
    second = await receive(queue)
    assert second is not None and second.message_id == first.message_id
    await second.ack()


async def test_unroutable_reschedules_real_broker(database: Database, messaging: Messaging) -> None:
    row = await seed(database)
    async with database.session_factory.begin() as session:
        current = await session.get(Outbox, row.id)
        assert current is not None
        current.destination = "missing.route"
    assert await OutboxRelay(database, messaging).run_once() == 0
    failed = await read(database, row.id)
    assert failed.published_at is None
    assert failed.last_error == "publish_unroutable"
    assert failed.available_at > datetime.now(UTC)


async def test_processor_third_failure_relay_delivers_to_dlq(
    database: Database, messaging: Messaging, monkeypatch: pytest.MonkeyPatch
) -> None:
    class FailingWebhookSender:
        def __init__(self) -> None:
            self.calls: list[tuple[UUID, int]] = []

        async def send(self, payment: Payment) -> None:
            self.calls.append((payment.webhook_event_id, payment.expected_attempt))
            raise DeliveryError("webhook_timeout")

    # Real emulator decision, processor, DB transactions, relay, confirms and broker;
    # only the emulator delay and the not-yet-implemented HTTP transport are replaced.
    sleep = AsyncMock()
    monkeypatch.setattr("payments.gateway.sleep", sleep)
    initial = await seed(database)
    original = PaymentEvent.model_validate(initial.payload)
    sender = FailingWebhookSender()
    processor = PaymentProcessor(database, Gateway(), sender)
    relay = OutboxRelay(database, messaging)
    main_queue = await messaging.broker.declare_queue(messaging.topology.queue)

    for attempt in [1, 2, 3]:
        if attempt > 1:
            # Advance just the fixture-owned durable retry deadline. Exact default
            # 2/4s scheduling is covered by the processor tests; no wall-clock sleeps.
            async with database.session_factory.begin() as session:
                retry = (
                    await session.scalars(
                        select(Outbox).where(
                            Outbox.payment_id == initial.payment_id,
                            Outbox.event_type == "payments.new",
                            Outbox.attempt == attempt,
                        )
                    )
                ).one()
                retry.available_at = datetime.now(UTC) - timedelta(seconds=1)
        assert await relay.run_once() == 1
        incoming = await receive(main_queue)
        event = PaymentEvent.model_validate_json(incoming.body)
        assert event.event_id == original.event_id
        assert event.generation == original.generation
        assert event.payment_id == original.payment_id
        assert event.attempt == attempt
        assert incoming.exchange == "payments"
        assert incoming.routing_key == "payments.new"
        await processor.process(event)
        await incoming.ack()

    async with database.session_factory() as session:
        payment = await session.get(Payment, original.payment_id)
        assert payment is not None
        assert payment.status in {"succeeded", "failed"}
        terminal_status = payment.status
        processed_at = payment.processed_at
        assert processed_at is not None
        assert payment.webhook_status == "exhausted"
        assert payment.expected_attempt == 3
        dead_row = (
            await session.scalars(
                select(Outbox).where(
                    Outbox.payment_id == original.payment_id,
                    Outbox.event_type == "payments.dlq",
                )
            )
        ).one()
        assert dead_row.published_at is None

    count = await relay.run_once()
    published = await read(database, dead_row.id)
    assert (count, published.last_error) == (1, None)
    assert published.published_at is not None
    assert published.publish_attempts == 1
    dead_queue = await messaging.broker.declare_queue(messaging.topology.dead_queue)
    dead = await receive(dead_queue)
    assert dead.exchange == "payments.dlx"
    assert dead.routing_key == "payments.dlq"
    assert dead.delivery_mode == DeliveryMode.PERSISTENT
    assert dead.message_id == str(dead_row.id)
    assert json.loads(dead.body) == published.payload
    assert published.payload == {
        "schema_version": 1,
        "event_id": str(original.event_id),
        "payment_id": str(original.payment_id),
        "generation": str(original.generation),
        "attempt": 3,
        "diagnostics": {
            "reason": "webhook_attempts_exhausted",
            "delivery_error": "webhook_timeout",
        },
    }
    await dead.ack()
    assert sender.calls == [(original.event_id, attempt) for attempt in [1, 2, 3]]
    assert sleep.await_count == 1  # Only the first pending event conducts the payment.
    async with database.session_factory() as session:
        payment = await session.get(Payment, original.payment_id)
        assert payment is not None
        assert payment.status == terminal_status
        assert payment.processed_at == processed_at
        assert payment.webhook_status == "exhausted"
        all_rows = list(
            await session.scalars(select(Outbox).where(Outbox.payment_id == original.payment_id))
        )
        assert len(all_rows) == 4
        assert all(row.published_at is not None for row in all_rows)
