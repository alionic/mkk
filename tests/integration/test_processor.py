import asyncio
import json
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime, timedelta
from typing import Literal, cast
from uuid import UUID, uuid4

import pytest
from sqlalchemy import select, text
from sqlalchemy.exc import DBAPIError

from payments.db import Database
from payments.gateway import Gateway
from payments.models import Outbox, Payment
from payments.processor import DeliveryError, InvalidPaymentEvent, PaymentProcessor
from payments.schemas import PaymentCreate, PaymentEvent
from payments.service import create_payment


class GatewaySpy(Gateway):
    """Only replace the emulator delay/outcome; persistence always uses real PostgreSQL."""

    def __init__(self, result: Literal["succeeded", "failed"] = "succeeded") -> None:
        self.result = result
        self.calls: list[UUID] = []
        self.entered = asyncio.Event()
        self.release = asyncio.Event()
        self.release.set()

    async def process(self, payment_id: UUID) -> Literal["succeeded", "failed"]:
        self.calls.append(payment_id)
        self.entered.set()
        await self.release.wait()
        return self.result


class Sender:
    def __init__(
        self,
        error: BaseException | None = None,
        check: Callable[[Payment], Awaitable[None]] | None = None,
    ) -> None:
        self.error = error
        self.check = check
        self.calls: list[Payment] = []

    async def send(self, payment: Payment) -> None:
        self.calls.append(payment)
        if self.check:
            await self.check(payment)
        if self.error:
            raise self.error


async def seed(database: Database) -> PaymentEvent:
    async with database.session_factory.begin() as session:
        payment = await create_payment(
            session,
            PaymentCreate.model_validate(
                {
                    "amount": "10.00",
                    "currency": "USD",
                    "webhook_url": "https://example.com/hook?token=private-url-secret",
                    "metadata": {"private-metadata-secret": "private-value-secret"},
                }
            ),
            str(uuid4()),
        )
        return PaymentEvent(
            schema_version=1,
            event_id=payment.webhook_event_id,
            payment_id=payment.id,
            generation=payment.generation,
            attempt=1,
        )


async def read(database: Database, payment_id: UUID) -> Payment:
    async with database.session_factory() as session:
        payment = await session.get(Payment, payment_id)
        assert payment is not None
        return payment


async def rows(database: Database, event: PaymentEvent) -> list[Outbox]:
    async with database.session_factory() as session:
        return list(
            await session.scalars(
                select(Outbox)
                .where(Outbox.payment_id == event.payment_id)
                .order_by(Outbox.created_at, Outbox.id)
            )
        )


async def fail_commit(database: Database, table: str, condition: str = "true") -> None:
    # Identifiers/condition are test-owned literals, never inputs or application data.
    assert table in {"payments", "outbox"}
    async with database.engine.begin() as connection:
        await connection.execute(
            text("""
            CREATE FUNCTION reject_commit() RETURNS trigger LANGUAGE plpgsql AS $$
            BEGIN RAISE EXCEPTION 'injected commit failure'; END $$
            """)
        )
        await connection.execute(
            text(f"""
            CREATE CONSTRAINT TRIGGER fail_commit AFTER INSERT OR UPDATE ON {table}
            DEFERRABLE INITIALLY DEFERRED FOR EACH ROW
            WHEN ({condition}) EXECUTE FUNCTION reject_commit()
            """)
        )


async def stop_failing_commit(database: Database, table: str) -> None:
    assert table in {"payments", "outbox"}
    async with database.engine.begin() as connection:
        await connection.execute(text(f"DROP TRIGGER fail_commit ON {table}"))


async def test_terminal_status_saved_before_webhook(database: Database) -> None:
    event = await seed(database)

    async def check(payment: Payment) -> None:
        current = await read(database, payment.id)
        assert current.status == payment.status == "succeeded"
        assert current.processed_at == payment.processed_at
        assert current.processed_at is not None
        assert payment.metadata_ == {"private-metadata-secret": "private-value-secret"}
        assert payment.webhook_event_id == event.event_id
        # A separate connection can take the lock NOWAIT: no DB tx spans HTTP I/O.
        async with database.session_factory.begin() as session:
            await session.execute(
                select(Payment).where(Payment.id == payment.id).with_for_update(nowait=True)
            )

    sender = Sender(check=check)
    await PaymentProcessor(database, GatewaySpy(), sender).process(event)
    payment = await read(database, event.payment_id)
    assert payment.webhook_status == "delivered"
    assert payment.webhook_delivered_at is not None
    assert payment.last_error is None
    assert len(sender.calls) == 1
    assert len(await rows(database, event)) == 1


async def test_decline_still_notifies(database: Database) -> None:
    event = await seed(database)
    sender = Sender()
    await PaymentProcessor(database, GatewaySpy("failed"), sender).process(event)
    payment = await read(database, event.payment_id)
    assert payment.status == sender.calls[0].status == "failed"
    assert payment.processed_at is not None
    assert payment.webhook_status == "delivered"


async def test_duplicate_does_not_charge_twice(database: Database) -> None:
    event = await seed(database)
    gateway, sender = GatewaySpy(), Sender()
    processor = PaymentProcessor(database, gateway, sender)
    await processor.process(event)
    processed_at = (await read(database, event.payment_id)).processed_at
    await processor.process(event)
    assert gateway.calls == [event.payment_id]
    assert len(sender.calls) == 1
    assert (await read(database, event.payment_id)).processed_at == processed_at


async def test_old_attempt_noop(database: Database) -> None:
    event = await seed(database)
    async with database.session_factory.begin() as session:
        payment = await session.get(Payment, event.payment_id)
        assert payment is not None
        payment.expected_attempt = 2
    gateway, sender = GatewaySpy(), Sender()
    await PaymentProcessor(database, gateway, sender).process(event)
    assert gateway.calls == sender.calls == []
    assert (await read(database, event.payment_id)).status == "pending"
    assert len(await rows(database, event)) == 1


@pytest.mark.parametrize("result", ["succeeded", "failed"])
async def test_attempts_1_2_3_schedule_2_4_and_dlq(
    database: Database, result: Literal["succeeded", "failed"]
) -> None:
    event = await seed(database)
    gateway = GatewaySpy(result)
    sender = Sender(DeliveryError("webhook_timeout"))
    processor = PaymentProcessor(database, gateway, sender)
    for attempt, delay in [(1, 2), (2, 4), (3, 0)]:
        before = datetime.now(UTC)
        await processor.process(event.model_copy(update={"attempt": attempt}))
        after = datetime.now(UTC)
        payment = await read(database, event.payment_id)
        assert payment.status == result
        assert payment.expected_attempt == min(attempt + 1, 3)
        assert payment.webhook_status == ("exhausted" if attempt == 3 else "pending")
        assert payment.webhook_delivered_at is None
        assert payment.last_error == "webhook_timeout"
        outbox = await rows(database, event)
        assert len(outbox) == attempt + 1
        newest = outbox[-1]
        assert before + timedelta(seconds=delay) <= newest.available_at
        assert newest.available_at <= after + timedelta(seconds=delay)
        assert newest.attempt == min(attempt + 1, 3)
        assert newest.generation == event.generation
        assert newest.schema_version == 1
        assert newest.published_at is None
        expected = {
            "schema_version": 1,
            "event_id": str(event.event_id),
            "payment_id": str(event.payment_id),
            "generation": str(event.generation),
            "attempt": min(attempt + 1, 3),
        }
        if attempt < 3:
            assert newest.event_type == newest.destination == "payments.new"
            assert newest.payload == expected
        else:
            assert newest.event_type == newest.destination == "payments.dlq"
            assert newest.payload == {
                **expected,
                "diagnostics": {
                    "reason": "webhook_attempts_exhausted",
                    "delivery_error": "webhook_timeout",
                },
            }
    assert gateway.calls == [event.payment_id]
    assert len(sender.calls) == 3
    assert {payment.webhook_event_id for payment in sender.calls} == {event.event_id}
    await processor.process(event.model_copy(update={"attempt": 3}))
    assert len(sender.calls) == 3
    assert len(await rows(database, event)) == 4


@pytest.mark.parametrize("attempt", [1, 3])
async def test_retry_transaction_rolls_back_atomically(database: Database, attempt: int) -> None:
    event = await seed(database)
    async with database.session_factory.begin() as session:
        payment = await session.get(Payment, event.payment_id)
        assert payment is not None
        payment.expected_attempt = attempt
    event = event.model_copy(update={"attempt": attempt})
    await fail_commit(database, "outbox")
    sender = Sender(DeliveryError("webhook_network_error"))
    gateway = GatewaySpy()
    processor = PaymentProcessor(database, gateway, sender)
    with pytest.raises(DBAPIError, match="injected commit failure"):
        await processor.process(event)
    payment = await read(database, event.payment_id)
    assert payment.status == "succeeded"  # The earlier gateway commit remains durable.
    assert payment.processed_at is not None
    assert payment.expected_attempt == attempt
    assert payment.webhook_status == "pending"
    assert payment.last_error is None
    assert len(await rows(database, event)) == 1
    await stop_failing_commit(database, "outbox")
    await processor.process(event)
    assert gateway.calls == [event.payment_id]
    assert len(sender.calls) == 2
    assert len(await rows(database, event)) == 2


async def test_status_commit_failure_prevents_send(database: Database) -> None:
    event = await seed(database)
    await fail_commit(database, "payments")
    sender = Sender()
    with pytest.raises(DBAPIError, match="injected commit failure"):
        await PaymentProcessor(database, GatewaySpy(), sender).process(event)
    assert sender.calls == []
    payment = await read(database, event.payment_id)
    assert payment.status == "pending"
    assert payment.processed_at is None
    assert payment.expected_attempt == 1


async def test_crash_after_status_commit_reuses_result(database: Database) -> None:
    event = await seed(database)
    gateway = GatewaySpy("failed")
    crashed = Sender(asyncio.CancelledError())
    with pytest.raises(asyncio.CancelledError):
        await PaymentProcessor(database, gateway, crashed).process(event)
    first = await read(database, event.payment_id)
    assert first.status == "failed"
    assert first.processed_at is not None
    assert first.webhook_status == "pending"
    sender = Sender()
    await PaymentProcessor(database, gateway, sender).process(event)
    assert gateway.calls == [event.payment_id]
    assert sender.calls[0].processed_at == first.processed_at
    assert sender.calls[0].webhook_event_id == crashed.calls[0].webhook_event_id == event.event_id
    assert (await read(database, event.payment_id)).webhook_status == "delivered"


async def test_crash_after_retry_commit_skips_old_message(database: Database) -> None:
    event = await seed(database)
    gateway = GatewaySpy()
    sender = Sender(DeliveryError("webhook_http_error"))
    await PaymentProcessor(database, gateway, sender).process(event)
    # Caller dies before broker ack, then a fresh process receives the same envelope.
    replay_sender = Sender()
    replay_gateway = GatewaySpy()
    replay = PaymentProcessor(database, replay_gateway, replay_sender)
    await replay.process(event)
    assert replay_gateway.calls == replay_sender.calls == []
    assert (await read(database, event.payment_id)).expected_attempt == 2
    outbox = await rows(database, event)
    assert len(outbox) == 2
    await replay.process(PaymentEvent.model_validate(outbox[-1].payload))
    assert replay_gateway.calls == []
    assert len(replay_sender.calls) == 1
    assert replay_sender.calls[0].webhook_event_id == event.event_id
    assert (await read(database, event.payment_id)).webhook_status == "delivered"


async def test_new_generation_excludes_old(database: Database) -> None:
    event = await seed(database)
    async with database.session_factory.begin() as session:
        payment = await session.get(Payment, event.payment_id)
        assert payment is not None
        payment.generation = uuid4()
    gateway, sender = GatewaySpy(), Sender()
    processor = PaymentProcessor(database, gateway, sender)
    await processor.process(event)
    assert gateway.calls == sender.calls == []
    current = await read(database, event.payment_id)
    assert current.status == "pending"
    assert current.webhook_event_id == event.event_id
    await processor.process(event.model_copy(update={"generation": current.generation}))
    assert gateway.calls == [event.payment_id]
    assert len(sender.calls) == 1


@pytest.mark.parametrize("state", ["delivered", "exhausted"])
async def test_exhausted_is_noop(
    database: Database, state: Literal["delivered", "exhausted"]
) -> None:
    event = await seed(database)
    async with database.session_factory.begin() as session:
        payment = await session.get(Payment, event.payment_id)
        assert payment is not None
        payment.webhook_status = state
        if state == "delivered":
            payment.webhook_delivered_at = datetime.now(UTC)
    gateway, sender = GatewaySpy(), Sender()
    await PaymentProcessor(database, gateway, sender).process(event)
    assert gateway.calls == sender.calls == []
    assert (await read(database, event.payment_id)).status == "pending"
    assert len(await rows(database, event)) == 1


@pytest.mark.parametrize("change", ["future_attempt", "event_id", "missing_payment", "schema"])
async def test_invalid_envelope_rejects(database: Database, change: str) -> None:
    event = await seed(database)
    updates: dict[str, object] = {
        "future_attempt": {"attempt": 2},
        "event_id": {"event_id": uuid4()},
        "missing_payment": {"payment_id": uuid4()},
        "schema": {"schema_version": 2},
    }
    modified = event.model_copy(update=cast(dict[str, object], updates[change]))
    gateway, sender = GatewaySpy(), Sender()
    with pytest.raises(InvalidPaymentEvent) as error:
        await PaymentProcessor(database, gateway, sender).process(modified)
    assert "private" not in str(error.value)
    assert gateway.calls == sender.calls == []
    assert (await read(database, event.payment_id)).status == "pending"
    assert len(await rows(database, event)) == 1


async def test_unexpected_sender_error_is_not_delivery_failure(database: Database) -> None:
    event = await seed(database)
    sender = Sender(RuntimeError("unexpected infrastructure error"))
    with pytest.raises(RuntimeError, match="unexpected infrastructure error"):
        await PaymentProcessor(database, GatewaySpy(), sender).process(event)
    payment = await read(database, event.payment_id)
    assert payment.expected_attempt == 1
    assert payment.webhook_status == "pending"
    assert payment.last_error is None
    assert len(await rows(database, event)) == 1


async def test_delivery_commit_failure_repeats_same_webhook_identity(database: Database) -> None:
    event = await seed(database)
    await fail_commit(database, "payments", "NEW.webhook_status = 'delivered'")
    gateway, sender = GatewaySpy(), Sender()
    processor = PaymentProcessor(database, gateway, sender)
    with pytest.raises(DBAPIError, match="injected commit failure"):
        await processor.process(event)
    payment = await read(database, event.payment_id)
    assert payment.webhook_status == "pending"
    assert payment.webhook_delivered_at is None
    assert payment.expected_attempt == 1
    assert len(await rows(database, event)) == 1
    await stop_failing_commit(database, "payments")
    await processor.process(event)
    assert gateway.calls == [event.payment_id]
    assert len(sender.calls) == 2
    assert {payment.webhook_event_id for payment in sender.calls} == {event.event_id}
    assert (await read(database, event.payment_id)).webhook_status == "delivered"


async def test_dlq_diagnostics_do_not_leak_untrusted_errors(database: Database) -> None:
    event = await seed(database)
    sender = Sender(DeliveryError("https://private-user:private-pass@example.com/private-secret"))
    processor = PaymentProcessor(database, GatewaySpy(), sender)
    for attempt in [1, 2, 3]:
        await processor.process(event.model_copy(update={"attempt": attempt}))
    payment = await read(database, event.payment_id)
    dlq = (await rows(database, event))[-1]
    assert payment.last_error == "webhook_failed"
    assert dlq.payload["diagnostics"] == {
        "reason": "webhook_attempts_exhausted",
        "delivery_error": "webhook_failed",
    }
    serialized = json.dumps(dlq.payload)
    assert "private" not in serialized
    assert "example.com" not in serialized
    assert "private" not in str(sender.error)


async def test_pending_gateway_holds_row_lock(database: Database) -> None:
    event = await seed(database)
    gateway = GatewaySpy()
    gateway.release.clear()
    sender = Sender()
    first = asyncio.create_task(PaymentProcessor(database, gateway, sender).process(event))
    try:
        await asyncio.wait_for(gateway.entered.wait(), 2)
        assert (await read(database, event.payment_id)).status == "pending"
        with pytest.raises(DBAPIError):
            async with database.session_factory.begin() as session:
                await session.execute(
                    select(Payment)
                    .where(Payment.id == event.payment_id)
                    .with_for_update(nowait=True)
                )
    finally:
        gateway.release.set()
        await first
    assert (await read(database, event.payment_id)).webhook_status == "delivered"


async def test_generation_change_during_send_does_not_mutate_new_flow(database: Database) -> None:
    event = await seed(database)
    new_generation = uuid4()

    async def replay(payment: Payment) -> None:
        async with database.session_factory.begin() as session:
            current = await session.get(Payment, payment.id)
            assert current is not None
            current.generation = new_generation

    sender = Sender(DeliveryError("webhook_timeout"), check=replay)
    await PaymentProcessor(database, GatewaySpy(), sender).process(event)
    payment = await read(database, event.payment_id)
    assert payment.generation == new_generation
    assert payment.expected_attempt == 1
    assert payment.webhook_status == "pending"
    assert payment.last_error is None
    assert len(await rows(database, event)) == 1
