import asyncio
from uuid import uuid4

import pytest

from payments.cli import ReplayRejected, replay_webhook
from payments.db import Database
from payments.models import Payment, utc_now
from payments.processor import PaymentProcessor
from payments.schemas import PaymentEvent

from .test_processor import GatewaySpy, Sender, fail_commit, read, rows, seed


async def exhaust(database: Database, event: PaymentEvent) -> None:
    async with database.session_factory.begin() as session:
        payment = await session.get(Payment, event.payment_id)
        assert payment is not None
        payment.status = "succeeded"
        payment.processed_at = utc_now()
        payment.webhook_status = "exhausted"
        payment.expected_attempt = 3
        payment.last_error = "webhook_timeout"


async def test_replay_exhausted_atomic(database: Database) -> None:
    old = await seed(database)
    await exhaust(database, old)
    before = await read(database, old.payment_id)
    event = await replay_webhook(database, old.payment_id)
    current = await read(database, old.payment_id)
    assert current.generation == event.generation != old.generation
    assert current.webhook_status == "pending" and current.expected_attempt == 1
    assert current.last_error is None and current.webhook_delivered_at is None
    for field in (
        "status",
        "processed_at",
        "webhook_event_id",
        "amount",
        "currency",
        "metadata_",
        "webhook_url",
        "request_hash",
        "description",
        "idempotency_key",
    ):
        assert getattr(current, field) == getattr(before, field)
    outbox = await rows(database, old)
    assert len(outbox) == 2
    fresh = next(row for row in outbox if row.generation == event.generation)
    assert fresh.destination == fresh.event_type == "payments.new"
    assert fresh.attempt == 1 and fresh.published_at is None
    assert PaymentEvent.model_validate(fresh.payload) == event
    assert event.event_id == old.event_id
    await exhaust(database, event)
    third = await replay_webhook(database, old.payment_id)
    assert len({old.generation, event.generation, third.generation}) == 3


@pytest.mark.parametrize("state", ["pending", "delivered", "missing", "nonterminal"])
async def test_replay_rejects_pending_or_delivered(database: Database, state: str) -> None:
    old = await seed(database)
    async with database.session_factory.begin() as session:
        payment = await session.get(Payment, old.payment_id)
        assert payment is not None
        if state == "delivered":
            payment.webhook_status = "delivered"
            payment.webhook_delivered_at = utc_now()
        if state == "nonterminal":
            payment.webhook_status = "exhausted"
    with pytest.raises(ReplayRejected):
        await replay_webhook(database, uuid4() if state == "missing" else old.payment_id)
    assert (await read(database, old.payment_id)).generation == old.generation
    assert len(await rows(database, old)) == 1


async def test_replay_commit_failure_rolls_back(database: Database) -> None:
    old = await seed(database)
    await exhaust(database, old)
    await fail_commit(database, "outbox")
    with pytest.raises(Exception, match="injected commit failure"):
        await replay_webhook(database, old.payment_id)
    current = await read(database, old.payment_id)
    assert current.generation == old.generation and current.webhook_status == "exhausted"
    assert len(await rows(database, old)) == 1


async def test_concurrent_replay_rowlock(database: Database) -> None:
    old = await seed(database)
    await exhaust(database, old)
    results = await asyncio.gather(
        replay_webhook(database, old.payment_id),
        replay_webhook(database, old.payment_id),
        return_exceptions=True,
    )
    assert sum(isinstance(result, PaymentEvent) for result in results) == 1
    assert sum(isinstance(result, ReplayRejected) for result in results) == 1
    assert len(await rows(database, old)) == 2


async def test_old_generation_after_operator_replay_noop(database: Database) -> None:
    old = await seed(database)
    await exhaust(database, old)
    fresh = await replay_webhook(database, old.payment_id)
    gateway, sender = GatewaySpy(), Sender()
    processor = PaymentProcessor(database, gateway, sender)
    for attempt in (1, 2, 3):
        await processor.process(old.model_copy(update={"attempt": attempt}))
    assert gateway.calls == sender.calls == []
    current = await read(database, old.payment_id)
    assert current.expected_attempt == 1 and current.webhook_status == "pending"
    assert current.generation == fresh.generation
    assert len(await rows(database, old)) == 2
