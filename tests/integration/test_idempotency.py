import asyncio
from datetime import UTC, datetime
from uuid import uuid4

import pytest
from httpx import AsyncClient
from sqlalchemy import func, select, text
from sqlalchemy.exc import DBAPIError, IntegrityError

from payments.db import Database
from payments.models import Outbox, Payment
from payments.schemas import PaymentCreate
from payments.service import create_payment, get_payment

BODY = {"amount": "10.00", "currency": "USD", "webhook_url": "https://example.com/hook"}


async def test_concurrent_same_key(client: AsyncClient, database: Database) -> None:
    responses = await asyncio.gather(
        *(client.post("/api/v1/payments", json=BODY) for _ in range(10))
    )
    assert [r.status_code for r in responses] == [202] * 10
    assert len({r.json()["payment_id"] for r in responses}) == 1
    async with database.session_factory() as session:
        assert await session.scalar(select(func.count()).select_from(Payment)) == 1
        assert await session.scalar(select(func.count()).select_from(Outbox)) == 1


async def test_conflicting_key(client: AsyncClient, database: Database) -> None:
    responses = await asyncio.gather(
        client.post("/api/v1/payments", json=BODY),
        client.post("/api/v1/payments", json=BODY | {"amount": "11.00"}),
    )
    assert sorted(r.status_code for r in responses) == [202, 409]
    assert "request_hash" not in responses[1].text
    async with database.session_factory() as session:
        assert await session.scalar(select(func.count()).select_from(Outbox)) == 1


async def test_canonical_replay_keeps_terminal_payment(
    client: AsyncClient, database: Database
) -> None:
    first = (await client.post("/api/v1/payments", json=BODY)).json()
    async with database.session_factory.begin() as session:
        payment = await get_payment(session, uuid4())
        assert payment is None
        payment = (await session.scalars(select(Payment))).one()
        payment.status = "succeeded"
        payment.processed_at = datetime.now(UTC)
    replay = await client.post("/api/v1/payments", json=BODY | {"amount": "10.0"})
    assert replay.status_code == 202
    assert replay.json() == first | {"status": "succeeded"}


async def test_atomic_payment_outbox_rollback(database: Database) -> None:
    # PostgreSQL trigger faults the *second write*, proving payment INSERT rolls back.
    async with database.engine.begin() as connection:
        await connection.execute(
            text("""
            CREATE FUNCTION reject_outbox() RETURNS trigger LANGUAGE plpgsql AS $$
            BEGIN RAISE EXCEPTION 'injected outbox failure'; END $$
        """)
        )
        await connection.execute(
            text("""
            CREATE TRIGGER fail_outbox BEFORE INSERT ON outbox
            FOR EACH ROW EXECUTE FUNCTION reject_outbox()
        """)
        )
    with pytest.raises(DBAPIError, match="injected outbox failure"):
        async with database.session_factory.begin() as session:
            await create_payment(session, PaymentCreate.model_validate(BODY), "atomic-key")
    async with database.session_factory() as session:
        assert await session.scalar(select(func.count()).select_from(Payment)) == 0
        assert await session.scalar(select(func.count()).select_from(Outbox)) == 0


async def test_generation_uniqueness_and_stable_event_id(database: Database) -> None:
    async with database.session_factory.begin() as session:
        payment = await create_payment(session, PaymentCreate.model_validate(BODY), "generation")
        payment_id = payment.id
        event_id = payment.webhook_event_id
        original_generation = payment.generation
        original = (await session.scalars(select(Outbox))).one()
        values = {
            "payment_id": payment.id,
            "generation": payment.generation,
            "event_type": original.event_type,
            "attempt": 1,
            "schema_version": 1,
            "payload": original.payload,
            "destination": original.destination,
        }
    with pytest.raises(IntegrityError):
        async with database.session_factory.begin() as session:
            session.add(Outbox(**values))
    async with database.session_factory.begin() as session:
        replay_payment = await session.get(Payment, payment_id)
        assert replay_payment is not None
        replay_payment.generation = uuid4()
        values["generation"] = replay_payment.generation
        session.add(Outbox(**values))
        assert replay_payment.generation != original_generation
        assert replay_payment.webhook_event_id == event_id
    async with database.session_factory() as session:
        assert await session.scalar(select(func.count()).select_from(Outbox)) == 2
