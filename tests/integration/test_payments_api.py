import json
from collections.abc import AsyncIterator
from decimal import Decimal
from typing import Any
from uuid import UUID, uuid4

import pytest
from httpx import AsyncClient
from sqlalchemy import select

from payments.db import Database
from payments.models import Outbox, Payment
from payments.schemas import PaymentEvent

BODY = {
    "amount": "12.30",
    "currency": "RUB",
    "description": "order",
    "metadata": {"order": 4},
    "webhook_url": "https://a.b.example.com/hook?token=private-query",
}


async def test_create_get(client: AsyncClient, database: Database) -> None:
    response = await client.post("/api/v1/payments", json=BODY)
    assert response.status_code == 202
    accepted = response.json()
    assert set(accepted) == {"payment_id", "status", "created_at"}
    assert accepted["status"] == "pending"
    response = await client.get(f"/api/v1/payments/{accepted['payment_id']}")
    assert response.status_code == 200
    payment = response.json()
    assert payment | BODY == payment
    assert payment["amount"] == "12.30"
    assert payment["processed_at"] is None
    assert payment["webhook_status"] == "pending"
    assert payment["webhook_delivered_at"] is None
    assert set(payment) == set(BODY) | {
        "payment_id",
        "status",
        "created_at",
        "processed_at",
        "webhook_status",
        "webhook_delivered_at",
    }
    async with database.session_factory() as session:
        stored = (await session.scalars(select(Payment))).one()
        event = (await session.scalars(select(Outbox))).one()
        envelope = PaymentEvent.model_validate(event.payload)
        assert stored.amount == Decimal("12.30")
        assert stored.metadata_ == {"order": 4}
        assert stored.expected_attempt == 1
        assert envelope.payment_id == UUID(accepted["payment_id"])
        assert envelope.generation == stored.generation == event.generation
        assert envelope.event_id == stored.webhook_event_id
        assert envelope.schema_version == event.schema_version == 1
        assert envelope.attempt == event.attempt == 1
        assert event.event_type == "payments.new"
        assert event.destination == "payments.new"
        assert event.publish_attempts == 0
        assert event.published_at is None


async def test_create_without_broker(client: AsyncClient) -> None:
    # Settings deliberately point RabbitMQ at a closed port.
    assert (await client.post("/api/v1/payments", json=BODY)).status_code == 202
    assert (await client.get("/health/ready")).status_code == 200
    assert (await client.get("/health/live")).status_code == 200


async def test_exact_limit_streaming_body(client: AsyncClient) -> None:
    raw = json.dumps(BODY).encode()
    raw += b" " * (32768 - len(raw))

    async def chunks() -> AsyncIterator[bytes]:
        yield raw[:16000]
        yield raw[16000:]

    response = await client.post(
        "/api/v1/payments", content=chunks(), headers={"Content-Type": "application/json"}
    )
    assert response.status_code == 202


async def test_missing_payment(client: AsyncClient) -> None:
    assert (await client.get(f"/api/v1/payments/{uuid4()}")).status_code == 404
    assert (await client.get("/api/v1/payments/not-a-uuid")).status_code == 422


@pytest.mark.parametrize("amount", ["9999999999999999.99", "9007199254740993.01", "0.01"])
async def test_exact_numeric_money(client: AsyncClient, amount: str) -> None:
    raw = json.dumps(BODY).replace('"12.30"', amount)
    response = await client.post(
        "/api/v1/payments", content=raw, headers={"Content-Type": "application/json"}
    )
    assert response.status_code == 202
    payment = await client.get(f"/api/v1/payments/{response.json()['payment_id']}")
    assert payment.json()["amount"] == amount


@pytest.mark.parametrize(
    "raw",
    [
        b'{"amount":1.000,"currency":"RUB","webhook_url":"https://example.com"}',
        b'{"amount":1e9999999999999999999}',
        b'{"amount":',
        b'"private-body"',
        b'{"amount":true,"private-extra":"secret-value"}',
        b'{"amount":1,"currency":"RUB","metadata":{"secret-key": {"x":null}},'
        b'"webhook_url":"https://user:secret@example.com"}',
        b'{"amount":1,"currency":"RUB","metadata":{"secret-key":null},'
        b'"private-extra":"secret-value","webhook_url":"https://example.com"}',
        (b"[" * 10000) + (b"]" * 10000),
    ],
)
async def test_raw_validation_is_sanitized_422(client: AsyncClient, raw: bytes) -> None:
    response = await client.post(
        "/api/v1/payments", content=raw, headers={"Content-Type": "application/json"}
    )
    assert response.status_code == 422
    for secret in ("secret-value", "private-extra", "private-body", "secret-key", "user:"):
        assert secret not in response.text
    assert '"input"' not in response.text
    assert '"ctx"' not in response.text


@pytest.mark.parametrize(
    "url", ["http://example.com/hook", "https://attacker.net/hook?secret-value"]
)
async def test_url_policy_before_persistence(
    client: AsyncClient, database: Database, url: str
) -> None:
    response = await client.post("/api/v1/payments", json=BODY | {"webhook_url": url})
    assert response.status_code == 422
    assert url not in response.text
    async with database.session_factory() as session:
        assert (await session.scalars(select(Payment))).all() == []
        assert (await session.scalars(select(Outbox))).all() == []


@pytest.mark.parametrize("key", [None, "", "x" * 129])
async def test_invalid_idempotency_key(client: AsyncClient, key: str | None) -> None:
    client.headers.pop("Idempotency-Key")
    headers = {} if key is None else {"Idempotency-Key": key}
    response = await client.post("/api/v1/payments", json=BODY, headers=headers)
    assert response.status_code == 422
    assert "x" * 129 not in response.text


@pytest.mark.parametrize(
    "change",
    [
        {"description": "PRIVATE_NUL\x00description"},
        {"metadata": {"PRIVATE_NUL\x00key": 1}},
        {"metadata": {"key": "PRIVATE_NUL\x00value"}},
        {"metadata": {"outer": [{"PRIVATE_NUL\x00key": True}]}},
        {"metadata": {"outer": {"inner": "PRIVATE_NUL\x00value"}}},
        {"metadata": {"outer": [None, 1, ["PRIVATE_NUL\x00list"]]}},
        {"webhook_url": "https://example.com/PRIVATE_NUL\x00path"},
    ],
)
async def test_nul_input_is_sanitized_422_without_payment_or_outbox(
    client: AsyncClient, database: Database, change: dict[str, Any]
) -> None:
    response = await client.post("/api/v1/payments", json=BODY | change)
    assert response.status_code == 422
    assert "PRIVATE_NUL" not in response.text
    assert '"input"' not in response.text
    assert '"ctx"' not in response.text
    async with database.session_factory() as session:
        assert (await session.scalars(select(Payment))).all() == []
        assert (await session.scalars(select(Outbox))).all() == []


async def test_nul_idempotency_key_is_422_before_persistence(
    client: AsyncClient, database: Database
) -> None:
    # This user-controlled string is also stored as PostgreSQL text.
    response = await client.post(
        "/api/v1/payments", json=BODY, headers={"Idempotency-Key": "PRIVATE_NUL\x00key"}
    )
    assert response.status_code == 422
    assert "PRIVATE_NUL" not in response.text
    async with database.session_factory() as session:
        assert (await session.scalars(select(Payment))).all() == []
        assert (await session.scalars(select(Outbox))).all() == []
