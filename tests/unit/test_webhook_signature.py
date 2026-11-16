import hashlib
import hmac
import json
from datetime import UTC, datetime, timedelta, timezone
from decimal import Decimal
from uuid import UUID, uuid4

import pytest

from payments.models import Payment
from payments.webhooks import build_payload, sign_payload


@pytest.fixture
def payment() -> Payment:
    return Payment(
        id=UUID("11111111-1111-4111-8111-111111111111"),
        webhook_event_id=UUID("22222222-2222-4222-8222-222222222222"),
        generation=uuid4(),
        amount=Decimal("12.30"),
        currency="RUB",
        status="succeeded",
        description="Кофе ☕",
        metadata_={"z": [True, 1.5], "a": "value"},
        created_at=datetime(2026, 11, 15, 10, 0, tzinfo=UTC),
        processed_at=datetime(2026, 11, 15, 10, 0, 3, tzinfo=UTC),
        webhook_url="https://example.com/?token=SECRET",
        expected_attempt=1,
    )


def test_payload_public_contract(payment: Payment) -> None:
    body = build_payload(payment)
    assert json.loads(body) == {
        "event_id": "22222222-2222-4222-8222-222222222222",
        "payment_id": "11111111-1111-4111-8111-111111111111",
        "status": "succeeded",
        "amount": "12.30",
        "currency": "RUB",
        "description": "Кофе ☕",
        "metadata": {"a": "value", "z": [True, 1.5]},
        "created_at": "2026-11-15T10:00:00+00:00",
        "processed_at": "2026-11-15T10:00:03+00:00",
    }
    assert b"SECRET" not in body


def test_retry_keeps_body_and_event_id(payment: Payment) -> None:
    original = build_payload(payment)
    payment.expected_attempt = 3
    payment.generation = uuid4()
    payment.webhook_status = "exhausted"
    payment.last_error = "webhook_timeout"
    payment.metadata_ = {"a": "value", "z": [True, 1.5]}
    # Reloading TIMESTAMPTZ in a different zone must not change signed bytes.
    payment.created_at = payment.created_at.astimezone(timezone(timedelta(hours=3)))
    assert build_payload(payment) == original


@pytest.mark.parametrize("body", [b'{"x":1}', b'{ "x": 1 }\n', "Кофе ☕".encode()])
def test_hmac_exact_bytes(body: bytes) -> None:
    expected = "v1=" + hmac.new(b"secret", b"1700000000." + body, hashlib.sha256).hexdigest()
    assert sign_payload("secret", "1700000000", body) == expected
    assert sign_payload("secret", "1700000001", body) != expected
    assert sign_payload("other", "1700000000", body) != expected
