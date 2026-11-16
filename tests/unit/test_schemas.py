import hashlib
import json
from datetime import UTC, datetime
from decimal import Decimal, localcontext
from typing import Any
from uuid import UUID

import pytest
from pydantic import ValidationError

from payments.schemas import (
    PaymentAccepted,
    PaymentCreate,
    PaymentEvent,
    PaymentRead,
    canonical_request_hash,
)

PAYMENT_ID = UUID("b08d4dd4-70cd-4263-bd92-9defd88724ba")
EVENT_ID = UUID("13432eaf-f22b-4695-9679-b1102a422c8b")
GENERATION = UUID("80186c52-9d31-4166-b110-2b2c9c8ec5ec")
NOW = datetime(2026, 11, 15, tzinfo=UTC)


def payment(**overrides: Any) -> PaymentCreate:
    return PaymentCreate.model_validate(
        {"amount": "1.00", "currency": "RUB", "webhook_url": "https://example.com/hook"} | overrides
    )


@pytest.mark.parametrize("amount", ["0.01", "9999999999999999.99", Decimal("1.0"), 1])
def test_money_validation_accepts_exact_values(amount: object) -> None:
    body = payment(amount=amount)
    assert body.amount == Decimal(str(amount))
    assert body.model_dump(mode="json")["amount"] == format(Decimal(str(amount)), ".2f")


@pytest.mark.parametrize(
    "amount",
    ["0", "-1", "NaN", "Infinity", "-Infinity", "0.001", "1.000", "10000000000000000", True, None],
)
def test_money_validation_rejects_invalid_values(amount: object) -> None:
    with pytest.raises(ValueError):
        payment(amount=amount)


@pytest.mark.parametrize("amount", [0.01, 1.0, 9999999999999999.99, float("nan")])
def test_binary_float_amount_is_rejected_before_decimal_conversion(amount: float) -> None:
    with pytest.raises(ValueError):
        payment(amount=amount)


@pytest.mark.parametrize("raw", ["0.01", "9999999999999999.99", "1.00"])
def test_json_numeric_amount_is_parsed_without_float_rounding(raw: str) -> None:
    body = PaymentCreate.model_validate_json(
        '{"amount":' + raw + ',"currency":"RUB","webhook_url":"https://example.com"}'
    )
    assert body.amount == Decimal(raw)
    assert body.model_dump(mode="json")["amount"] == format(Decimal(raw), ".2f")


@pytest.mark.parametrize("raw", ["0.001", "1.000", "NaN", "Infinity", "1e" + "9" * 40])
def test_json_numeric_invalid_amount_is_a_validation_error(raw: str) -> None:
    with pytest.raises(ValueError):
        PaymentCreate.model_validate_json(
            '{"amount":' + raw + ',"currency":"RUB","webhook_url":"https://example.com"}'
        )


def test_json_metadata_keeps_number_types_and_hash_equivalence() -> None:
    body = PaymentCreate.model_validate_json(
        '{"amount":1.00,"currency":"RUB","webhook_url":"https://example.com/hook",'
        '"metadata":{"x":1.5,"flag":true,"nested":[2,null]}}'
    )
    assert body.metadata == {"x": 1.5, "flag": True, "nested": [2, None]}
    assert canonical_request_hash(body) == canonical_request_hash(payment(metadata=body.metadata))


def test_money_normalization_is_independent_of_decimal_context() -> None:
    with localcontext() as context:
        context.prec = 6
        assert payment(amount="9999999999999999.99").model_dump(mode="json")["amount"] == (
            "9999999999999999.99"
        )


def test_canonical_hash() -> None:
    first = payment(amount="1.0", metadata={"z": [1, {"b": 2, "a": 3}], "a": "я"})
    second = payment(amount="1.00", metadata={"a": "я", "z": [1, {"a": 3, "b": 2}]})
    expected = (
        '{"amount":"1.00","currency":"RUB","description":"",'
        '"metadata":{"a":"я","z":[1,{"a":3,"b":2}]},'
        '"webhook_url":"https://example.com/hook"}'
    )
    assert canonical_request_hash(first) == hashlib.sha256(expected.encode()).hexdigest()
    assert canonical_request_hash(first) == canonical_request_hash(second)


@pytest.mark.parametrize(
    "change",
    [
        {"amount": "1.01"},
        {"currency": "USD"},
        {"description": "another"},
        {"metadata": {"x": 1}},
        {"webhook_url": "https://example.com/hook?token=other"},
    ],
)
def test_hash_includes_every_request_field(change: dict[str, object]) -> None:
    assert canonical_request_hash(payment()) != canonical_request_hash(payment(**change))


@pytest.mark.parametrize("size,valid", [(8192, True), (8193, False)])
def test_metadata_size(size: int, valid: bool) -> None:
    metadata = {"x": "a" * (size - 8)}
    assert len(json.dumps(metadata, separators=(",", ":")).encode()) == size
    if valid:
        assert payment(metadata=metadata).metadata == metadata
    else:
        with pytest.raises(ValueError):
            payment(metadata=metadata)


def test_metadata_size_counts_utf8_bytes() -> None:
    assert payment(metadata={"x": "я" * 4092}).metadata == {"x": "я" * 4092}
    with pytest.raises(ValueError):
        payment(metadata={"x": "я" * 4093})


@pytest.mark.parametrize(
    "metadata", [[], "text", None, {"x": float("nan")}, {"x": float("inf")}, {1: "x"}]
)
def test_metadata_is_a_finite_json_object(metadata: object) -> None:
    with pytest.raises(ValueError):
        payment(metadata=metadata)


@pytest.mark.parametrize("change", [{"currency": "rub"}, {"currency": "GBP"}, {"extra": 1}])
def test_schema_rejects_unknown_fields_and_currencies(change: dict[str, object]) -> None:
    with pytest.raises(ValueError):
        payment(**change)


def test_description_and_url_boundaries_and_defaults() -> None:
    assert payment().description == ""
    assert payment().metadata == {}
    assert payment(description="я" * 1024).description == "я" * 1024
    assert len(payment(webhook_url="https://example.com/" + "a" * 2028).webhook_url) == 2048
    for change in ({"description": "x" * 1025}, {"webhook_url": "x" * 2049}):
        with pytest.raises(ValueError):
            payment(**change)


def test_response_contracts_preserve_money_and_delivery_status_separately() -> None:
    accepted = PaymentAccepted(payment_id=PAYMENT_ID, status="pending", created_at=NOW)
    assert accepted.model_dump(mode="json") == {
        "payment_id": str(PAYMENT_ID),
        "status": "pending",
        "created_at": "2026-11-15T00:00:00Z",
    }
    read = PaymentRead.model_validate(
        payment().model_dump()
        | {
            "payment_id": PAYMENT_ID,
            "status": "succeeded",
            "created_at": NOW,
            "processed_at": NOW,
            "webhook_status": "exhausted",
            "webhook_delivered_at": None,
        }
    )
    assert read.status == "succeeded"
    assert read.webhook_status == "exhausted"
    assert read.model_dump(mode="json")["amount"] == "1.00"
    assert "request_hash" not in read.model_dump()


def test_response_and_event_unknown_fields_are_forbidden() -> None:
    for model, data in (
        (PaymentAccepted, {"payment_id": PAYMENT_ID, "status": "pending", "created_at": NOW}),
        (
            PaymentEvent,
            {
                "payment_id": PAYMENT_ID,
                "event_id": EVENT_ID,
                "generation": GENERATION,
                "attempt": 1,
                "schema_version": 1,
            },
        ),
    ):
        with pytest.raises(ValueError):
            model.model_validate(data | {"webhook_url": "https://example.com"})


def test_event_requires_generation_attempt_and_version() -> None:
    envelope = {
        "schema_version": 1,
        "event_id": EVENT_ID,
        "payment_id": PAYMENT_ID,
        "generation": GENERATION,
        "attempt": 1,
    }
    assert (
        PaymentEvent.model_validate_json(json.dumps(envelope, default=str)).model_dump() == envelope
    )
    for required in envelope:
        with pytest.raises(ValueError):
            PaymentEvent.model_validate(
                {key: value for key, value in envelope.items() if key != required}
            )


@pytest.mark.parametrize("attempt", [0, 4, True, "1", 1.5])
def test_event_rejects_invalid_attempt(attempt: object) -> None:
    with pytest.raises(ValueError):
        PaymentEvent.model_validate(
            {
                "schema_version": 1,
                "event_id": EVENT_ID,
                "payment_id": PAYMENT_ID,
                "generation": GENERATION,
                "attempt": attempt,
            }
        )


@pytest.mark.parametrize("version", [0, 2, True, "1"])
def test_event_rejects_unsupported_version(version: object) -> None:
    with pytest.raises(ValueError):
        PaymentEvent.model_validate(
            {
                "schema_version": version,
                "event_id": EVENT_ID,
                "payment_id": PAYMENT_ID,
                "generation": GENERATION,
                "attempt": 1,
            }
        )


def test_event_identity_does_not_derive_from_generation() -> None:
    first = PaymentEvent(
        event_id=EVENT_ID, payment_id=PAYMENT_ID, generation=GENERATION, attempt=1, schema_version=1
    )
    replay = PaymentEvent(
        event_id=EVENT_ID, payment_id=PAYMENT_ID, generation=PAYMENT_ID, attempt=1, schema_version=1
    )
    assert replay.event_id == first.event_id
    assert replay.generation != first.generation


def test_naive_response_datetime_is_rejected() -> None:
    with pytest.raises(ValueError):
        PaymentAccepted(payment_id=PAYMENT_ID, status="pending", created_at=datetime(2026, 1, 1))


def test_invalid_json_is_a_validation_error_without_url_query() -> None:
    with pytest.raises(ValueError):
        PaymentCreate.model_validate_json("{")
    with pytest.raises(ValueError) as error:
        payment(webhook_url="https://user:pass@example.com/hook?token=DO_NOT_LOG")
    assert "DO_NOT_LOG" not in str(error.value)


def test_model_diagnostics_hide_url_queries_and_metadata() -> None:
    body = payment(
        webhook_url="https://example.com/hook?token=PRIVATE_QUERY",
        metadata={"secret": "PRIVATE_METADATA"},
    )
    assert "PRIVATE_QUERY" not in repr(body)
    assert "PRIVATE_METADATA" not in repr(body)
    assert body.model_dump(mode="json")["metadata"] == {"secret": "PRIVATE_METADATA"}


def test_deeply_nested_json_is_a_validation_error_not_parser_recursion_error() -> None:
    raw = (
        '{"amount":1.00,"currency":"RUB","webhook_url":"https://example.com",'
        '"metadata":{"x":' + "[" * 10000 + "null" + "]" * 10000 + "}}"
    )
    with pytest.raises(ValueError):
        PaymentCreate.model_validate_json(raw)


@pytest.mark.parametrize(
    "change",
    [
        {"description": "PRIVATE_NUL\x00description"},
        {"metadata": {"PRIVATE_NUL\x00key": 1}},
        {"metadata": {"key": "PRIVATE_NUL\x00value"}},
        {"metadata": {"outer": [{"PRIVATE_NUL\x00key": True}]}},
        {"metadata": {"outer": {"inner": "PRIVATE_NUL\x00value"}}},
        {"metadata": {"outer": [None, 1, ["PRIVATE_NUL\x00list"]]}},
    ],
)
@pytest.mark.parametrize("raw_json", [False, True])
def test_postgres_incompatible_nul_is_rejected_before_hashing(
    change: dict[str, Any], raw_json: bool
) -> None:
    data = {"amount": "1.00", "currency": "RUB", "webhook_url": "https://example.com"} | change
    with pytest.raises(ValidationError) as error:
        if raw_json:
            PaymentCreate.model_validate_json(json.dumps(data))
        else:
            PaymentCreate.model_validate(data)
    assert "PRIVATE_NUL" not in str(error.value)


def test_literal_backslash_u0000_is_preserved_not_stripped() -> None:
    value = r"literal\u0000 is not a NUL"
    body = payment(description=value, metadata={value: [value]})
    assert body.description == value
    assert body.metadata == {value: [value]}
