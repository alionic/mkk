"""Public payment contracts and deterministic idempotency hashing."""

import hashlib
import json
from decimal import Decimal, localcontext
from typing import Annotated, Any, Literal, Self
from uuid import UUID

from pydantic import (
    AfterValidator,
    AwareDatetime,
    BaseModel,
    BeforeValidator,
    ConfigDict,
    Field,
    JsonValue,
    field_validator,
)
from pydantic.config import ExtraValues

from payments.security import normalize_webhook_url

type Currency = Literal["RUB", "USD", "EUR"]
type PaymentStatus = Literal["pending", "succeeded", "failed"]
type WebhookStatus = Literal["pending", "delivered", "exhausted"]


def _reject_nul(value: str) -> str:
    if "\x00" in value:
        raise ValueError("NUL is not allowed in stored text")
    return value


type NulFreeText = Annotated[str, AfterValidator(_reject_nul)]


def _reject_binary_float(value: object) -> object:
    if isinstance(value, (float, bool)):
        raise ValueError("amount must be an exact decimal string, integer, or Decimal")
    return value


def _two_decimal_places(value: Decimal) -> Decimal:
    exponent = value.as_tuple().exponent
    if not isinstance(exponent, int) or exponent < -2:
        raise ValueError("amount must have at most two decimal places")
    # Never depend on a caller's thread-local Decimal precision.
    with localcontext() as context:
        context.prec = 18
        return value.quantize(Decimal("0.01"))


type Money = Annotated[
    Decimal,
    Field(gt=0, le=Decimal("9999999999999999.99"), max_digits=18, decimal_places=2),
    BeforeValidator(_reject_binary_float),
    AfterValidator(_two_decimal_places),
]


def _canonical_json(value: object) -> bytes:
    return json.dumps(
        value, sort_keys=True, ensure_ascii=False, allow_nan=False, separators=(",", ":")
    ).encode("utf-8")


class Contract(BaseModel):
    model_config = ConfigDict(extra="forbid", hide_input_in_errors=True)


class PaymentCreate(Contract):
    amount: Money
    currency: Currency
    description: NulFreeText = Field(default="", max_length=1024)
    metadata: dict[str, JsonValue] = Field(default_factory=dict, repr=False)
    webhook_url: str = Field(max_length=2048, repr=False)

    @classmethod
    def model_validate_json(
        cls,
        json_data: str | bytes | bytearray,
        *,
        strict: bool | None = None,
        extra: ExtraValues | None = None,
        context: Any | None = None,
        by_alias: bool | None = None,
        by_name: bool | None = None,
    ) -> Self:
        """Preserve the original numeric amount, unlike the float-first JSON parser.

        Use this entrypoint for raw request bodies. Python mappings must supply
        fractional amounts as strings or Decimal: a float has already lost the
        original precision. Metadata retains normal JSON types, including floats.
        """
        try:
            data = json.loads(json_data)
            if isinstance(data, dict) and "amount" in data:
                # A second small parse isolates exact money without changing
                # metadata's standard JSON number representation.
                data["amount"] = json.loads(json_data, parse_float=str)["amount"]
                json_data = json.dumps(data)
        except (ValueError, RecursionError):
            # Delegate malformed/deep JSON to Pydantic's bounded ValidationError path.
            pass
        # Retain JSON-mode semantics, including strict UUID/datetime parsing.
        return super().model_validate_json(
            json_data,
            strict=strict,
            extra=extra,
            context=context,
            by_alias=by_alias,
            by_name=by_name,
        )

    @field_validator("metadata")
    @classmethod
    def validate_metadata(cls, value: dict[str, JsonValue]) -> dict[str, JsonValue]:
        # PostgreSQL JSONB rejects NUL in keys as well as string values. Walk
        # validated JSON iteratively, including strings nested in lists/objects.
        pending: list[JsonValue] = [value]
        while pending:
            item = pending.pop()
            if isinstance(item, str):
                _reject_nul(item)
            elif isinstance(item, dict):
                for key in item:
                    _reject_nul(key)
                pending.extend(item.values())
            elif isinstance(item, list):
                pending.extend(item)
        if len(_canonical_json(value)) > 8192:
            raise ValueError("metadata exceeds 8192 UTF-8 bytes")
        return value

    @field_validator("webhook_url")
    @classmethod
    def validate_url_structure(cls, value: str) -> str:
        # Settings-dependent HTTPS/allowlist policy must also be applied by the
        # application. Structure validation alone must permit explicit dev HTTP.
        return normalize_webhook_url(value, allow_unsafe=True)


class PaymentAccepted(Contract):
    payment_id: UUID
    status: PaymentStatus
    created_at: AwareDatetime


class PaymentRead(PaymentCreate):
    payment_id: UUID
    status: PaymentStatus
    created_at: AwareDatetime
    processed_at: AwareDatetime | None = None
    webhook_status: WebhookStatus
    webhook_delivered_at: AwareDatetime | None = None


class PaymentEvent(Contract):
    """Broker envelope; event_id is independent of the processing generation.

    The caller supplies IDs explicitly. Keep the stable webhook event identity
    when replay creates a new generation; do not put trusted URL/secrets here.
    """

    schema_version: Literal[1]
    event_id: UUID
    payment_id: UUID
    generation: UUID
    attempt: int = Field(ge=1, le=3, strict=True)

    @field_validator("schema_version", mode="before")
    @classmethod
    def require_integer_version(cls, value: object) -> object:
        if type(value) is not int:
            raise ValueError("schema_version must be an integer")
        return value


def canonical_request_hash(body: PaymentCreate) -> str:
    """SHA-256 of compact sorted UTF-8 JSON, with amount always a 2-place string."""
    return hashlib.sha256(_canonical_json(body.model_dump(mode="json"))).hexdigest()
