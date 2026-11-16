"""Payment state and durable, generation-aware transactional Outbox."""

from datetime import UTC, datetime
from decimal import Decimal
from uuid import UUID, uuid4

from pydantic import JsonValue
from sqlalchemy import (
    CheckConstraint,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    Numeric,
    String,
    UniqueConstraint,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from payments.schemas import Currency, PaymentStatus, WebhookStatus


def utc_now() -> datetime:
    return datetime.now(UTC)


class Base(DeclarativeBase):
    pass


class Payment(Base):
    __tablename__ = "payments"
    __table_args__ = (
        UniqueConstraint("idempotency_key", name="uq_payments_idempotency_key"),
        # PostgreSQL sorts numeric NaN above finite numbers; > 0 alone admits it.
        CheckConstraint(
            "amount > 0 AND amount <= 9999999999999999.99", name="ck_payments_amount_positive"
        ),
        CheckConstraint("currency IN ('RUB', 'USD', 'EUR')", name="ck_payments_currency"),
        CheckConstraint("status IN ('pending', 'succeeded', 'failed')", name="ck_payments_status"),
        CheckConstraint(
            "(status = 'pending' AND processed_at IS NULL) OR "
            "(status IN ('succeeded', 'failed') AND processed_at IS NOT NULL)",
            name="ck_payments_processed_at",
        ),
        CheckConstraint(
            "webhook_status IN ('pending', 'delivered', 'exhausted')",
            name="ck_payments_webhook_status",
        ),
        CheckConstraint(
            "(webhook_status = 'delivered') = (webhook_delivered_at IS NOT NULL)",
            name="ck_payments_webhook_delivered_at",
        ),
        CheckConstraint("expected_attempt BETWEEN 1 AND 3", name="ck_payments_expected_attempt"),
        CheckConstraint(
            "length(idempotency_key) BETWEEN 1 AND 128", name="ck_payments_idempotency_key"
        ),
        CheckConstraint("request_hash ~ '^[0-9a-f]{64}$'", name="ck_payments_request_hash"),
        CheckConstraint("jsonb_typeof(metadata) = 'object'", name="ck_payments_metadata_object"),
    )

    id: Mapped[UUID] = mapped_column(primary_key=True, default=uuid4)
    amount: Mapped[Decimal] = mapped_column(Numeric(18, 2))
    currency: Mapped[Currency] = mapped_column(String(3))
    description: Mapped[str] = mapped_column(String(1024), default="", server_default="")
    metadata_: Mapped[dict[str, JsonValue]] = mapped_column(
        "metadata", JSONB, default=dict, server_default=text("'{}'::jsonb")
    )
    status: Mapped[PaymentStatus] = mapped_column(
        String(16), default="pending", server_default="pending"
    )
    idempotency_key: Mapped[str] = mapped_column(String(128))
    request_hash: Mapped[str] = mapped_column(String(64))
    webhook_url: Mapped[str] = mapped_column(String(2048))
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utc_now, server_default=text("now()")
    )
    processed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    webhook_event_id: Mapped[UUID] = mapped_column(default=uuid4)
    webhook_status: Mapped[WebhookStatus] = mapped_column(
        String(16), default="pending", server_default="pending"
    )
    webhook_delivered_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    expected_attempt: Mapped[int] = mapped_column(Integer, default=1, server_default="1")
    generation: Mapped[UUID] = mapped_column(default=uuid4)
    # Safe category only, never a raw exception/URL/response body.
    last_error: Mapped[str | None] = mapped_column(String(128))


class Outbox(Base):
    __tablename__ = "outbox"
    __table_args__ = (
        UniqueConstraint(
            "payment_id", "generation", "event_type", "attempt", name="uq_outbox_logical_event"
        ),
        CheckConstraint("attempt BETWEEN 1 AND 3", name="ck_outbox_attempt"),
        CheckConstraint("schema_version = 1", name="ck_outbox_schema_version"),
        CheckConstraint("publish_attempts >= 0", name="ck_outbox_publish_attempts"),
        CheckConstraint("jsonb_typeof(payload) = 'object'", name="ck_outbox_payload_object"),
        Index(
            "ix_outbox_unpublished_ready",
            "available_at",
            "created_at",
            postgresql_where=text("published_at IS NULL"),
        ),
    )

    id: Mapped[UUID] = mapped_column(primary_key=True, default=uuid4)
    payment_id: Mapped[UUID] = mapped_column(ForeignKey("payments.id"))
    generation: Mapped[UUID]
    event_type: Mapped[str] = mapped_column(String(128))
    schema_version: Mapped[int] = mapped_column(Integer)
    attempt: Mapped[int] = mapped_column(Integer)
    payload: Mapped[dict[str, JsonValue]] = mapped_column(JSONB)
    destination: Mapped[str] = mapped_column(String(128))
    available_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utc_now, server_default=text("now()")
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utc_now, server_default=text("now()")
    )
    published_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    publish_attempts: Mapped[int] = mapped_column(Integer, default=0, server_default="0")
    last_error: Mapped[str | None] = mapped_column(String(128))
