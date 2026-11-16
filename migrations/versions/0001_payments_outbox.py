"""Initial payments and generation-aware Outbox.

Revision ID: 0001
Revises:
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0001"
down_revision: str | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "payments",
        sa.Column("id", sa.Uuid(), primary_key=True),
        sa.Column("amount", sa.Numeric(18, 2), nullable=False),
        sa.Column("currency", sa.String(3), nullable=False),
        sa.Column("description", sa.String(1024), nullable=False, server_default=""),
        sa.Column(
            "metadata", postgresql.JSONB(), nullable=False, server_default=sa.text("'{}'::jsonb")
        ),
        sa.Column("status", sa.String(16), nullable=False, server_default="pending"),
        sa.Column("idempotency_key", sa.String(128), nullable=False),
        sa.Column("request_hash", sa.String(64), nullable=False),
        sa.Column("webhook_url", sa.String(2048), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.Column("processed_at", sa.DateTime(timezone=True)),
        sa.Column("webhook_event_id", sa.Uuid(), nullable=False),
        sa.Column("webhook_status", sa.String(16), nullable=False, server_default="pending"),
        sa.Column("webhook_delivered_at", sa.DateTime(timezone=True)),
        sa.Column("expected_attempt", sa.Integer(), nullable=False, server_default="1"),
        sa.Column("generation", sa.Uuid(), nullable=False),
        sa.Column("last_error", sa.String(128)),
        sa.UniqueConstraint("idempotency_key", name="uq_payments_idempotency_key"),
        sa.CheckConstraint(
            "amount > 0 AND amount <= 9999999999999999.99", name="ck_payments_amount_positive"
        ),
        sa.CheckConstraint("currency IN ('RUB', 'USD', 'EUR')", name="ck_payments_currency"),
        sa.CheckConstraint(
            "status IN ('pending', 'succeeded', 'failed')", name="ck_payments_status"
        ),
        sa.CheckConstraint(
            "(status = 'pending' AND processed_at IS NULL) OR "
            "(status IN ('succeeded', 'failed') AND processed_at IS NOT NULL)",
            name="ck_payments_processed_at",
        ),
        sa.CheckConstraint(
            "webhook_status IN ('pending', 'delivered', 'exhausted')",
            name="ck_payments_webhook_status",
        ),
        sa.CheckConstraint(
            "(webhook_status = 'delivered') = (webhook_delivered_at IS NOT NULL)",
            name="ck_payments_webhook_delivered_at",
        ),
        sa.CheckConstraint("expected_attempt BETWEEN 1 AND 3", name="ck_payments_expected_attempt"),
        sa.CheckConstraint(
            "length(idempotency_key) BETWEEN 1 AND 128", name="ck_payments_idempotency_key"
        ),
        sa.CheckConstraint("request_hash ~ '^[0-9a-f]{64}$'", name="ck_payments_request_hash"),
        sa.CheckConstraint("jsonb_typeof(metadata) = 'object'", name="ck_payments_metadata_object"),
    )
    op.create_table(
        "outbox",
        sa.Column("id", sa.Uuid(), primary_key=True),
        sa.Column("payment_id", sa.Uuid(), sa.ForeignKey("payments.id"), nullable=False),
        sa.Column("generation", sa.Uuid(), nullable=False),
        sa.Column("event_type", sa.String(128), nullable=False),
        sa.Column("schema_version", sa.Integer(), nullable=False),
        sa.Column("attempt", sa.Integer(), nullable=False),
        sa.Column("payload", postgresql.JSONB(), nullable=False),
        sa.Column("destination", sa.String(128), nullable=False),
        sa.Column(
            "available_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.Column("published_at", sa.DateTime(timezone=True)),
        sa.Column("publish_attempts", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("last_error", sa.String(128)),
        sa.UniqueConstraint(
            "payment_id", "generation", "event_type", "attempt", name="uq_outbox_logical_event"
        ),
        sa.CheckConstraint("attempt BETWEEN 1 AND 3", name="ck_outbox_attempt"),
        sa.CheckConstraint("schema_version = 1", name="ck_outbox_schema_version"),
        sa.CheckConstraint("publish_attempts >= 0", name="ck_outbox_publish_attempts"),
        sa.CheckConstraint("jsonb_typeof(payload) = 'object'", name="ck_outbox_payload_object"),
    )
    op.create_index(
        "ix_outbox_unpublished_ready",
        "outbox",
        ["available_at", "created_at"],
        postgresql_where=sa.text("published_at IS NULL"),
    )


def downgrade() -> None:
    op.drop_index("ix_outbox_unpublished_ready", table_name="outbox")
    op.drop_table("outbox")
    op.drop_table("payments")
