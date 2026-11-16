"""Payment creation; callers own the transaction, including its commit/rollback."""

from uuid import UUID, uuid4

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from payments.models import Outbox, Payment
from payments.schemas import PaymentCreate, PaymentEvent, canonical_request_hash


class IdempotencyConflict(Exception):
    """The key already names a different normalized request (no sensitive details)."""


async def create_payment(session: AsyncSession, body: PaymentCreate, key: str) -> Payment:
    """Persist payment + initial event atomically within a READ COMMITTED transaction.

    HTTP callers must first apply settings-dependent validate_webhook_url policy.
    No broker I/O. A conflicting INSERT waits on the unique index; the next SELECT
    has a fresh READ COMMITTED snapshot and sees the committed winning row.
    """
    request_hash = canonical_request_hash(body)
    statement = (
        insert(Payment)
        .values(
            id=uuid4(),
            amount=body.amount,
            currency=body.currency,
            description=body.description,
            metadata_=body.metadata,
            webhook_url=body.webhook_url,
            idempotency_key=key,
            request_hash=request_hash,
            generation=uuid4(),
            webhook_event_id=uuid4(),
        )
        .on_conflict_do_nothing(constraint="uq_payments_idempotency_key")
        .returning(Payment)
    )
    payment = (await session.scalars(statement)).one_or_none()
    if payment is None:
        payment = (
            await session.scalars(select(Payment).where(Payment.idempotency_key == key))
        ).one()
        if payment.request_hash != request_hash:
            raise IdempotencyConflict
        return payment

    envelope = PaymentEvent(
        schema_version=1,
        event_id=payment.webhook_event_id,
        payment_id=payment.id,
        generation=payment.generation,
        attempt=1,
    )
    session.add(
        Outbox(
            payment_id=payment.id,
            generation=payment.generation,
            event_type="payments.new",
            schema_version=1,
            attempt=1,
            payload=envelope.model_dump(mode="json"),
            destination="payments.new",
        )
    )
    await session.flush()
    return payment


async def get_payment(session: AsyncSession, payment_id: UUID) -> Payment | None:
    return await session.get(Payment, payment_id)
