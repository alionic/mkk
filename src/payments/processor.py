"""Payment state machine, without broker acknowledgements or HTTP implementation.

The caller must serialize processing (SAC + prefetch=1). Row locks protect each
state transition, not the network call: a committed gateway result is reused on
redelivery. HTTP success followed by a failed commit may deliver the same stable
webhook_event_id again. This is deliberately at-least-once, not exactly-once.
"""

import math
from datetime import timedelta
from typing import Literal, Protocol

from pydantic import ValidationError
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from payments.db import Database
from payments.gateway import Gateway
from payments.models import Outbox, Payment, utc_now
from payments.schemas import PaymentEvent

type InvalidEventReason = Literal[
    "invalid_envelope", "missing_payment", "event_id_mismatch", "unexpected_attempt"
]


class InvalidPaymentEvent(Exception):
    """Permanent envelope failure: the worker should reject to DLX, not retry."""

    def __init__(self, reason: InvalidEventReason = "invalid_envelope") -> None:
        self.reason = reason
        super().__init__(reason)


class DeliveryError(Exception):
    """Expected transport failure; only allowlisted categories cross into storage.

    HTTP senders import this exception and WebhookSender from this module. Never
    pass raw response bodies/URLs as diagnostics; unknown codes are sanitized too.
    Other exceptions (including DB failures and cancellation) must propagate.
    """

    def __init__(self, code: str = "webhook_failed") -> None:
        self.code = (
            code
            if code
            in {
                "webhook_timeout",
                "webhook_http_error",
                "webhook_network_error",
                "webhook_url_rejected",
                "webhook_failed",
            }
            else "webhook_failed"
        )
        super().__init__(self.code)


class WebhookSender(Protocol):
    async def send(self, payment: Payment) -> None:
        """Deliver a terminal payment, raising DeliveryError for transport failures."""
        ...


async def _current_payment(session: AsyncSession, event: PaymentEvent) -> Payment | None:
    """Caller owns this transaction and must retain the lock until its commit."""
    payment = await session.scalar(
        select(Payment).where(Payment.id == event.payment_id).with_for_update()
    )
    if payment is None:
        raise InvalidPaymentEvent("missing_payment")
    # Old/finished flows are no-ops before charging, sending or changing any state.
    if (
        payment.generation != event.generation
        or event.attempt < payment.expected_attempt
        or payment.webhook_status in {"delivered", "exhausted"}
    ):
        return None
    if payment.webhook_event_id != event.event_id:
        raise InvalidPaymentEvent("event_id_mismatch")
    if event.attempt != payment.expected_attempt:
        raise InvalidPaymentEvent("unexpected_attempt")
    return payment


class PaymentProcessor:
    def __init__(
        self,
        database: Database,
        gateway: Gateway,
        sender: WebhookSender,
        *,
        retry_base: float = 2,
    ) -> None:
        if not math.isfinite(retry_base) or retry_base <= 0:
            raise ValueError("retry_base must be finite and positive")
        self.database = database
        self.gateway = gateway
        self.sender = sender
        self.retry_base = retry_base

    async def process(self, event: PaymentEvent) -> None:
        # Revalidate even model_copy/model_construct instances, without exposing
        # Pydantic input/context or permitting untrusted URLs in broker envelopes.
        if not isinstance(event, PaymentEvent):
            raise InvalidPaymentEvent
        try:
            event = PaymentEvent.model_validate(vars(event))
        except ValidationError:
            raise InvalidPaymentEvent from None

        async with self.database.session_factory.begin() as session:
            payment = await _current_payment(session, event)
            if payment is None:
                return
            if payment.status == "pending":
                # Only the emulator's bounded 2..5s delay holds this row lock.
                payment.status = await self.gateway.process(payment.id)
                payment.processed_at = utc_now()
        # COMMIT has completed, session/lock released, all sender fields loaded.
        delivery_error: str | None = None
        try:
            await self.sender.send(payment)
        except DeliveryError as error:
            delivery_error = error.code

        # Only the network call above is caught. Flush/COMMIT errors below escape
        # to the worker and must never result in an ack or consume an attempt.
        async with self.database.session_factory.begin() as session:
            current = await _current_payment(session, event)
            if current is None:
                return  # Replay/another flow changed generation while HTTP was in flight.
            now = utc_now()
            current.last_error = delivery_error
            if delivery_error is None:
                current.webhook_status = "delivered"
                current.webhook_delivered_at = now
                return

            if event.attempt < 3:
                current.expected_attempt = event.attempt + 1
                retry = PaymentEvent(
                    schema_version=1,
                    event_id=current.webhook_event_id,
                    payment_id=current.id,
                    generation=current.generation,
                    attempt=current.expected_attempt,
                )
                session.add(
                    Outbox(
                        payment_id=current.id,
                        generation=current.generation,
                        event_type="payments.new",
                        schema_version=1,
                        attempt=retry.attempt,
                        payload=retry.model_dump(mode="json"),
                        destination="payments.new",
                        available_at=now + timedelta(seconds=self.retry_base * event.attempt),
                    )
                )
            else:
                current.webhook_status = "exhausted"
                session.add(
                    Outbox(
                        payment_id=current.id,
                        generation=current.generation,
                        event_type="payments.dlq",
                        schema_version=1,
                        attempt=event.attempt,
                        payload={
                            **event.model_dump(mode="json"),
                            "diagnostics": {
                                "reason": "webhook_attempts_exhausted",
                                "delivery_error": delivery_error,
                            },
                        },
                        destination="payments.dlq",
                        available_at=now,
                    )
                )
