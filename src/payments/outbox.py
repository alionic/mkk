"""At-least-once Outbox relay; no payment/webhook business logic lives here."""

import asyncio
import logging
import math
from contextlib import suppress
from datetime import UTC, datetime, timedelta
from typing import Protocol
from uuid import UUID

from sqlalchemy import func, select
from sqlalchemy.exc import SQLAlchemyError

from payments.db import Database
from payments.messaging import PublishError
from payments.models import Outbox

logger = logging.getLogger(__name__)


class EventPublisher(Protocol):
    async def start(self) -> None: ...

    async def close(self) -> None: ...

    async def publish_event(
        self, event: dict[str, object], destination: str, message_id: UUID
    ) -> None: ...


def retry_delay(attempt: int, *, base_seconds: float, max_seconds: float) -> float:
    """Cap before exponentiation: even a long broker outage cannot overflow."""
    exponent = max(attempt - 1, 0)
    if exponent >= math.log2(max_seconds) - math.log2(base_seconds):
        return max_seconds
    return min(max_seconds, math.ldexp(base_seconds, exponent))


class OutboxRelay:
    def __init__(
        self,
        database: Database,
        messaging: EventPublisher,
        *,
        batch_size: int = 100,
        poll_interval: float = 1,
        publish_timeout: float = 5,
        retry_base: float = 1,
        retry_cap: float = 60,
        transaction_timeout: float = 35,
    ) -> None:
        if batch_size < 1:
            raise ValueError("batch_size must be positive")
        for value in (poll_interval, publish_timeout, retry_base, retry_cap, transaction_timeout):
            if not math.isfinite(value) or value <= 0:
                raise ValueError("relay intervals must be finite and positive")
        if retry_cap < retry_base:
            raise ValueError("retry_cap must be at least retry_base")
        self.database = database
        self.messaging = messaging
        self.batch_size = batch_size
        self.poll_interval = poll_interval
        self.publish_timeout = publish_timeout
        self.retry_base = retry_base
        self.retry_cap = retry_cap
        self.transaction_timeout = transaction_timeout

    async def run_once(self) -> int:
        """Return committed successes; publish/commit ambiguity permits duplicates.

        A locked row has its own transaction, held through publisher confirm.
        DB failures (especially COMMIT failure after confirm) propagate, never
        get misclassified as a publish failure, and never return false success.
        """
        published = 0
        for _ in range(self.batch_size):
            succeeded = False
            async with (
                asyncio.timeout(self.transaction_timeout),
                self.database.session_factory.begin() as session,
            ):
                row = await session.scalar(
                    select(Outbox)
                    .where(Outbox.published_at.is_(None), Outbox.available_at <= func.now())
                    .order_by(Outbox.available_at, Outbox.created_at, Outbox.id)
                    .limit(1)
                    .with_for_update(skip_locked=True)
                )
                if row is None:
                    break
                row.publish_attempts += 1
                try:
                    async with asyncio.timeout(self.publish_timeout):
                        await self.messaging.publish_event(
                            dict(row.payload), row.destination, row.id
                        )
                except Exception as error:
                    # Transport errors are never persisted as arbitrary text/URLs.
                    category = "publish_failed"
                    if isinstance(error, TimeoutError):
                        category = "publish_timeout"
                    elif isinstance(error, PublishError) and str(error) in {
                        "publish_timeout",
                        "publish_unroutable",
                        "publish_not_confirmed",
                    }:
                        category = str(error)
                    row.last_error = category
                    row.available_at = datetime.now(UTC) + timedelta(
                        seconds=retry_delay(
                            row.publish_attempts,
                            base_seconds=self.retry_base,
                            max_seconds=self.retry_cap,
                        )
                    )
                else:
                    row.published_at = datetime.now(UTC)
                    row.last_error = None
                    succeeded = True
            # This line is reached only after the transaction COMMIT succeeded.
            published += int(succeeded)
        return published

    async def run(self, stop: asyncio.Event, *, manage_lifecycle: bool = True) -> None:
        """Run batches; standalone owns transport, worker explicitly opts out."""
        try:
            if manage_lifecycle:
                await self.messaging.start()
            while not stop.is_set():
                try:
                    count = await self.run_once()
                except (SQLAlchemyError, OSError, TimeoutError):
                    logger.warning("outbox_iteration_failed")  # No exception text/DSN.
                    count = 0
                if count < self.batch_size and not stop.is_set():
                    with suppress(TimeoutError):
                        await asyncio.wait_for(stop.wait(), timeout=self.poll_interval)
        finally:
            if manage_lifecycle:
                await self.messaging.close()
