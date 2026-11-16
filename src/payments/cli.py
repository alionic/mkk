"""Offline OpenAPI export and atomic, generation-fenced operator replay."""

import argparse
import asyncio
import json
import logging
from collections.abc import Sequence
from pathlib import Path
from typing import Never
from uuid import UUID, uuid4

from sqlalchemy import select

from payments.api import create_app
from payments.config import Settings
from payments.db import Database
from payments.health import DEFAULT_HEARTBEAT, healthy
from payments.logging import configure_logging
from payments.models import Outbox, Payment
from payments.schemas import PaymentEvent

logger = logging.getLogger(__name__)


class SafeArgumentParser(argparse.ArgumentParser):
    def error(self, message: str) -> Never:
        self.exit(2, "invalid command\n")


class ReplayRejected(Exception):
    """Only an exhausted webhook for a terminal payment can be replayed."""


async def replay_webhook(database: Database, payment_id: UUID) -> PaymentEvent:
    async with database.session_factory.begin() as session:
        payment = await session.scalar(
            select(Payment).where(Payment.id == payment_id).with_for_update()
        )
        if payment is None or payment.webhook_status != "exhausted" or payment.status == "pending":
            raise ReplayRejected("replay_rejected")
        payment.generation = uuid4()
        payment.expected_attempt = 1
        payment.webhook_status = "pending"
        payment.webhook_delivered_at = None
        payment.last_error = None
        event = PaymentEvent(
            schema_version=1,
            event_id=payment.webhook_event_id,
            payment_id=payment.id,
            generation=payment.generation,
            attempt=1,
        )
        # A fresh main-queue envelope, never the extended DLQ diagnostics payload.
        session.add(
            Outbox(
                payment_id=payment.id,
                generation=payment.generation,
                event_type="payments.new",
                destination="payments.new",
                schema_version=1,
                attempt=1,
                payload=event.model_dump(mode="json"),
            )
        )
    return event  # Only after the COMMIT, including deferred constraints.


async def export_schema() -> dict[str, object]:
    # Deliberately ignore runtime environment, including invalid/missing secrets.
    # Engine construction is lazy; app.openapi() does not enter its lifespan.
    config = Settings(
        database_url="postgresql+asyncpg://offline:offline@127.0.0.1:1/offline",
        rabbitmq_url="amqp://offline:offline@127.0.0.1:1/",
        api_key="offline",
        webhook_secret="offline",
        webhook_allowed_domains=["example.com"],
        environment="production",
        allow_unsafe_webhooks=False,
        retry_base_seconds=2,
    )
    app = create_app(config)
    try:
        return dict(app.openapi())
    finally:
        await app.state.database.dispose()


async def _replay(payment_id: UUID) -> None:
    database = Database(Settings().database_url)
    try:
        event = await replay_webhook(database, payment_id)
        logger.info(
            "replay_created", extra={"payment_id": event.payment_id, "event_id": event.event_id}
        )
    finally:
        await database.dispose()


def main(argv: Sequence[str] | None = None) -> int:
    parser = SafeArgumentParser(prog="payments")
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("export-schema")
    health = commands.add_parser("worker-health")
    health.add_argument("--path", type=Path, default=DEFAULT_HEARTBEAT)
    replay = commands.add_parser("replay-webhook")
    replay.add_argument("payment_id")
    args = parser.parse_args(argv)
    if args.command == "worker-health":
        return 0 if healthy(args.path) else 1
    configure_logging()
    try:
        if args.command == "export-schema":
            print(json.dumps(asyncio.run(export_schema()), ensure_ascii=True))
        else:
            # argparse's UUID type error would echo arbitrary input (possibly a URL).
            try:
                payment_id = UUID(args.payment_id)
            except ValueError:
                raise ReplayRejected("replay_rejected") from None
            asyncio.run(_replay(payment_id))
    except ReplayRejected:
        logger.warning("replay_rejected")
        return 1
    except Exception:
        logger.error("command_failed")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
