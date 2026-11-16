"""One manual-ack consumer, a supervised relay, and bounded signal-aware shutdown."""

import asyncio
import logging
import math
import signal
from collections.abc import Awaitable
from pathlib import Path
from time import monotonic

from aiormq.exceptions import AMQPError
from faststream import AckPolicy
from faststream.rabbit import RabbitMessage
from faststream.rabbit.message import RabbitMessage as RawRabbitMessage
from pydantic import ValidationError
from sqlalchemy.exc import SQLAlchemyError

from payments.config import Settings
from payments.db import Database
from payments.gateway import Gateway
from payments.health import DEFAULT_HEARTBEAT, write_heartbeat
from payments.logging import configure_logging
from payments.messaging import Messaging
from payments.outbox import OutboxRelay, retry_delay
from payments.processor import InvalidPaymentEvent, PaymentProcessor
from payments.schemas import PaymentEvent
from payments.webhooks import WebhookClient

logger = logging.getLogger(__name__)


async def raw_body(message: RawRabbitMessage) -> bytes:
    # FastStream normally JSON-decodes before dependency injection. Bypass it so
    # malformed JSON reaches our explicit reject path, not framework middleware.
    return bytes(message.body)


class Worker:
    def __init__(
        self,
        settings: Settings,
        *,
        database: Database | None = None,
        messaging: Messaging | None = None,
        sender: WebhookClient | None = None,
        processor: PaymentProcessor | None = None,
        drain_timeout: float = 90,
        cleanup_timeout: float = 5,
        processing_timeout: float = 80,
        backoff_base: float = 1,
        backoff_cap: float = 30,
        heartbeat_interval: float = 10,
        heartbeat_path: Path | None = None,
    ) -> None:
        for interval in (
            drain_timeout,
            cleanup_timeout,
            processing_timeout,
            backoff_base,
            backoff_cap,
            heartbeat_interval,
        ):
            if not math.isfinite(interval) or interval <= 0:
                raise ValueError("worker intervals must be finite and positive")
        if backoff_cap < backoff_base:
            raise ValueError("backoff cap must be at least base")
        self.database = database if database is not None else Database(settings.database_url)
        self.messaging = messaging if messaging is not None else Messaging(settings.rabbitmq_url)
        self.sender = sender if sender is not None else WebhookClient(settings)
        self.processor = (
            processor
            if processor is not None
            else PaymentProcessor(
                self.database,
                Gateway(),
                self.sender,
                retry_base=settings.retry_base_seconds,
            )
        )
        self.relay = OutboxRelay(self.database, self.messaging)
        self.drain_timeout = drain_timeout  # >= gateway5 + webhook10 + DB/commit budgets.
        self.cleanup_timeout = cleanup_timeout
        self.processing_timeout = processing_timeout
        self.backoff_base, self.backoff_cap = backoff_base, backoff_cap
        self.heartbeat_interval = heartbeat_interval
        self.heartbeat_path = heartbeat_path
        self.stop = asyncio.Event()
        self.ready = asyncio.Event()
        self.active: set[asyncio.Task[object]] = set()
        self.infrastructure_failures = 0
        self.state = "starting"
        self.fatal = False
        self._relay_task: asyncio.Task[None] | None = None
        # Registration must precede Messaging.start(), which starts subscribers.
        self.subscriber = self.messaging.broker.subscriber(
            self.messaging.topology.queue,
            self.messaging.topology.exchange,
            ack_policy=AckPolicy.MANUAL,
            decoder=raw_body,
            no_reply=True,
        )
        self.subscriber(self.handle)

    async def handle(self, message: RabbitMessage) -> None:
        task = asyncio.current_task()
        assert task is not None
        self.active.add(task)
        started = monotonic()
        fields: dict[str, object] = {}
        try:
            if self.stop.is_set():
                return  # Closing the channel requeues it; never ack cancelled work.
            try:
                if len(message.body) > 32768:
                    raise InvalidPaymentEvent
                event = PaymentEvent.model_validate_json(message.body)
                fields = {
                    "event_id": event.event_id,
                    "payment_id": event.payment_id,
                    "attempt": event.attempt,
                }
                async with asyncio.timeout(self.processing_timeout):
                    await self.processor.process(event)
            except (InvalidPaymentEvent, ValidationError):
                logger.warning(
                    "payment_rejected", extra={**fields, "errorcategory": "invalid_event"}
                )
                await message.reject(requeue=False)
            except (SQLAlchemyError, OSError, TimeoutError, AMQPError):
                self.infrastructure_failures += 1
                logger.warning(
                    "payment_requeued", extra={**fields, "errorcategory": "infrastructure_error"}
                )
                delay = retry_delay(
                    self.infrastructure_failures,
                    base_seconds=self.backoff_base,
                    max_seconds=self.backoff_cap,
                )
                try:
                    await asyncio.wait_for(self.stop.wait(), timeout=delay)
                except TimeoutError:
                    await message.nack(requeue=True)
            else:
                # process() returned after its durable commit (including no-op).
                await message.ack()
                self.infrastructure_failures = 0
                logger.info(
                    "payment_processed", extra={**fields, "duration": monotonic() - started}
                )
        except asyncio.CancelledError:
            raise
        except Exception:
            # Framework consumes handler exceptions. Explicitly fail the supervisor
            # instead of leaving an apparently healthy, wedged unacked consumer.
            self.fatal = True
            self.stop.set()
            logger.error("worker_failed", extra={"errorcategory": "unexpected_error"})
        finally:
            self.active.discard(task)

    def health(self) -> dict[str, str | bool]:
        connection = self.messaging.broker._connection
        relay_alive = self._relay_task is not None and not self._relay_task.done()
        connected = (
            connection is not None and not connection.is_closed and connection.connected.is_set()
        )
        return {
            "state": "fatal" if self.fatal else self.state,
            "consumer_alive": bool(
                self.state == "running"
                and not self.fatal
                and not self.stop.is_set()
                and connected
                and self.subscriber.running
                and relay_alive
            ),
            "relay_alive": relay_alive,
        }

    def _heartbeat(self) -> None:
        if self.heartbeat_path is not None:
            write_heartbeat(self.heartbeat_path, self.health())
        category = (
            "worker_fatal"
            if self.fatal
            else "worker_running"
            if self.health()["consumer_alive"]
            else "worker_not_ready"
        )
        logger.info("worker_heartbeat", extra={"errorcategory": category})

    async def run(self, stop: asyncio.Event | None = None) -> None:
        if stop is not None:
            self.stop = stop
        waiter: asyncio.Task[bool] | None = None
        try:
            self._heartbeat()  # Invalidate old ready state before the first network await.
            await self.messaging.start()
            self._relay_task = asyncio.create_task(
                self.relay.run(self.stop, manage_lifecycle=False)
            )
            waiter = asyncio.create_task(self.stop.wait())
            self.state = "running"
            self._heartbeat()
            self.ready.set()
            logger.info("worker_started")
            while not self.stop.is_set():
                await asyncio.wait(
                    (self._relay_task, waiter),
                    timeout=self.heartbeat_interval,
                    return_when=asyncio.FIRST_COMPLETED,
                )
                if self._relay_task.done() and (
                    not self.stop.is_set()
                    or (
                        not self._relay_task.cancelled()
                        and self._relay_task.exception() is not None
                    )
                ):
                    self.fatal = True
                    self.stop.set()
                self._heartbeat()
            if self.fatal:
                raise RuntimeError("worker_failed")
        except asyncio.CancelledError:
            raise
        except Exception:
            self.fatal = True
            raise RuntimeError("worker_failed") from None
        finally:
            self.stop.set()
            self.state = "stopping"
            self.subscriber.running = False  # Stop admission before the first await.
            try:
                self._heartbeat()
            except OSError:
                self.fatal = True  # A broken heartbeat must not skip resource cleanup.
            cleanup = asyncio.create_task(self._shutdown(waiter))
            cancelled: asyncio.CancelledError | None = None
            while not cleanup.done():
                try:
                    await asyncio.shield(cleanup)
                except asyncio.CancelledError as error:
                    cancelled = error
                except Exception:
                    break  # Retrieve/sanitize the owned task's error below.
            try:
                cleanup.result()
            except Exception:
                self.fatal = True
                logger.error("worker_failed", extra={"errorcategory": "shutdown_error"})
                raise RuntimeError("worker_failed") from None
            if cancelled is not None:
                raise cancelled

    async def _cleanup_phase(
        self, operation: Awaitable[object], *, timeout: float | None = None
    ) -> bool:
        """Independent cooperative deadline; a failed phase cannot skip the next.

        The owner is shielded from caller cancellation. A deadline cancels and
        awaits the operation, including gather's children: sends are not detached.
        This cannot force termination of code that ignores every cancellation.
        """
        try:
            async with asyncio.timeout(self.cleanup_timeout if timeout is None else timeout):
                await operation
        except (Exception, asyncio.CancelledError):
            return False
        return True

    async def _shutdown(self, waiter: asyncio.Task[bool] | None) -> None:
        tasks: list[asyncio.Task[object]] = []
        if waiter is not None:
            waiter.cancel()
            tasks.append(waiter)
        if self._relay_task is not None:
            self._relay_task.cancel()
            tasks.append(self._relay_task)
        failed = not await self._cleanup_phase(self.messaging.stop_consuming())
        failed |= not await self._cleanup_phase(asyncio.gather(*tasks, return_exceptions=True))
        if self.active:
            _, pending = await asyncio.wait(tuple(self.active), timeout=self.drain_timeout)
            for task in pending:
                task.cancel()
            failed |= not await self._cleanup_phase(
                asyncio.gather(*pending, return_exceptions=True)
            )
        # Preserve Messaging.close's shielded drain + independent transport
        # budgets; its outer budget leaves room for all of those phases.
        broker_budget = (
            self.messaging.timeout_seconds * (len(self.messaging.broker.subscribers) + 3)
            + self.cleanup_timeout
        )
        failed |= not await self._cleanup_phase(self.messaging.close(), timeout=broker_budget)
        failed |= not await self._cleanup_phase(self.sender.close())
        failed |= not await self._cleanup_phase(self.database.dispose())
        self.state = "stopped"
        self.fatal |= failed
        try:
            self._heartbeat()
        except OSError:
            failed = True
        logger.info("worker_stopped")
        if failed:
            raise RuntimeError("worker_failed")


async def _serve() -> None:
    worker = Worker(Settings(), heartbeat_path=DEFAULT_HEARTBEAT)
    loop = asyncio.get_running_loop()
    for signum in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(signum, worker.stop.set)
    try:
        await worker.run()
    finally:
        for signum in (signal.SIGTERM, signal.SIGINT):
            loop.remove_signal_handler(signum)


def main() -> int:
    configure_logging()
    try:
        asyncio.run(_serve())
    except KeyboardInterrupt:
        return 0
    except Exception:
        logger.error("worker_failed", extra={"errorcategory": "unexpected_error"})
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
