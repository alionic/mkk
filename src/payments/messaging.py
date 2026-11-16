"""Shared FastStream topology and confirmed, mandatory RabbitMQ publishing.

Workers register subscribers on ``messaging.broker`` with ``topology.queue`` and
``topology.exchange`` *before* start(). They own acknowledgement/business logic.
Every successful publish has received a broker Ack, not just written a socket.
"""

import asyncio
import math
from dataclasses import dataclass
from typing import cast
from uuid import UUID

from aiormq.exceptions import AMQPError
from aiormq.exceptions import PublishError as ReturnedMessage
from faststream.rabbit import RabbitBroker, RabbitExchange, RabbitQueue
from faststream.rabbit.schemas import Channel, QueueType
from faststream.rabbit.subscriber.usecase import RabbitSubscriber
from pamqp.commands import Basic


@dataclass(frozen=True)
class RabbitTopology:
    exchange: RabbitExchange
    queue: RabbitQueue
    dead_exchange: RabbitExchange
    dead_queue: RabbitQueue


def create_topology(*, timeout_seconds: float = 5) -> RabbitTopology:
    return RabbitTopology(
        exchange=RabbitExchange("payments", durable=True, timeout=timeout_seconds),
        queue=RabbitQueue(
            "payments.new",
            queue_type=QueueType.QUORUM,
            durable=True,
            routing_key="payments.new",
            timeout=timeout_seconds,
            arguments={
                "x-single-active-consumer": True,
                "x-dead-letter-exchange": "payments.dlx",
                "x-dead-letter-routing-key": "payments.dlq",
                "x-dead-letter-strategy": "at-least-once",
                "x-overflow": "reject-publish",
                "x-delivery-limit": 3,
            },
        ),
        dead_exchange=RabbitExchange("payments.dlx", durable=True, timeout=timeout_seconds),
        dead_queue=RabbitQueue(
            "payments.dlq",
            queue_type=QueueType.QUORUM,
            durable=True,
            routing_key="payments.dlq",
            timeout=timeout_seconds,
        ),
    )


def create_broker(url: str, *, timeout_seconds: float = 5) -> RabbitBroker:
    """The same channel policy applies to future FastStream subscribers."""
    return RabbitBroker(
        url,
        timeout=timeout_seconds,
        fail_fast=True,
        reconnect_interval=1,
        graceful_timeout=timeout_seconds,
        default_channel=Channel(
            prefetch_count=1,
            publisher_confirms=True,
            on_return_raises=True,
        ),
        logger=None,
    )


class PublishError(RuntimeError):
    """Safe transport category; contains no connection string or payload."""


class Messaging:
    def __init__(self, url: str, *, timeout_seconds: float = 5) -> None:
        if not math.isfinite(timeout_seconds) or timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be finite and positive")
        self.timeout_seconds = timeout_seconds
        self.topology = create_topology(timeout_seconds=timeout_seconds)
        self.broker = create_broker(url, timeout_seconds=timeout_seconds)
        self._started = False

    async def start(self) -> None:
        """Declare both routes before publishing or starting any subscribers."""
        if self._started:
            return
        try:
            async with asyncio.timeout(self.timeout_seconds):
                await self.broker.connect()
                dead_exchange = await self.broker.declare_exchange(self.topology.dead_exchange)
                dead_queue = await self.broker.declare_queue(self.topology.dead_queue)
                await dead_queue.bind(
                    dead_exchange,
                    routing_key="payments.dlq",
                    timeout=self.timeout_seconds,
                )
                exchange = await self.broker.declare_exchange(self.topology.exchange)
                queue = await self.broker.declare_queue(self.topology.queue)
                await queue.bind(exchange, routing_key="payments.new", timeout=self.timeout_seconds)
                await self.broker.start()
                self._started = True
        except BaseException:
            await self.close()
            raise

    async def close(self) -> None:
        """Finish bounded cleanup before propagating caller cancellation."""
        self._started = False
        cleanup = asyncio.create_task(self._close())
        cancelled: asyncio.CancelledError | None = None
        while not cleanup.done():
            try:
                await asyncio.shield(cleanup)
            except asyncio.CancelledError as error:
                cancelled = error
        cleanup.result()
        if cancelled is not None:
            raise cancelled

    async def stop_consuming(self) -> None:
        """Cancel broker intake without draining or closing the publish channel.

        FastStream 0.6's stop() drains *before* basic.cancel. Keep the narrow
        transport-only adaptation here, beside the existing shutdown adapter.
        """
        for base_subscriber in self.broker.subscribers:
            subscriber = cast(RabbitSubscriber, base_subscriber)
            subscriber.running = False
            queue = subscriber._queue_obj
            tag = subscriber._consumer_tag
            if queue is not None and tag is not None and not queue.channel.is_closed:
                async with asyncio.timeout(self.timeout_seconds):
                    await queue.cancel(tag, timeout=self.timeout_seconds)
                subscriber._consumer_tag = None

    async def _close(self) -> None:
        # FastStream 0.6 stop() has no transport-only/finally cleanup API.
        # Keep its subscriber -> channel -> connection order, but give drain
        # and each transport close independent budgets. A subscriber timeout
        # or a failed channel close must never skip closing the connection.
        try:
            drain_timeout = self.timeout_seconds * (len(self.broker.subscribers) + 1)
            async with asyncio.timeout(drain_timeout):
                for subscriber in self.broker.subscribers:
                    await subscriber.stop()
        finally:
            self.broker.running = False
            try:
                if self.broker._channel is not None and not self.broker._channel.is_closed:
                    async with asyncio.timeout(self.timeout_seconds):
                        await self.broker._channel.close()
            finally:
                if self.broker._connection is not None:
                    async with asyncio.timeout(self.timeout_seconds):
                        await self.broker._connection.close()
                self.broker._channel = None
                self.broker._connection = None
                self.broker.config.disconnect()

    async def publish_event(
        self, event: dict[str, object], destination: str, message_id: UUID
    ) -> None:
        if not self._started:
            raise PublishError("publisher_not_started")
        try:
            async with asyncio.timeout(self.timeout_seconds):
                confirmation = await self.broker.publish(
                    event,
                    exchange=(
                        self.topology.dead_exchange
                        if destination == "payments.dlq"
                        else self.topology.exchange
                    ),
                    routing_key=destination,
                    message_id=str(message_id),
                    mandatory=True,
                    persist=True,
                    timeout=self.timeout_seconds,
                )
                if not isinstance(confirmation, Basic.Ack):
                    raise PublishError("publish_not_confirmed")
        except ReturnedMessage:
            raise PublishError("publish_unroutable") from None
        except TimeoutError:
            raise PublishError("publish_timeout") from None
        except (AMQPError, OSError):
            raise PublishError("publish_failed") from None
