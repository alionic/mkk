"""Consume (not basic.get): quorum single-active queues forbid basic.get."""

import asyncio

from aio_pika.abc import AbstractIncomingMessage, AbstractQueue


async def receive(queue: AbstractQueue, *, timeout: float = 15) -> AbstractIncomingMessage:
    message: asyncio.Future[AbstractIncomingMessage] = asyncio.get_running_loop().create_future()

    async def delivered(item: AbstractIncomingMessage) -> None:
        if not message.done():
            message.set_result(item)

    tag = await queue.consume(delivered, timeout=3)
    try:
        return await asyncio.wait_for(message, timeout)
    finally:
        await queue.cancel(tag, timeout=3)
