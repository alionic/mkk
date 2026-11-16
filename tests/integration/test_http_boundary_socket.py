"""Real Uvicorn sockets: keep-alive timeout does not bound an active body read."""

import asyncio
import socket
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import pytest
import uvicorn

from payments.api import create_app
from payments.config import Settings


@asynccontextmanager
async def api_socket(settings: Settings) -> AsyncIterator[int]:
    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    server = uvicorn.Server(
        uvicorn.Config(
            create_app(settings),
            access_log=False,
            log_config=None,
            timeout_keep_alive=0,
            timeout_graceful_shutdown=1,
        )
    )
    running = asyncio.create_task(server.serve(sockets=[sock]))
    try:
        async with asyncio.timeout(3):
            while not server.started:
                await asyncio.sleep(0.005)
        yield sock.getsockname()[1]
    finally:
        server.should_exit = True
        await asyncio.wait_for(running, 3)
        sock.close()


@pytest.mark.parametrize("mode", ["empty", "stalled", "trickle"])
async def test_unfinished_chunked_body_closes_with_408(settings: Settings, mode: str) -> None:
    config = settings.model_copy(update={"http_body_timeout_seconds": 0.15})
    async with api_socket(config) as port:
        reader, writer = await asyncio.open_connection("127.0.0.1", port)
        writer.write(
            b"POST /api/v1/payments HTTP/1.1\r\nHost: localhost\r\n"
            b"X-API-Key: test-api-key\r\nIdempotency-Key: timeout\r\n"
            b"Transfer-Encoding: chunked\r\n\r\n"
        )
        if mode != "empty":
            writer.write(b"1\r\nx\r\n")
        await writer.drain()

        async def trickle() -> None:
            if mode == "trickle":
                while True:
                    await asyncio.sleep(0.03)
                    writer.write(b"1\r\nx\r\n")
                    await writer.drain()

        sending = asyncio.create_task(trickle())
        try:
            try:
                # read() also proves EOF/connection close, not merely response headers.
                response = await asyncio.wait_for(reader.read(), 0.8)
            except TimeoutError:
                pytest.fail("Uvicorn retained an unfinished request beyond the total body budget")
            assert response.startswith(b"HTTP/1.1 408 ")
            assert b"connection: close\r\n" in response.lower()
            assert response.endswith(b'{"detail":"Request body timeout"}')
        finally:
            sending.cancel()
            await asyncio.gather(sending, return_exceptions=True)
            writer.close()
            await writer.wait_closed()
