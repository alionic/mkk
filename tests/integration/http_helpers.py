"""Raw HTTP receiver with deterministic delayed EOF/overflow at the response cap."""

import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager


@asynccontextmanager
async def capped_response_socket(framing: str, ending: str) -> AsyncIterator[int]:
    release = asyncio.Event()
    tasks: set[asyncio.Task[None]] = set()

    async def handler(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        task = asyncio.current_task()
        assert task is not None
        tasks.add(task)
        try:
            headers = await reader.readuntil(b"\r\n\r\n")
            length = next(
                int(line.split(b":", 1)[1])
                for line in headers.lower().split(b"\r\n")
                if line.startswith(b"content-length:")
            )
            await reader.readexactly(length)
            header = b"Transfer-Encoding: chunked\r\n" if framing == "chunked" else b""
            writer.write(b"HTTP/1.1 200 OK\r\n" + header + b"Connection: close\r\n\r\n")
            body = b"x" * 65536
            writer.write(b"10000\r\n" + body + b"\r\n" if framing == "chunked" else body)
            await writer.drain()
            # Force the exact cap and its framing terminator into separate packets.
            await asyncio.sleep(0.05)
            if ending == "eof":
                if framing == "chunked":
                    writer.write(b"0\r\n\r\n")
                    await writer.drain()
                return  # close-delimited EOF is the TCP close in finally.
            if ending == "excess":
                writer.write(b"1\r\ny\r\n" if framing == "chunked" else b"y")
                await writer.drain()
            # No EOF: overflow must reject promptly, exact-cap must hit total timeout.
            await release.wait()
        finally:
            writer.close()
            await writer.wait_closed()
            tasks.discard(task)

    server = await asyncio.start_server(handler, "127.0.0.1", 0)
    try:
        yield server.sockets[0].getsockname()[1]
    finally:
        server.close()
        release.set()
        await asyncio.wait_for(asyncio.gather(*tasks), 1)
        await server.wait_closed()
