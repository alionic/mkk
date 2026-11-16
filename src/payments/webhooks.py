"""Pinned-DNS webhook delivery. The worker owns and closes one client per loop.

No retries live here: the processor owns durable attempts. Each send uses a fresh
connection and resolver result; aiohttp connects to those numeric IPs while the
request URL retains the original hostname for TLS SNI/certificate verification.
"""

import asyncio
import hashlib
import hmac
import ipaddress
import json
import math
import socket
import ssl
import time
from datetime import UTC, datetime
from types import TracebackType
from typing import Self

import aiohttp
from aiohttp.abc import AbstractResolver, ResolveResult

from payments.config import Settings
from payments.models import Payment
from payments.processor import DeliveryError
from payments.security import validate_webhook_url

_RESPONSE_LIMIT = 64 * 1024


def _utc(value: datetime) -> str:
    return value.astimezone(UTC).isoformat()


def build_payload(payment: Payment) -> bytes:
    """Canonical UTF-8 bytes, independent of attempt, generation and delivery state."""
    return json.dumps(
        {
            "event_id": str(payment.webhook_event_id),
            "payment_id": str(payment.id),
            "status": payment.status,
            "amount": format(payment.amount, ".2f"),
            "currency": payment.currency,
            "description": payment.description,
            "metadata": payment.metadata_,
            "created_at": _utc(payment.created_at),
            "processed_at": _utc(payment.processed_at) if payment.processed_at else None,
        },
        ensure_ascii=False,
        sort_keys=True,
        allow_nan=False,
        separators=(",", ":"),
    ).encode("utf-8")


def sign_payload(secret: str, timestamp: str, body: bytes) -> str:
    """Sign timestamp + '.' + the exact bytes supplied to the HTTP transport."""
    digest = hmac.new(
        secret.encode("utf-8"), timestamp.encode("ascii") + b"." + body, hashlib.sha256
    ).hexdigest()
    return "v1=" + digest


def _is_public(address: ipaddress.IPv4Address | ipaddress.IPv6Address) -> bool:
    if isinstance(address, ipaddress.IPv6Address) and address.ipv4_mapped is not None:
        return _is_public(address.ipv4_mapped)
    return (
        address.is_global
        and not address.is_private
        and not address.is_loopback
        and not address.is_link_local
        and not address.is_reserved
        and not address.is_multicast
        and not address.is_unspecified
    )


class PublicResolver(AbstractResolver):
    """Validate *all* DNS answers before returning any numeric connector targets.

    There is deliberately no preflight lookup or subsequent ordinary resolver.
    An empty/error answer is a network failure; one forbidden IP rejects the
    whole answer set, not just that IP. No resolver state needs async cleanup.
    """

    def __init__(self, settings: Settings) -> None:
        validated = Settings.model_validate(settings.model_dump())
        self._allow_private = validated.allow_unsafe_webhooks

    async def resolve(
        self, host: str, port: int = 0, family: socket.AddressFamily = socket.AF_INET
    ) -> list[ResolveResult]:
        try:
            answers = await asyncio.get_running_loop().getaddrinfo(
                host, port, family=family, type=socket.SOCK_STREAM, proto=socket.IPPROTO_TCP
            )
        except OSError:
            raise DeliveryError("webhook_network_error") from None
        if not answers:
            raise DeliveryError("webhook_network_error") from None
        result: list[ResolveResult] = []
        for answer_family, _, proto, _, sockaddr in answers:
            try:
                address = ipaddress.ip_address(sockaddr[0])
            except ValueError:
                raise DeliveryError("webhook_url_rejected") from None
            if (
                answer_family not in {socket.AF_INET, socket.AF_INET6}
                or (address.version == 4) != (answer_family == socket.AF_INET)
                or (not self._allow_private and not _is_public(address))
            ):
                raise DeliveryError("webhook_url_rejected") from None
            result.append(
                ResolveResult(
                    hostname=host,
                    host=str(address),
                    port=port,
                    family=answer_family,
                    proto=proto,
                    flags=socket.AI_NUMERICHOST | socket.AI_NUMERICSERV,
                )
            )
        return result

    async def close(self) -> None:
        """No persistent DNS sockets, tasks or cache are owned by this resolver."""


class WebhookClient:
    """Lazy, loop-local sender implementing processor.WebhookSender.

    Construction performs no I/O and creates no event-loop resources. Use
    ``async with WebhookClient(settings)`` or explicitly ``await open()`` (send
    also opens lazily), then ``await close()`` in the worker's finally block.
    Shutdown is terminal and idempotent. The owner should drain/cancel sends
    before close; cancelling a send releases its response and concurrency slot.
    Test/tuning overrides can only shorten the 10s total/3s connect upper bounds.
    """

    def __init__(
        self,
        settings: Settings,
        *,
        max_concurrency: int = 10,
        total_timeout: float = 10,
        connect_timeout: float = 3,
    ) -> None:
        self._settings = Settings.model_validate(settings.model_dump())
        if (
            isinstance(max_concurrency, bool)
            or not isinstance(max_concurrency, int)
            or max_concurrency < 1
            or not math.isfinite(total_timeout)
            or not 0 < total_timeout <= 10
            or not math.isfinite(connect_timeout)
            or not 0 < connect_timeout <= 3
        ):
            raise ValueError("invalid webhook transport limits")
        self._max_concurrency = max_concurrency
        self._total_timeout = total_timeout
        self._connect_timeout = connect_timeout
        self._session: aiohttp.ClientSession | None = None
        self._semaphore: asyncio.Semaphore | None = None
        self._loop: asyncio.AbstractEventLoop | None = None
        self._closed = False

    async def open(self) -> Self:
        if self._closed:
            raise RuntimeError("webhook client is closed")
        loop = asyncio.get_running_loop()
        if self._loop is not None and self._loop is not loop:
            raise RuntimeError("webhook client belongs to another event loop")
        if self._session is None:
            connector = aiohttp.TCPConnector(
                resolver=PublicResolver(self._settings),
                use_dns_cache=False,
                ssl=ssl.create_default_context(),
                force_close=True,
                limit=self._max_concurrency,
                limit_per_host=self._max_concurrency,
            )
            self._session = aiohttp.ClientSession(
                connector=connector,
                trust_env=False,
                cookie_jar=aiohttp.DummyCookieJar(),
                timeout=aiohttp.ClientTimeout(
                    total=self._total_timeout,
                    connect=self._connect_timeout,
                    sock_connect=self._connect_timeout,
                    ceil_threshold=11,
                ),
                auto_decompress=False,
                read_bufsize=8192,
                skip_auto_headers={"Accept-Encoding"},
            )
            self._semaphore = asyncio.Semaphore(self._max_concurrency)
            self._loop = loop
        return self

    async def send(self, payment: Payment) -> None:
        try:
            url = validate_webhook_url(payment.webhook_url, self._settings)
        except ValueError:
            raise DeliveryError("webhook_url_rejected") from None
        body = build_payload(payment)
        await self.open()
        assert self._session is not None and self._semaphore is not None
        try:
            # Includes waiting for a free concurrency slot, not only socket I/O.
            async with asyncio.timeout(self._total_timeout), self._semaphore:
                timestamp = str(int(time.time()))
                async with self._session.post(
                    url,
                    data=body,
                    allow_redirects=False,
                    headers={
                        "Content-Type": "application/json",
                        "Idempotency-Key": str(payment.webhook_event_id),
                        "X-Webhook-Timestamp": timestamp,
                        "X-Webhook-Signature": sign_payload(
                            self._settings.webhook_secret.get_secret_value(), timestamp, body
                        ),
                    },
                ) as response:
                    if not 200 <= response.status < 300:
                        raise DeliveryError("webhook_http_error") from None
                    if (
                        response.content_length is not None
                        and response.content_length > _RESPONSE_LIMIT
                    ):
                        raise DeliveryError("webhook_http_error") from None
                    remaining = _RESPONSE_LIMIT
                    while not response.content.at_eof():
                        # At the cap, one bounded lookahead byte distinguishes
                        # overflow from delayed chunked/connection-close EOF.
                        # This wait remains inside the same total deadline.
                        chunk = await response.content.read(min(8192, remaining + 1))
                        if len(chunk) > remaining:
                            raise DeliveryError("webhook_http_error") from None
                        if not chunk:
                            break
                        remaining -= len(chunk)
        except TimeoutError:
            raise DeliveryError("webhook_timeout") from None
        except (aiohttp.ClientError, OSError):
            raise DeliveryError("webhook_network_error") from None

    async def close(self) -> None:
        self._closed = True
        if self._session is not None:
            await self._session.close()

    async def __aenter__(self) -> Self:
        return await self.open()

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        await self.close()
