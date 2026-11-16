import asyncio
import socket
from typing import Any

import pytest

from payments.config import Settings
from payments.models import Payment
from payments.processor import DeliveryError
from payments.webhooks import PublicResolver, WebhookClient


@pytest.mark.parametrize(
    "address",
    [
        "127.0.0.1",
        "10.0.0.1",
        "169.254.169.254",
        "172.16.0.1",
        "192.168.1.1",
        "0.0.0.0",
        "100.64.0.1",
        "192.0.2.1",
        "240.0.0.1",
        "224.0.0.1",
        "::1",
        "::",
        "fc00::1",
        "fe80::1",
        "ff02::1",
        "2001:db8::1",
        "::ffff:127.0.0.1",
        "::ffff:10.0.0.1",
        "::ffff:169.254.169.254",
    ],
)
@pytest.mark.parametrize("private_first", [False, True])
async def test_resolver_rejects_any_private_answer(
    settings: Settings, monkeypatch: pytest.MonkeyPatch, address: str, private_first: bool
) -> None:
    public = (socket.AF_INET, socket.SOCK_STREAM, 6, "", ("8.8.8.8", 443))
    family = socket.AF_INET6 if ":" in address else socket.AF_INET
    sockaddr = (address, 443, 0, 0) if family == socket.AF_INET6 else (address, 443)
    private = (family, socket.SOCK_STREAM, 6, "", sockaddr)

    async def lookup(*args: Any, **kwargs: Any) -> list[Any]:
        return [private, public] if private_first else [public, private]

    monkeypatch.setattr(asyncio.get_running_loop(), "getaddrinfo", lookup)
    resolver = PublicResolver(settings)
    try:
        with pytest.raises(DeliveryError, match="^webhook_url_rejected$"):
            await resolver.resolve("hooks.example.com", 443, socket.AF_UNSPEC)
    finally:
        await resolver.close()


async def test_resolver_returns_only_numeric_validated_addresses(
    settings: Settings, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def lookup(*args: Any, **kwargs: Any) -> list[Any]:
        return [
            (socket.AF_INET, socket.SOCK_STREAM, 6, "", ("8.8.8.8", 443)),
            (socket.AF_INET6, socket.SOCK_STREAM, 6, "", ("2606:4700:4700::1111", 443, 0, 0)),
        ]

    monkeypatch.setattr(asyncio.get_running_loop(), "getaddrinfo", lookup)
    resolver = PublicResolver(settings)
    try:
        answers = await resolver.resolve("hooks.example.com", 443, socket.AF_UNSPEC)
        assert [answer["host"] for answer in answers] == ["8.8.8.8", "2606:4700:4700::1111"]
        assert all(answer["hostname"] == "hooks.example.com" for answer in answers)
        assert all(answer["flags"] & socket.AI_NUMERICHOST for answer in answers)
    finally:
        await resolver.close()


@pytest.mark.parametrize("failure", [[], OSError("SECRET DNS text")])
async def test_dns_failure_is_sanitized(
    settings: Settings, monkeypatch: pytest.MonkeyPatch, failure: list[object] | OSError
) -> None:
    async def lookup(*args: Any, **kwargs: Any) -> list[object]:
        if isinstance(failure, OSError):
            raise failure
        return failure

    monkeypatch.setattr(asyncio.get_running_loop(), "getaddrinfo", lookup)
    resolver = PublicResolver(settings)
    try:
        with pytest.raises(DeliveryError, match="^webhook_network_error$") as error:
            await resolver.resolve("hooks.example.com", 443)
        assert error.value.__suppress_context__
    finally:
        await resolver.close()


@pytest.mark.parametrize(
    "url",
    [
        "https://evil.example.net/?token=SECRET",
        "https://example.com.evil.net/",
        "http://example.com/",
        "https://example.com:8443/",
        "https://127.0.0.1/",
        "https://user:SECRET@example.com/",
        "https://example.com/#SECRET",
    ],
)
async def test_allowlist_rechecked_at_send(settings: Settings, url: str) -> None:
    client = WebhookClient(settings)
    try:
        with pytest.raises(DeliveryError, match="^webhook_url_rejected$"):
            await client.send(Payment(webhook_url=url))
    finally:
        await client.close()


@pytest.mark.parametrize("url", ["http://not-allowed.test/", "http://127.0.0.1/"])
async def test_dev_mode_never_bypasses_allowlist_or_literal_policy(
    settings: Settings, url: str
) -> None:
    client = WebhookClient(settings.model_copy(update={"allow_unsafe_webhooks": True}))
    try:
        with pytest.raises(DeliveryError, match="^webhook_url_rejected$"):
            await client.send(Payment(webhook_url=url))
    finally:
        await client.close()


def test_client_revalidates_unsafe_production_settings(settings: Settings) -> None:
    with pytest.raises(ValueError):
        WebhookClient(
            settings.model_copy(update={"environment": "production", "allow_unsafe_webhooks": True})
        )


@pytest.mark.parametrize(
    "kwargs",
    [{"total_timeout": 11}, {"connect_timeout": 4}, {"total_timeout": 0}, {"max_concurrency": 0}],
)
def test_transport_limits_cannot_be_disabled(settings: Settings, kwargs: dict[str, int]) -> None:
    with pytest.raises(ValueError):
        WebhookClient(settings, **kwargs)
