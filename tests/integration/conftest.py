"""Rabbit tests own random vhosts, never shared application queues."""

import os
from collections.abc import AsyncIterator
from urllib.parse import quote, urlsplit, urlunsplit
from uuid import uuid4

import pytest
from httpx import AsyncClient, BasicAuth

from payments.messaging import Messaging


@pytest.fixture
async def rabbit_admin() -> AsyncIterator[AsyncClient]:
    url = os.environ.get("TEST_RABBITMQ_URL")
    management = os.environ.get("TEST_RABBITMQ_MANAGEMENT_URL")
    if not url or not management:
        pytest.skip("RabbitMQ test URLs not configured; real RabbitMQ is required")
    parsed = urlsplit(url)
    async with AsyncClient(
        base_url=management,
        auth=BasicAuth(parsed.username or "", parsed.password or ""),
        timeout=10,
        trust_env=False,
    ) as client:
        yield client


@pytest.fixture
async def rabbit_url(rabbit_admin: AsyncClient) -> AsyncIterator[str]:
    parsed = urlsplit(os.environ["TEST_RABBITMQ_URL"])
    name = f"payments_test_{uuid4().hex}"
    (await rabbit_admin.put(f"/api/vhosts/{name}", json={})).raise_for_status()
    try:
        response = await rabbit_admin.put(
            f"/api/permissions/{name}/{quote(parsed.username or '', safe='')}",
            json={"configure": ".*", "write": ".*", "read": ".*"},
        )
        response.raise_for_status()
        yield urlunsplit(parsed._replace(path=f"/{name}"))
    finally:
        # Only the UUID vhost created by this fixture is deleted.
        (await rabbit_admin.delete(f"/api/vhosts/{name}")).raise_for_status()


@pytest.fixture
async def messaging(rabbit_url: str) -> AsyncIterator[Messaging]:
    publisher = Messaging(rabbit_url, timeout_seconds=5)
    try:
        await publisher.start()
        yield publisher
    finally:
        await publisher.close()
