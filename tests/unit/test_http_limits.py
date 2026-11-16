import asyncio
from collections.abc import AsyncIterator
from typing import Any

import pytest
from httpx import ASGITransport, AsyncClient

from payments.api import create_app
from payments.config import Settings


@pytest.mark.parametrize(
    "path",
    [
        "/api/v1/payments",
        "/health/live",
        "/health/ready",
        "/unknown",
        "/docs",
        "/redoc",
        "/openapi.json",
    ],
)
@pytest.mark.parametrize("key", [None, "wrong-key", "test-api-key-extra"])
async def test_auth_all_routes(settings: Settings, path: str, key: str | None) -> None:
    app = create_app(settings)
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        headers = {} if key is None else {"X-API-Key": key}
        response = await client.get(path, headers=headers)
    assert response.status_code == 401
    assert response.json() == {"detail": "Unauthorized"}


@pytest.mark.parametrize("path", ["/docs", "/redoc", "/openapi.json", "/unknown"])
async def test_docs_disabled(settings: Settings, path: str) -> None:
    async with AsyncClient(
        transport=ASGITransport(app=create_app(settings)), base_url="http://test"
    ) as client:
        response = await client.get(path, headers={"X-API-Key": "test-api-key"})
    assert response.status_code == 404


@pytest.mark.parametrize("declared_length", [None, "1", "32769"])
async def test_oversized_streaming_body(settings: Settings, declared_length: str | None) -> None:
    async def chunks() -> AsyncIterator[bytes]:
        yield b" " * 16000
        yield b" " * 16000
        yield b" " * 769
        raise AssertionError("must not read beyond size limit")

    headers = {"X-API-Key": "test-api-key", "Idempotency-Key": "limit-key"}
    if declared_length is not None:
        headers["Content-Length"] = declared_length
    async with AsyncClient(
        transport=ASGITransport(app=create_app(settings)), base_url="http://test"
    ) as client:
        response = await client.post("/api/v1/payments", content=chunks(), headers=headers)
    assert response.status_code == 413


async def test_exact_body_limit_parsed_as_json(settings: Settings) -> None:
    async with AsyncClient(
        transport=ASGITransport(app=create_app(settings)), base_url="http://test"
    ) as client:
        response = await client.post(
            "/api/v1/payments",
            content=b" " * 32768,
            headers={"X-API-Key": "test-api-key", "Idempotency-Key": "limit-key"},
        )
    assert response.status_code == 422


async def test_database_unavailable_readiness_is_sanitized(settings: Settings) -> None:
    app = create_app(settings)
    async with (
        app.router.lifespan_context(app),
        AsyncClient(
            transport=ASGITransport(app=app),
            base_url="http://test",
            headers={"X-API-Key": "test-api-key"},
        ) as client,
    ):
        assert (await client.get("/health/live")).status_code == 200
        response = await client.get("/health/ready")
    assert response.status_code == 503
    assert response.json() == {"detail": "Service unavailable"}


async def test_auth_before_body_read(settings: Settings) -> None:
    async def forbidden_body() -> AsyncIterator[bytes]:
        raise AssertionError("unauthenticated body must not be read")
        yield b""  # pragma: no cover

    async with AsyncClient(
        transport=ASGITransport(app=create_app(settings)), base_url="http://test"
    ) as client:
        response = await client.post("/api/v1/payments", content=forbidden_body())
    assert response.status_code == 401


@pytest.mark.parametrize("trickle", [False, True])
async def test_body_total_deadline_stops_reading(settings: Settings, trickle: bool) -> None:
    stopped = asyncio.Event()

    async def chunks() -> AsyncIterator[bytes]:
        try:
            yield b"PRIVATE_BODY"
            while True:
                await asyncio.sleep(0.02 if trickle else 10)
                yield b" "
        finally:
            stopped.set()

    app = create_app(settings.model_copy(update={"http_body_timeout_seconds": 0.1}))
    async with (
        app.router.lifespan_context(app),
        AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client,
    ):
        try:
            response = await asyncio.wait_for(
                client.post(
                    "/api/v1/payments",
                    content=chunks(),
                    headers={"X-API-Key": "test-api-key", "Idempotency-Key": "timeout"},
                ),
                0.6,
            )
        except TimeoutError:
            pytest.fail("body read did not terminate within its total budget")
    assert response.status_code == 408
    assert response.json() == {"detail": "Request body timeout"}
    assert response.headers["Connection"] == "close"
    assert stopped.is_set()


def test_openapi_request_auth_and_idempotency(settings: Settings) -> None:
    schema: dict[str, Any] = create_app(settings).openapi()
    operation = schema["paths"]["/api/v1/payments"]["post"]
    body = operation["requestBody"]
    assert body["required"] is True
    properties = body["content"]["application/json"]["schema"]["properties"]
    assert {"amount", "currency", "description", "metadata", "webhook_url"} <= properties.keys()
    parameter = next(p for p in operation["parameters"] if p["name"] == "Idempotency-Key")
    assert parameter["required"] is True
    assert parameter["schema"]["minLength"] == 1
    assert parameter["schema"]["maxLength"] == 128
    assert operation["security"] == [{"APIKeyHeader": []}]
    assert schema["components"]["securitySchemes"]["APIKeyHeader"] == {
        "type": "apiKey",
        "in": "header",
        "name": "X-API-Key",
    }
    for path in ("/health/live", "/health/ready"):
        assert schema["paths"][path]["get"]["security"] == [{"APIKeyHeader": []}]


def test_openapi_input_money_does_not_override_output(settings: Settings) -> None:
    schema = create_app(settings).openapi()
    definitions = schema["components"]["schemas"]
    response_amount = definitions["PaymentRead"]["properties"]["amount"]
    output_money = definitions[response_amount["$ref"].rsplit("/", 1)[1]]
    assert output_money["type"] == "string"
    operation = schema["paths"]["/api/v1/payments"]["post"]
    input_amount = operation["requestBody"]["content"]["application/json"]["schema"]["properties"][
        "amount"
    ]
    input_money = definitions[input_amount["$ref"].rsplit("/", 1)[1]]
    assert {item["type"] for item in input_money["anyOf"]} == {"number", "string"}
