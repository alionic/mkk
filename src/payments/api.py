"""Authenticated HTTP boundary with bounded raw, exact-decimal JSON parsing."""

import asyncio
import hashlib
import secrets
from collections.abc import AsyncIterator, Sequence
from contextlib import asynccontextmanager
from typing import Annotated, Any
from uuid import UUID

from fastapi import FastAPI, Header, HTTPException, Request, Security
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from fastapi.security import APIKeyHeader
from pydantic import ValidationError
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from starlette.datastructures import Headers
from starlette.types import ASGIApp, Message, Receive, Scope, Send

from payments.config import Settings
from payments.db import Database
from payments.logging import configure_logging
from payments.schemas import NulFreeText, PaymentAccepted, PaymentCreate, PaymentRead
from payments.security import validate_webhook_url
from payments.service import IdempotencyConflict, create_payment, get_payment

MAX_BODY_BYTES = 32 * 1024


class HTTPBoundary:
    """Enforce auth, actual bytes and a total body budget before routing/parsing."""

    def __init__(self, app: ASGIApp, api_key: str, body_timeout: float = 10) -> None:
        self.app = app
        self.key_digest = hashlib.sha256(api_key.encode("utf-8")).digest()
        self.body_timeout = body_timeout

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return
        headers = Headers(scope=scope)
        keys = headers.getlist("x-api-key")
        supplied = keys[0] if len(keys) == 1 else ""
        matched = secrets.compare_digest(
            hashlib.sha256(supplied.encode("utf-8")).digest(), self.key_digest
        )
        if not matched:
            await JSONResponse({"detail": "Unauthorized"}, status_code=401)(scope, receive, send)
            return
        body = bytearray()
        try:
            # One deadline for the entire stream, not reset by trickled chunks.
            async with asyncio.timeout(self.body_timeout):
                while True:
                    message = await receive()
                    if message["type"] == "http.disconnect":
                        return
                    chunk = message.get("body", b"")
                    if len(body) + len(chunk) > MAX_BODY_BYTES:
                        await JSONResponse({"detail": "Request body too large"}, status_code=413)(
                            scope, receive, send
                        )
                        return
                    body.extend(chunk)
                    if not message.get("more_body", False):
                        break
        except TimeoutError:
            # Do not drain an unfinished request or reuse its HTTP/1.x connection.
            # Connection-specific headers are prohibited on HTTP/2 and HTTP/3.
            close = {"Connection": "close"} if scope["http_version"] in {"1.0", "1.1"} else {}
            await JSONResponse({"detail": "Request body timeout"}, status_code=408, headers=close)(
                scope, receive, send
            )
            return

        delivered = False

        async def bounded_receive() -> Message:
            nonlocal delivered
            if not delivered:
                delivered = True
                return {"type": "http.request", "body": bytes(body), "more_body": False}
            return await receive()

        await self.app(scope, bounded_receive, send)


def _invalid_request_detail(errors: Sequence[Any]) -> list[dict[str, Any]]:
    # Even loc/message may contain user values (extra field names, metadata keys).
    # Whitelist only the top-level schema field, never echo arbitrary diagnostics.
    safe_fields = set(PaymentCreate.model_fields) | {"payment_id", "Idempotency-Key"}
    detail = []
    for error in errors:
        loc = error.get("loc", ())
        safe_loc = ["body"]
        if loc and loc[0] in {"body", "header", "path", "query"}:
            safe_loc = [loc[0]]
            loc = loc[1:]
        if loc and loc[0] in safe_fields:
            safe_loc.append(loc[0])
        detail.append({"loc": safe_loc, "msg": "Invalid request", "type": "value_error"})
    return detail


def create_app(settings: Settings | None = None) -> FastAPI:
    config = settings if settings is not None else Settings()
    database = Database(config.database_url)

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        try:
            yield
        finally:
            await database.dispose()

    app = FastAPI(
        title="Payments API",
        version="1.0.0",
        lifespan=lifespan,
        docs_url=None,
        redoc_url=None,
        openapi_url=None,
        dependencies=[Security(APIKeyHeader(name="X-API-Key", auto_error=False))],
    )
    app.state.database = database
    app.state.settings = config
    app.add_middleware(
        HTTPBoundary,
        api_key=config.api_key.get_secret_value(),
        body_timeout=config.http_body_timeout_seconds,
    )

    @app.exception_handler(RequestValidationError)
    async def request_validation_error(
        request: Request, exc: RequestValidationError
    ) -> JSONResponse:
        return JSONResponse({"detail": _invalid_request_detail(exc.errors())}, status_code=422)

    @app.exception_handler(SQLAlchemyError)
    @app.exception_handler(OSError)
    @app.exception_handler(TimeoutError)
    async def database_error(request: Request, exc: Exception) -> JSONResponse:
        return JSONResponse({"detail": "Service unavailable"}, status_code=503)

    # Raw parsing bypasses FastAPI's automatic request schema registration. Give
    # input definitions their own namespace so Money cannot overwrite the string
    # output schema generated for PaymentRead.
    request_schema = PaymentCreate.model_json_schema(
        ref_template="#/components/schemas/PaymentCreate-{model}"
    )
    request_definitions = {
        f"PaymentCreate-{name}": definition
        for name, definition in request_schema.pop("$defs", {}).items()
    }

    @app.post(
        "/api/v1/payments",
        status_code=202,
        openapi_extra={
            "requestBody": {
                "required": True,
                "content": {"application/json": {"schema": request_schema}},
            }
        },
    )
    async def post_payment(
        request: Request,
        key: Annotated[NulFreeText, Header(alias="Idempotency-Key", min_length=1, max_length=128)],
    ) -> PaymentAccepted:
        try:
            body = PaymentCreate.model_validate_json(await request.body())
        except ValidationError as exc:
            return_errors = exc.errors(include_input=False, include_context=False)
            raise HTTPException(
                status_code=422, detail=_invalid_request_detail(return_errors)
            ) from None
        try:
            body.webhook_url = validate_webhook_url(body.webhook_url, config)
        except ValueError:
            raise HTTPException(status_code=422, detail="Invalid webhook URL") from None
        try:
            async with database.session_factory.begin() as session:
                payment = await create_payment(session, body, key)
                response = PaymentAccepted(
                    payment_id=payment.id, status=payment.status, created_at=payment.created_at
                )
        except IdempotencyConflict:
            raise HTTPException(status_code=409, detail="Idempotency key conflict") from None
        return response

    @app.get("/api/v1/payments/{payment_id}")
    async def read_payment(payment_id: UUID) -> PaymentRead:
        async with database.session_factory() as session:
            payment = await get_payment(session, payment_id)
            if payment is None:
                raise HTTPException(status_code=404, detail="Payment not found")
            return PaymentRead(
                payment_id=payment.id,
                amount=payment.amount,
                currency=payment.currency,
                description=payment.description,
                metadata=payment.metadata_,
                webhook_url=payment.webhook_url,
                status=payment.status,
                created_at=payment.created_at,
                processed_at=payment.processed_at,
                webhook_status=payment.webhook_status,
                webhook_delivered_at=payment.webhook_delivered_at,
            )

    @app.get("/health/live")
    async def live() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/health/ready")
    async def ready() -> dict[str, str]:
        async with database.session_factory() as session:
            await session.execute(text("SELECT 1"))
        return {"status": "ok"}

    # HTTP schema serving is disabled; app.openapi() remains available for CLI export.
    app.openapi_schema = app.openapi()
    app.openapi_schema.setdefault("components", {}).setdefault("schemas", {}).update(
        request_definitions
    )
    return app


def main() -> None:
    import uvicorn

    configure_logging()
    uvicorn.run(
        "payments.api:create_app",
        factory=True,
        host="0.0.0.0",
        port=8000,
        access_log=False,
        log_config=None,
    )


if __name__ == "__main__":
    main()
