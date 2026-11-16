"""Real PostgreSQL integration fixtures; never reuse application tables."""

import os
from collections.abc import AsyncIterator
from uuid import uuid4

import pytest
from alembic import command
from alembic.config import Config
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient
from sqlalchemy import Connection, text
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import create_async_engine

from payments.api import create_app
from payments.config import Settings
from payments.db import Database


@pytest.fixture
def settings() -> Settings:
    return Settings(
        database_url="postgresql+asyncpg://unused:unused@127.0.0.1:1/unused",
        rabbitmq_url="amqp://unused:unused@127.0.0.1:1/",
        api_key="test-api-key",
        webhook_secret="test-webhook-secret",
        webhook_allowed_domains=["example.com"],
        environment="test",
    )


def migrate(connection: Connection, revision: str, *, downgrade: bool = False) -> None:
    config = Config("alembic.ini")
    config.attributes["connection"] = connection
    if downgrade:
        command.downgrade(config, revision)
    else:
        command.upgrade(config, revision)


@pytest.fixture
async def database() -> AsyncIterator[Database]:
    url = os.environ.get("TEST_DATABASE_URL")
    if not url:
        pytest.skip("TEST_DATABASE_URL not configured; real PostgreSQL is required")
    parsed = make_url(url)
    if parsed.drivername != "postgresql+asyncpg":
        pytest.fail("TEST_DATABASE_URL must use PostgreSQL/asyncpg", pytrace=False)
    # CREATE/DROP is restricted to a random database owned by this fixture.
    name = f"payments_test_{uuid4().hex}"
    admin = create_async_engine(url, isolation_level="AUTOCOMMIT", hide_parameters=True)
    async with admin.connect() as connection:
        await connection.execute(text(f'CREATE DATABASE "{name}"'))
    db = Database(parsed.set(database=name).render_as_string(hide_password=False))
    try:
        async with db.engine.begin() as connection:
            await connection.run_sync(migrate, "head")
        yield db
    finally:
        await db.dispose()
        async with admin.connect() as connection:
            await connection.execute(text(f'DROP DATABASE "{name}"'))
        await admin.dispose()


@pytest.fixture
def app(settings: Settings, database: Database) -> FastAPI:
    return create_app(
        settings.model_copy(
            update={"database_url": database.engine.url.render_as_string(hide_password=False)}
        )
    )


@pytest.fixture
async def client(app: FastAPI) -> AsyncIterator[AsyncClient]:
    async with (
        app.router.lifespan_context(app),
        AsyncClient(
            transport=ASGITransport(app=app),
            base_url="http://test",
            headers={"X-API-Key": "test-api-key", "Idempotency-Key": "test-key"},
        ) as http,
    ):
        yield http
