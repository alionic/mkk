"""Async Alembic entrypoint; DATABASE_URL only, no broker/secrets needed."""

import asyncio
import os

from alembic import context
from sqlalchemy import Connection

from payments.db import Database
from payments.models import Base

config = context.config
target_metadata = Base.metadata


def run_migrations(connection: Connection) -> None:
    context.configure(connection=connection, target_metadata=target_metadata, compare_type=True)
    with context.begin_transaction():
        context.run_migrations()


async def run_online() -> None:
    database = Database(os.environ["DATABASE_URL"])
    try:
        async with database.engine.connect() as connection:
            await connection.run_sync(run_migrations)
    finally:
        await database.dispose()


if context.is_offline_mode():
    context.configure(
        url=os.environ["DATABASE_URL"],
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )
    with context.begin_transaction():
        context.run_migrations()
elif (connection := config.attributes.get("connection")) is not None:
    run_migrations(connection)
else:
    asyncio.run(run_online())
