import pytest
from alembic.autogenerate import compare_metadata
from alembic.migration import MigrationContext
from conftest import migrate
from sqlalchemy import Connection, Numeric, inspect, text
from sqlalchemy.exc import IntegrityError

from payments.db import Database
from payments.models import Base
from payments.schemas import PaymentCreate
from payments.service import create_payment


async def test_upgrade_downgrade_upgrade(database: Database) -> None:
    async with database.engine.begin() as connection:
        tables = await connection.run_sync(lambda c: inspect(c).get_table_names())
        assert {"payments", "outbox", "alembic_version"} <= set(tables)
        await connection.run_sync(migrate, "base", downgrade=True)
        tables = await connection.run_sync(lambda c: inspect(c).get_table_names())
        assert "payments" not in tables and "outbox" not in tables
        await connection.run_sync(migrate, "head")
        columns = await connection.run_sync(lambda c: inspect(c).get_columns("payments"))
        amount = next(c["type"] for c in columns if c["name"] == "amount")
        assert isinstance(amount, Numeric)
        assert (amount.precision, amount.scale) == (18, 2)
        indexes = await connection.run_sync(lambda c: inspect(c).get_indexes("outbox"))
        assert any(
            i["column_names"] == ["available_at", "created_at"]
            and "published_at IS NULL" in str(i["dialect_options"])
            for i in indexes
        )


async def test_migration_matches_models(database: Database) -> None:
    def differences(connection: Connection) -> list[object]:
        return list(compare_metadata(MigrationContext.configure(connection), Base.metadata))

    async with database.engine.connect() as connection:
        assert await connection.run_sync(differences) == []


@pytest.mark.parametrize(
    "assignment",
    [
        "amount = 0",
        "amount = 'NaN'::numeric",
        "currency = 'GBP'",
        "status = 'unknown'",
        "status = 'succeeded'",
        "processed_at = now()",
        "expected_attempt = 4",
        "webhook_status = 'unknown'",
        "metadata = '[]'::jsonb",
        "idempotency_key = ''",
        "request_hash = 'bad'",
    ],
)
async def test_database_payment_constraints(database: Database, assignment: str) -> None:
    async with database.session_factory.begin() as session:
        await create_payment(
            session,
            PaymentCreate(amount="1", currency="EUR", webhook_url="https://example.com"),
            "constraint-key",
        )
    with pytest.raises(IntegrityError):
        async with database.engine.begin() as connection:
            await connection.execute(text(f"UPDATE payments SET {assignment}"))


@pytest.mark.parametrize(
    "assignment",
    [
        "attempt = 0",
        "attempt = 4",
        "schema_version = 2",
        "publish_attempts = -1",
        "payload = '[]'::jsonb",
        "payment_id = '00000000-0000-0000-0000-000000000000'",
    ],
)
async def test_database_outbox_constraints(database: Database, assignment: str) -> None:
    async with database.session_factory.begin() as session:
        await create_payment(
            session,
            PaymentCreate(amount="1", currency="EUR", webhook_url="https://example.com"),
            "constraint-key",
        )
    with pytest.raises(IntegrityError):
        async with database.engine.begin() as connection:
            await connection.execute(text(f"UPDATE outbox SET {assignment}"))
