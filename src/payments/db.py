"""Explicit async PostgreSQL engine/session ownership; no import-time connections."""

from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine


class Database:
    def __init__(self, url: str) -> None:
        self.engine = create_async_engine(
            url,
            isolation_level="READ COMMITTED",
            pool_pre_ping=True,
            hide_parameters=True,
            connect_args={"timeout": 5, "command_timeout": 30},
        )
        self.session_factory = async_sessionmaker(self.engine, expire_on_commit=False)

    async def dispose(self) -> None:
        await self.engine.dispose()
