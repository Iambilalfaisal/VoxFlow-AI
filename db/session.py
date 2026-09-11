from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.pool import NullPool

from core.config import settings

# pgbouncer (transaction pooling mode) is already doing connection pooling in
# front of Postgres, so SQLAlchemy must not pool on top of it (NullPool), and
# asyncpg must not cache prepared statements per-connection — in transaction
# mode a "connection" can be handed to a different backend session between
# statements, which would make a cached statement id point at the wrong plan.
engine = create_async_engine(
    settings.database_url,
    poolclass=NullPool,
    connect_args={"statement_cache_size": 0},
)

async_session_maker = async_sessionmaker(engine, expire_on_commit=False)


async def get_session() -> AsyncSession:
    async with async_session_maker() as session:
        yield session


def _resolve_read_dsn() -> str:
    """Points at settings.database_read_url once a read replica exists;
    falls back to the same write engine's DSN today. Extracted to a
    function (rather than inlined below) so the fallback is directly
    unit-testable without reconstructing real engines."""
    return settings.database_read_url or settings.database_url


# Read-path seam. Kept as a separate engine (not just a separate
# sessionmaker on `engine`) so a future replica DSN takes effect without
# touching the write path.
read_engine = create_async_engine(
    _resolve_read_dsn(),
    poolclass=NullPool,
    connect_args={"statement_cache_size": 0},
)

read_session_maker = async_sessionmaker(read_engine, expire_on_commit=False)


async def get_read_session() -> AsyncSession:
    async with read_session_maker() as session:
        yield session
