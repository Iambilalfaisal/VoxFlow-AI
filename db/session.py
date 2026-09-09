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
