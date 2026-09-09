from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    environment: str = "development"

    # Points at pgbouncer (transaction pooling), not Postgres directly - this
    # is what the FastAPI app uses at runtime.
    database_url: str = "postgresql+asyncpg://voxflow:voxflow@localhost:6432/voxflow"

    # Alembic connects directly to Postgres, bypassing pgbouncer - DDL over a
    # transaction-pooled connection is unreliable (advisory locks, multi-
    # statement transactions, etc. don't survive pgbouncer handing the
    # connection to a different session mid-migration).
    migrations_database_url: str = "postgresql+asyncpg://voxflow:voxflow@localhost:5433/voxflow"

    redis_url: str = "redis://localhost:6379/0"

    livekit_url: str
    livekit_api_key: str
    livekit_api_secret: str

    jwt_secret: str
    jwt_algorithm: str = "HS256"
    jwt_expire_minutes: int = 60 * 24


settings = Settings()
