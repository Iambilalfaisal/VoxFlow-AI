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

    # Read-path seam: unset today, so reads fall back to database_url (same
    # Postgres/pgbouncer as writes). Set this once a read replica exists.
    database_read_url: str | None = None

    # History queue (Redis Streams) - see services/queue.py and
    # worker/history_writer.py.
    history_stream_name: str = "voxflow:history-events"
    history_dlq_stream_name: str = "voxflow:history-events:dlq"
    history_consumer_group: str = "history-writer"
    history_writer_batch_size: int = 50
    history_writer_block_ms: int = 5000
    history_writer_max_deliveries: int = 5
    # How long a stream entry sits unacked before XAUTOCLAIM will reclaim it
    # (e.g. the writer that read it crashed before ack/dead-letter).
    history_writer_min_idle_ms: int = 30000
    history_publish_max_retries: int = 3
    history_publish_retry_backoff_seconds: float = 0.5  # doubles each attempt

    livekit_url: str
    livekit_api_key: str
    livekit_api_secret: str

    jwt_secret: str
    jwt_algorithm: str = "HS256"
    jwt_expire_minutes: int = 60 * 24


settings = Settings()
