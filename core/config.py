from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    environment: str = "development"

    # Points at pgbouncer (transaction pooling), not Postgres directly.
    database_url: str = "postgresql+asyncpg://voxflow:voxflow@localhost:6432/voxflow"
    redis_url: str = "redis://localhost:6379/0"

    livekit_url: str
    livekit_api_key: str
    livekit_api_secret: str

    jwt_secret: str
    jwt_algorithm: str = "HS256"
    jwt_expire_minutes: int = 60 * 24


settings = Settings()
