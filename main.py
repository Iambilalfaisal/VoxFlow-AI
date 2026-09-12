from contextlib import asynccontextmanager

from fastapi import FastAPI

from api.routes import http, livekit
from core.config import settings
from core.metrics import metrics_asgi_app
from core.tracing import configure_tracing, instrument_fastapi
from db.session import engine, read_engine


@asynccontextmanager
async def lifespan(app: FastAPI):
    yield
    await engine.dispose()
    await read_engine.dispose()


app = FastAPI(title="VoxFlow AI", lifespan=lifespan)

if settings.enable_observability:
    configure_tracing("voxflow-api")
    instrument_fastapi(app)
    app.mount("/metrics", metrics_asgi_app())

app.include_router(http.router)
app.include_router(livekit.router)
