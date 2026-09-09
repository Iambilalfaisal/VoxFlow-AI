from contextlib import asynccontextmanager

from fastapi import FastAPI

from api.routes import http, livekit
from db.session import engine


@asynccontextmanager
async def lifespan(app: FastAPI):
    yield
    await engine.dispose()


app = FastAPI(title="VoxFlow AI", lifespan=lifespan)

app.include_router(http.router)
app.include_router(livekit.router)
