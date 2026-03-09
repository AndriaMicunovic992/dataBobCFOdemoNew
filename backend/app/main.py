import gc
import os

# Limit Polars thread pool on memory-constrained deployments (e.g. Railway).
# Must be set before polars is imported anywhere.
os.environ.setdefault("POLARS_MAX_THREADS", "2")

from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

from app.config import settings
from app.database import async_engine, Base
import app.models  # noqa: F401 – registers models with Base.metadata
from app.api.routes import router
from app.services.calendar_svc import ensure_calendar


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup: create all tables
    async with async_engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    try:
        await ensure_calendar()
    except Exception:
        pass  # non-fatal — app starts even if calendar seeding fails
    yield
    # Shutdown: dispose engine
    await async_engine.dispose()


app = FastAPI(
    title="DataBobIQ API",
    version="0.1.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origins_list,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(router, prefix="/api")


@app.get("/api/health")
async def health_check():
    payload: dict = {"status": "ok"}
    try:
        import psutil
        mem = psutil.virtual_memory()
        payload["memory_used_mb"] = round(mem.used / 1024 / 1024)
        payload["memory_total_mb"] = round(mem.total / 1024 / 1024)
        payload["memory_percent"] = mem.percent
    except ImportError:
        pass
    gc.collect()
    return payload


if os.path.exists("static"):
    app.mount("/", StaticFiles(directory="static", html=True), name="static")
