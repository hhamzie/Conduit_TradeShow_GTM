from __future__ import annotations

from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from starlette.middleware.sessions import SessionMiddleware

from app.config import BASE_DIR, get_settings
from app.database import init_db
from app.web.routes.analytics import router as analytics_router
from app.web.routes.auth import router as auth_router
from app.web.routes.clay_webhooks import router as clay_webhooks_router
from app.web.routes.health import router as health_router
from app.web.routes.shows import router as shows_router
from app.web.routes.workflow import router as workflow_router


settings = get_settings()


@asynccontextmanager
async def lifespan(_app: FastAPI):
    init_db()
    yield


def create_app() -> FastAPI:
    app = FastAPI(title=settings.app_name, lifespan=lifespan)
    app.add_middleware(
        SessionMiddleware,
        secret_key=settings.session_secret,
        same_site="lax",
    )
    app.mount("/static", StaticFiles(directory=str(BASE_DIR / "static")), name="static")
    app.include_router(health_router)
    app.include_router(auth_router)
    app.include_router(analytics_router)
    app.include_router(clay_webhooks_router)
    app.include_router(workflow_router)
    app.include_router(shows_router)
    return app
