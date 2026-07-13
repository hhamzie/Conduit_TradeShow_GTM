from __future__ import annotations

from fastapi import APIRouter

from app.config import get_settings


router = APIRouter()


@router.get("/healthz")
def healthz() -> dict[str, str]:
    settings = get_settings()
    return {
        "status": "ok",
        "revision": settings.deploy_revision,
        "scrape_execution_mode": settings.scrape_execution_mode,
    }
