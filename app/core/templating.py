from __future__ import annotations

from fastapi import Request
from fastapi.templating import Jinja2Templates

from app.config import BASE_DIR, get_settings


settings = get_settings()
templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))


def template_context(request: Request, **context: object) -> dict[str, object]:
    payload = {
        "request": request,
        "app_name": settings.app_name,
    }
    payload.update(context)
    return payload
