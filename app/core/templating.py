from __future__ import annotations

from fastapi import Request
from fastapi.templating import Jinja2Templates

from app.config import BASE_DIR, get_settings


settings = get_settings()
templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))


def pop_flash_message(request: Request) -> dict[str, str] | None:
    flash = request.session.pop("flash_message", None)
    if not isinstance(flash, dict):
        return None

    title = str(flash.get("title", "")).strip()
    detail = str(flash.get("detail", "")).strip()
    tone = str(flash.get("tone", "success")).strip() or "success"
    if not title and not detail:
        return None

    return {
        "title": title,
        "detail": detail,
        "tone": tone,
    }


def template_context(request: Request, **context: object) -> dict[str, object]:
    payload = {
        "request": request,
        "app_name": settings.app_name,
        "flash_message": pop_flash_message(request),
    }
    payload.update(context)
    return payload
