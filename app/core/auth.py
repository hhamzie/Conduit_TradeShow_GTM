from __future__ import annotations

import hmac

from fastapi import HTTPException, Request
from starlette import status

from app.config import get_settings


settings = get_settings()


def is_authenticated(request: Request) -> bool:
    return bool(request.session.get("authenticated"))


def can_manage(request: Request) -> bool:
    return is_authenticated(request)


def require_authenticated(request: Request) -> None:
    if not is_authenticated(request):
        raise HTTPException(
            status_code=status.HTTP_303_SEE_OTHER,
            headers={"Location": "/login"},
        )


def validate_credentials(username: str, password: str) -> bool:
    valid_username = hmac.compare_digest(username, settings.dashboard_username)
    valid_password = hmac.compare_digest(password, settings.dashboard_password)
    return valid_username and valid_password


def log_user_in(request: Request, username: str) -> None:
    request.session["authenticated"] = True
    request.session["username"] = username


def log_user_out(request: Request) -> None:
    request.session.clear()
