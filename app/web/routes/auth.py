from __future__ import annotations

from fastapi import APIRouter, Form, Request
from fastapi.responses import RedirectResponse
from starlette import status

from app.core.auth import is_authenticated, log_user_in, log_user_out, validate_credentials
from app.core.templating import template_context, templates


router = APIRouter()


@router.get("/login")
def login_page(request: Request):
    if is_authenticated(request):
        return RedirectResponse("/analytics", status_code=status.HTTP_303_SEE_OTHER)
    return templates.TemplateResponse(
        "login.html",
        template_context(request, error=""),
    )


@router.post("/login")
def login_submit(
    request: Request,
    username: str = Form(...),
    password: str = Form(...),
):
    if not validate_credentials(username, password):
        return templates.TemplateResponse(
            "login.html",
            template_context(request, error="Incorrect username or password."),
            status_code=status.HTTP_400_BAD_REQUEST,
        )

    log_user_in(request, username)
    return RedirectResponse("/analytics", status_code=status.HTTP_303_SEE_OTHER)


@router.post("/logout")
def logout(request: Request):
    log_user_out(request)
    return RedirectResponse("/analytics", status_code=status.HTTP_303_SEE_OTHER)
