from __future__ import annotations

from fastapi import APIRouter, Depends, Request
from sqlalchemy.orm import Session

from app.core.auth import require_authenticated
from app.core.templating import template_context, templates
from app.database import get_db
from app.pipedrive_analytics import get_latest_pipedrive_analytics


router = APIRouter()


@router.get("/analytics")
def pipedrive_analytics_dashboard(
    request: Request,
    db: Session = Depends(get_db),
):
    require_authenticated(request)
    payload = get_latest_pipedrive_analytics(db)
    return templates.TemplateResponse(
        "pipedrive_analytics.html",
        template_context(
            request,
            title="Pipedrive Sales Analytics",
            current_page="pipedrive_analytics",
            analytics=payload,
        ),
    )
