from __future__ import annotations

from datetime import datetime
from pathlib import Path

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import FileResponse, RedirectResponse
from sqlalchemy.orm import Session
from starlette import status

from app.core.auth import require_authenticated
from app.core.templating import template_context, templates
from app.database import get_db
from app.show_intelligence import build_show_analysis, build_show_analyses
from app.services import (
    approve_show,
    get_show,
    launch_show,
    list_shows,
    pause_show,
    queue_show_now,
    sync_show_from_clay,
)
from app.web.presenters import build_show_notice, summarize_show_error


router = APIRouter()


def _get_show_or_404(db: Session, show_id: int):
    show = get_show(db, show_id)
    if show is None:
        raise HTTPException(status_code=404, detail="Show not found.")
    return show


def _file_response(raw_path: str, *, not_found_detail: str) -> FileResponse:
    if not raw_path:
        raise HTTPException(status_code=404, detail=not_found_detail)

    export_path = Path(raw_path).expanduser()
    if not export_path.exists():
        raise HTTPException(status_code=404, detail=f"{not_found_detail.rstrip('.')} file no longer exists.")

    return FileResponse(export_path, filename=export_path.name, media_type="text/csv")


@router.get("/shows/dashboard")
def show_dashboard(request: Request, db: Session = Depends(get_db)):
    require_authenticated(request)
    shows = list_shows(db)
    analyses = build_show_analyses(shows, today=datetime.now().date(), company_limit=8)
    return templates.TemplateResponse(
        "show_dashboard.html",
        template_context(
            request,
            current_page="show_dashboard",
            title="Show Dashboard",
            analyses=analyses,
            tracked_count=len(analyses),
            high_priority_count=sum(1 for analysis in analyses if analysis.priority_slug == "high"),
            export_ready_count=sum(1 for analysis in analyses if analysis.export_ready),
            total_exhibitors=sum(analysis.exhibitor_count for analysis in analyses),
        ),
    )


@router.get("/shows/{show_id}")
def show_detail(show_id: int, request: Request, db: Session = Depends(get_db)):
    require_authenticated(request)
    show = _get_show_or_404(db, show_id)
    export_path = Path(show.latest_export_path) if show.latest_export_path else None
    enriched_export_path = Path(show.enriched_export_path) if show.enriched_export_path else None
    smartlead_ready_path = Path(show.smartlead_ready_export_path) if show.smartlead_ready_export_path else None
    return templates.TemplateResponse(
        "show_detail.html",
        template_context(
            request,
            current_page="show_dashboard",
            title=show.name,
            show=show,
            analysis=build_show_analysis(show, today=datetime.now().date(), company_limit=18),
            export_path=export_path,
            enriched_export_path=enriched_export_path,
            smartlead_ready_path=smartlead_ready_path,
            notice=build_show_notice(show),
            error_summary=summarize_show_error(show.last_error),
        ),
    )


@router.post("/shows/{show_id}/delete")
def delete_show(show_id: int, request: Request, db: Session = Depends(get_db)):
    require_authenticated(request)
    show = _get_show_or_404(db, show_id)
    db.delete(show)
    db.commit()
    return RedirectResponse("/", status_code=status.HTTP_303_SEE_OTHER)


@router.post("/shows/{show_id}/run-now")
def run_show_now(show_id: int, request: Request, db: Session = Depends(get_db)):
    require_authenticated(request)
    show = _get_show_or_404(db, show_id)
    queue_show_now(db, show)
    return RedirectResponse(f"/shows/{show_id}", status_code=status.HTTP_303_SEE_OTHER)


@router.get("/shows/{show_id}/export")
def download_export(show_id: int, request: Request, db: Session = Depends(get_db)):
    require_authenticated(request)
    show = _get_show_or_404(db, show_id)
    return _file_response(show.latest_export_path, not_found_detail="Export not found.")


@router.get("/shows/{show_id}/enriched-export")
def download_enriched_export(show_id: int, request: Request, db: Session = Depends(get_db)):
    require_authenticated(request)
    show = _get_show_or_404(db, show_id)
    return _file_response(show.enriched_export_path, not_found_detail="Enriched export not found.")


@router.get("/shows/{show_id}/smartlead-export")
def download_smartlead_export(show_id: int, request: Request, db: Session = Depends(get_db)):
    require_authenticated(request)
    show = _get_show_or_404(db, show_id)
    return _file_response(show.smartlead_ready_export_path, not_found_detail="Smartlead-ready export not found.")


@router.post("/shows/{show_id}/approve")
def approve_show_route(show_id: int, request: Request, db: Session = Depends(get_db)):
    require_authenticated(request)
    show = _get_show_or_404(db, show_id)
    approve_show(db, show)
    return RedirectResponse(f"/shows/{show_id}", status_code=status.HTTP_303_SEE_OTHER)


@router.post("/shows/{show_id}/sync-clay")
def sync_show_from_clay_route(show_id: int, request: Request, db: Session = Depends(get_db)):
    require_authenticated(request)
    show = _get_show_or_404(db, show_id)
    sync_show_from_clay(db, show)
    return RedirectResponse(f"/shows/{show_id}", status_code=status.HTTP_303_SEE_OTHER)


@router.post("/shows/{show_id}/launch")
def launch_show_route(show_id: int, request: Request, db: Session = Depends(get_db)):
    require_authenticated(request)
    show = _get_show_or_404(db, show_id)
    try:
        launch_show(db, show)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return RedirectResponse(f"/shows/{show_id}", status_code=status.HTTP_303_SEE_OTHER)


@router.post("/shows/{show_id}/pause")
def pause_show_route(show_id: int, request: Request, db: Session = Depends(get_db)):
    require_authenticated(request)
    show = _get_show_or_404(db, show_id)
    try:
        pause_show(db, show)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return RedirectResponse(f"/shows/{show_id}", status_code=status.HTTP_303_SEE_OTHER)
