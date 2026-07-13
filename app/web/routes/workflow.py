from __future__ import annotations

from datetime import datetime
from pathlib import Path

from fastapi import APIRouter, Depends, File, Form, HTTPException, Request, UploadFile
from fastapi.responses import FileResponse, JSONResponse, RedirectResponse
from sqlalchemy.orm import Session
from starlette import status

from app.core.bulk_jobs import bulk_scrape_jobs
from app.core.auth import require_authenticated
from app.core.templating import settings, template_context, templates
from app.database import get_db
from app.services import (
    create_or_update_show,
    get_show,
    import_shows_from_csv,
    list_shows,
    purge_show,
    queue_show_now,
    register_bulk_shows,
    upsert_show,
)
from app.web.presenters import WORKFLOW_SECTIONS, build_workflow_dashboard_view, shows_in_section


router = APIRouter()


@router.get("/")
def home():
    return RedirectResponse("/shows/dashboard", status_code=status.HTTP_303_SEE_OTHER)


@router.get("/workflow")
def dashboard(request: Request, db: Session = Depends(get_db)):
    require_authenticated(request)
    shows = list_shows(db)
    view = build_workflow_dashboard_view(shows, datetime.now())
    return templates.TemplateResponse(
        "index.html",
        template_context(
            request,
            current_page="workflow",
            can_manage=True,
            single_scrape_error=request.session.pop("single_scrape_error", ""),
            bulk_scrape_error=request.session.pop("bulk_scrape_error", ""),
            automation_error=request.session.pop("automation_error", ""),
            title="Workflow",
            weekly_sync_enabled=settings.weekly_show_sync_enabled,
            weekly_sync_source=(settings.weekly_show_sync_source_url or settings.weekly_show_sync_source_path),
            weekly_sync_mode=(
                "CSV feed"
                if (settings.weekly_show_sync_source_url or settings.weekly_show_sync_source_path)
                else "AI scan"
            ),
            weekly_sync_weekday=settings.weekly_show_sync_weekday,
            weekly_sync_hour=settings.weekly_show_sync_hour,
            weekly_sync_timezone=settings.weekly_show_sync_timezone,
            weekly_sync_lookahead_days=settings.weekly_show_sync_lookahead_days,
            **view.__dict__,
        ),
    )


@router.post("/shows/import")
async def import_shows(
    request: Request,
    file: UploadFile = File(...),
    db: Session = Depends(get_db),
):
    require_authenticated(request)
    payload = await file.read()
    if not payload:
        raise HTTPException(status_code=400, detail="The uploaded CSV was empty.")
    try:
        summary = import_shows_from_csv(db, payload, settings.default_run_offset_days)
    except ValueError as exc:
        request.session["automation_error"] = str(exc)
        return RedirectResponse("/workflow", status_code=status.HTTP_303_SEE_OTHER)

    request.session["flash_message"] = {
        "tone": "success",
        "title": "Trade shows added to the dashboard.",
        "detail": f"Created {summary.created}, updated {summary.updated}, skipped {summary.skipped}.",
    }
    return RedirectResponse("/shows/dashboard", status_code=status.HTTP_303_SEE_OTHER)


@router.post("/shows/add")
def add_single_show(
    request: Request,
    show_name: str = Form(...),
    event_date: str = Form(...),
    place: str = Form(...),
    link: str = Form(...),
    db: Session = Depends(get_db),
):
    require_authenticated(request)
    try:
        created = create_or_update_show(
            db,
            show_name=show_name,
            event_date_raw=event_date,
            place=place,
            link=link,
            run_offset_days=settings.default_run_offset_days,
        )
    except ValueError as exc:
        request.session["automation_error"] = str(exc)
        return RedirectResponse("/workflow", status_code=status.HTTP_303_SEE_OTHER)
    db.commit()
    request.session["flash_message"] = {
        "tone": "success",
        "title": "Trade show saved.",
        "detail": "The show was added to the dashboard." if created else "The existing dashboard record was updated.",
    }
    return RedirectResponse("/shows/dashboard", status_code=status.HTTP_303_SEE_OTHER)


@router.post("/scrape/single")
def scrape_single_show(
    request: Request,
    show_name: str = Form(...),
    event_date: str = Form(...),
    place: str = Form(...),
    link: str = Form(...),
    db: Session = Depends(get_db),
):
    show = None
    require_authenticated(request)
    try:
        show, _ = upsert_show(
            db,
            show_name=show_name,
            event_date_raw=event_date,
            place=place,
            link=link,
            run_offset_days=settings.default_run_offset_days,
        )
        db.commit()
        queue_show_now(db, show)
        request.session["flash_message"] = {
            "tone": "success",
            "title": "Trade show queued.",
            "detail": f"{show.name} is on the dashboard and queued for the headless worker.",
        }
    except ValueError as exc:
        request.session["single_scrape_error"] = str(exc)
        return RedirectResponse("/workflow", status_code=status.HTTP_303_SEE_OTHER)
    except Exception as exc:  # noqa: BLE001
        request.session["single_scrape_error"] = str(exc)
        if show is not None:
            return RedirectResponse(f"/shows/{show.id}", status_code=status.HTTP_303_SEE_OTHER)
        return RedirectResponse("/workflow", status_code=status.HTTP_303_SEE_OTHER)

    return RedirectResponse("/shows/dashboard", status_code=status.HTTP_303_SEE_OTHER)


@router.post("/scrape/bulk")
async def scrape_many_shows(
    request: Request,
    file: UploadFile = File(...),
    db: Session = Depends(get_db),
):
    require_authenticated(request)
    payload = await file.read()
    if not payload:
        request.session["bulk_scrape_error"] = "The uploaded CSV was empty."
        return RedirectResponse("/workflow", status_code=status.HTTP_303_SEE_OTHER)

    try:
        summary, queued_shows = register_bulk_shows(db, payload, settings.default_run_offset_days)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    queued_count = 0
    for queued_show in queued_shows:
        show = get_show(db, queued_show.show_id)
        if show is None:
            continue
        queue_show_now(db, show)
        queued_count += 1
    return JSONResponse(
        {
            "job_id": "",
            "queued": queued_count,
            "created": summary.created,
            "updated": summary.updated,
            "skipped": summary.skipped,
            "message": f"Queued {queued_count} show(s) for the headless worker.",
        }
    )


@router.get("/scrape/bulk/status/{job_id}")
def bulk_scrape_status(request: Request, job_id: str):
    require_authenticated(request)
    job = bulk_scrape_jobs.get_job(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="Bulk scrape job not found.")
    return JSONResponse(job)


@router.get("/scrape/bulk/download/{job_id}")
def bulk_scrape_download(request: Request, job_id: str):
    require_authenticated(request)
    job = bulk_scrape_jobs.get_job(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="Bulk scrape job not found.")
    if job.get("status") != "completed":
        raise HTTPException(status_code=409, detail="Bulk scrape is not finished yet.")

    archive_path = Path(str(job.get("archive_path") or "")).expanduser()
    if not archive_path.exists():
        raise HTTPException(status_code=404, detail="Bulk scrape archive was not found.")
    return FileResponse(
        archive_path,
        filename=archive_path.name,
        media_type="application/zip",
    )


@router.post("/shows/delete-all")
def delete_all_shows(
    request: Request,
    section: str = Form(...),
    db: Session = Depends(get_db),
):
    require_authenticated(request)
    if section not in WORKFLOW_SECTIONS:
        raise HTTPException(status_code=400, detail="Unknown dashboard section.")

    shows = list_shows(db)
    targets = shows_in_section(shows, section, datetime.now())
    for show in targets:
        purge_show(db, show)
    db.commit()
    return RedirectResponse("/workflow", status_code=status.HTTP_303_SEE_OTHER)
