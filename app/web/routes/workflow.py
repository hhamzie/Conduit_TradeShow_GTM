from __future__ import annotations

from datetime import datetime
from pathlib import Path

from fastapi import APIRouter, Depends, File, Form, HTTPException, Request, UploadFile
from fastapi.responses import FileResponse, JSONResponse, RedirectResponse
from sqlalchemy.orm import Session
from starlette import status

from app.core.auth import require_authenticated
from app.core.bulk_jobs import bulk_scrape_jobs
from app.core.templating import settings, template_context, templates
from app.database import get_db
from app.services import (
    create_or_update_show,
    import_shows_from_csv,
    list_shows,
    run_single_show_scrape,
)
from app.web.presenters import WORKFLOW_SECTIONS, build_workflow_dashboard_view, shows_in_section


router = APIRouter()


@router.get("/")
def dashboard(request: Request, db: Session = Depends(get_db)):
    require_authenticated(request)
    shows = list_shows(db)
    view = build_workflow_dashboard_view(shows, datetime.now())
    return templates.TemplateResponse(
        "index.html",
        template_context(
            request,
            current_page="workflow",
            default_offset=settings.default_run_offset_days,
            single_scrape_error=request.session.pop("single_scrape_error", ""),
            bulk_scrape_error=request.session.pop("bulk_scrape_error", ""),
            automation_error=request.session.pop("automation_error", ""),
            title="Workflow",
            **view.__dict__,
        ),
    )


@router.post("/shows/import")
async def import_shows(
    request: Request,
    file: UploadFile = File(...),
    run_offset_days: int = Form(...),
    db: Session = Depends(get_db),
):
    require_authenticated(request)
    payload = await file.read()
    if not payload:
        raise HTTPException(status_code=400, detail="The uploaded CSV was empty.")
    try:
        import_shows_from_csv(db, payload, run_offset_days)
    except ValueError as exc:
        request.session["automation_error"] = str(exc)
    return RedirectResponse("/", status_code=status.HTTP_303_SEE_OTHER)


@router.post("/shows/add")
def add_single_show(
    request: Request,
    show_name: str = Form(...),
    event_date: str = Form(...),
    place: str = Form(...),
    link: str = Form(...),
    run_offset_days: int = Form(...),
    db: Session = Depends(get_db),
):
    require_authenticated(request)
    try:
        create_or_update_show(
            db,
            show_name=show_name,
            event_date_raw=event_date,
            place=place,
            link=link,
            run_offset_days=run_offset_days,
        )
    except ValueError as exc:
        request.session["automation_error"] = str(exc)
        return RedirectResponse("/", status_code=status.HTTP_303_SEE_OTHER)
    db.commit()
    return RedirectResponse("/", status_code=status.HTTP_303_SEE_OTHER)


@router.post("/scrape/single")
def scrape_single_show(
    request: Request,
    show_name: str = Form(...),
    event_date: str = Form(""),
    place: str = Form(...),
    link: str = Form(...),
):
    require_authenticated(request)
    try:
        result = run_single_show_scrape(
            show_name=show_name,
            event_date_raw=event_date,
            place=place,
            link=link,
        )
    except ValueError as exc:
        request.session["single_scrape_error"] = str(exc)
        return RedirectResponse("/", status_code=status.HTTP_303_SEE_OTHER)
    except Exception as exc:  # noqa: BLE001
        request.session["single_scrape_error"] = str(exc)
        return RedirectResponse("/", status_code=status.HTTP_303_SEE_OTHER)

    return FileResponse(
        result.output_path,
        filename=result.output_path.name,
        media_type="text/csv",
    )


@router.post("/scrape/bulk")
async def scrape_many_shows(
    request: Request,
    file: UploadFile = File(...),
):
    require_authenticated(request)
    payload = await file.read()
    if not payload:
        request.session["bulk_scrape_error"] = "The uploaded CSV was empty."
        return RedirectResponse("/", status_code=status.HTTP_303_SEE_OTHER)

    job_id = bulk_scrape_jobs.start_job(payload)
    return JSONResponse({"job_id": job_id})


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
        db.delete(show)
    db.commit()
    return RedirectResponse("/", status_code=status.HTTP_303_SEE_OTHER)
