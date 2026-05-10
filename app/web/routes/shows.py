from __future__ import annotations

from datetime import datetime
import json
from pathlib import Path

from fastapi import APIRouter, Depends, File, Form, HTTPException, Request, UploadFile
from fastapi.responses import FileResponse, JSONResponse, RedirectResponse, Response
from sqlalchemy.orm import Session
from starlette import status

from app.config import get_settings
from app.core.auth import can_manage, require_authenticated
from app.core.bulk_jobs import bulk_scrape_jobs
from app.core.templating import template_context, templates
from app.database import get_db
from app.guide_services import (
    create_guide_row,
    delete_guide_row,
    export_trade_show_guide_workbook,
    import_trade_show_guide_workbook,
    rebuild_trade_show_guides,
    update_guide_row,
)
from app.models import ShowGuideRow
from app.providers import ensure_smartlead_campaign, fetch_smartlead_campaign_option, list_smartlead_campaign_options
from app.show_guides import build_guide_sheet_views, get_guide_sheet
from app.show_intelligence import build_show_analysis, build_show_analyses
from app.services import (
    approve_show,
    build_outbound_plan,
    get_show,
    launch_show,
    list_shows,
    manual_trade_show_scan_already_ran_today,
    pause_show,
    QueuedBulkShow,
    queue_show_now,
    record_manual_trade_show_scan,
    start_outbound_campaign,
    sync_show_from_clay,
    update_show,
    upsert_show,
)
from app.trade_show_feeder import TradeShowScanError, scan_upcoming_trade_shows_with_debug
from app.web.presenters import (
    build_show_notice,
    get_run_status_label,
    get_show_status_label,
    summarize_show_error,
)


router = APIRouter()


def _smartlead_campaign_url(campaign_id: int | None) -> str:
    if campaign_id:
        return f"https://app.smartlead.ai/app/campaigns/{campaign_id}"
    return "https://app.smartlead.ai/app/campaigns"


def _get_show_or_404(db: Session, show_id: int):
    show = get_show(db, show_id)
    if show is None:
        raise HTTPException(status_code=404, detail="Show not found.")
    return show


def _sanitize_next_path(next_path: str | None, *, fallback: str) -> str:
    raw = str(next_path or "").strip()
    if raw.startswith("/"):
        return raw
    return fallback


def _file_response(raw_path: str, *, not_found_detail: str) -> FileResponse:
    if not raw_path:
        raise HTTPException(status_code=404, detail=not_found_detail)

    export_path = Path(raw_path).expanduser()
    if not export_path.exists():
        raise HTTPException(status_code=404, detail=f"{not_found_detail.rstrip('.')} file no longer exists.")

    return FileResponse(export_path, filename=export_path.name, media_type="text/csv")


def _get_guide_row_or_404(db: Session, show_id: int, row_id: int) -> ShowGuideRow:
    row = db.get(ShowGuideRow, row_id)
    if row is None or row.show_id != show_id:
        raise HTTPException(status_code=404, detail="Guide row not found.")
    return row


def _serialize_scan_candidate(candidate) -> dict[str, str]:
    return {
        "show_name": candidate.show_name,
        "event_date_raw": candidate.event_date_raw,
        "place": candidate.place,
        "link": candidate.link,
        "summary": candidate.summary,
    }


def _serialize_scan_debug(debug) -> dict[str, object]:
    return {
        "start_date": debug.start_date,
        "end_date": debug.end_date,
        "lookahead_days": debug.lookahead_days,
        "candidate_count": debug.candidate_count,
        "passes": [
            {
                "pass_label": report.pass_label,
                "model_used": report.model_used,
                "raw_count": report.raw_count,
                "source_count": report.source_count,
                "accepted_count": report.accepted_count,
                "filtered_missing_fields": report.filtered_missing_fields,
                "filtered_non_physical": report.filtered_non_physical,
                "filtered_non_official_source": report.filtered_non_official_source,
                "filtered_duplicate": report.filtered_duplicate,
                "remapped_to_curated_source": report.remapped_to_curated_source,
                "sample_links": list(report.sample_links),
                "sample_sources": list(report.sample_sources),
                "error_message": report.error_message,
            }
            for report in debug.pass_reports
        ],
    }


@router.get("/shows/dashboard")
def show_dashboard(request: Request, db: Session = Depends(get_db)):
    shows = list_shows(db)
    analyses = build_show_analyses(shows, today=datetime.now().date(), company_limit=8)
    outbound_plans = {analysis.show.id: build_outbound_plan(db, analysis.show) for analysis in analyses}
    return templates.TemplateResponse(
        "show_dashboard.html",
        template_context(
            request,
            current_page="show_dashboard",
            can_manage=can_manage(request),
            title="Show Dashboard",
            analyses=analyses,
            outbound_plans=outbound_plans,
            show_status_label_for=get_show_status_label,
            tracked_count=len(analyses),
            upcoming_count=sum(1 for analysis in analyses if analysis.days_until_event >= 0),
            smartlead_campaign_count=sum(1 for analysis in analyses if analysis.show.smartlead_campaign_id),
            running_campaign_count=sum(1 for analysis in analyses if analysis.has_running_campaign),
            guide_ready_count=sum(1 for analysis in analyses if analysis.guide_company_count > 0),
            total_exhibitors=sum(analysis.exhibitor_count for analysis in analyses),
        ),
    )


@router.get("/shows/{show_id}")
def show_detail(show_id: int, request: Request, db: Session = Depends(get_db)):
    show = _get_show_or_404(db, show_id)
    export_path = Path(show.latest_export_path) if show.latest_export_path else None
    enriched_export_path = Path(show.enriched_export_path) if show.enriched_export_path else None
    smartlead_ready_path = Path(show.smartlead_ready_export_path) if show.smartlead_ready_export_path else None
    return templates.TemplateResponse(
        "show_detail.html",
        template_context(
            request,
            current_page="show_profile",
            can_manage=can_manage(request),
            title=f"{show.name} Profile",
            context_tab_label=f"{show.name} Profile",
            context_tab_href=f"/shows/{show.id}",
            show=show,
            show_status_label=get_show_status_label(show.status),
            run_status_label_for=get_run_status_label,
            analysis=build_show_analysis(show, today=datetime.now().date(), company_limit=60),
            guide_sheets=build_guide_sheet_views(show),
            export_path=export_path,
            enriched_export_path=enriched_export_path,
            smartlead_ready_path=smartlead_ready_path,
            notice=build_show_notice(show, datetime.now()),
            error_summary=summarize_show_error(show.last_error),
            smartlead_campaign_options=list_smartlead_campaign_options() if can_manage(request) else [],
            smartlead_campaign_url=_smartlead_campaign_url(show.smartlead_campaign_id),
            outbound_plan=build_outbound_plan(db, show),
        ),
    )


@router.get("/shows/{show_id}/guide")
def show_guide(show_id: int, request: Request, db: Session = Depends(get_db)):
    show = _get_show_or_404(db, show_id)
    return templates.TemplateResponse(
        "show_guide.html",
        template_context(
            request,
            current_page="show_guide",
            can_manage=can_manage(request),
            title=f"{show.name} Guide",
            context_tab_label=f"{show.name} Guide",
            context_tab_href=f"/shows/{show.id}/guide",
            show=show,
            analysis=build_show_analysis(show, today=datetime.now().date(), company_limit=18),
            guide_sheets=build_guide_sheet_views(show),
            smartlead_campaign_url=_smartlead_campaign_url(show.smartlead_campaign_id),
        ),
    )


@router.get("/shows/{show_id}/guide/download")
def download_guide_workbook(show_id: int, request: Request, db: Session = Depends(get_db)):
    show = _get_show_or_404(db, show_id)
    workbook_bytes = export_trade_show_guide_workbook(show)
    filename = f"{show.name.strip().replace('/', '-').replace(' ', '-')}_guide.xlsx"
    headers = {
        "Content-Disposition": f'attachment; filename="{filename}"',
    }
    return Response(
        content=workbook_bytes,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers=headers,
    )


@router.post("/shows/{show_id}/update")
def update_show_route(
    show_id: int,
    request: Request,
    show_name: str = Form(...),
    event_date: str = Form(...),
    place: str = Form(...),
    link: str = Form(...),
    run_offset_days: int = Form(...),
    db: Session = Depends(get_db),
):
    require_authenticated(request)
    show = _get_show_or_404(db, show_id)
    try:
        update_show(
            db,
            show=show,
            show_name=show_name,
            event_date_raw=event_date,
            place=place,
            link=link,
            run_offset_days=run_offset_days,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return RedirectResponse(f"/shows/{show_id}", status_code=status.HTTP_303_SEE_OTHER)


@router.post("/shows/{show_id}/delete")
def delete_show(show_id: int, request: Request, db: Session = Depends(get_db)):
    require_authenticated(request)
    show = _get_show_or_404(db, show_id)
    db.delete(show)
    db.commit()
    return RedirectResponse("/shows/dashboard", status_code=status.HTTP_303_SEE_OTHER)


@router.post("/shows/bulk/delete")
def delete_selected_shows(
    request: Request,
    show_ids: list[int] = Form(default_factory=list),
    next_path: str = Form("/shows/dashboard"),
    db: Session = Depends(get_db),
):
    require_authenticated(request)
    target_path = _sanitize_next_path(next_path, fallback="/shows/dashboard")
    unique_ids = list(dict.fromkeys(show_ids))
    if not unique_ids:
        request.session["flash_message"] = {
            "tone": "warning",
            "title": "No shows selected.",
            "detail": "Select at least one show first.",
        }
        return RedirectResponse(target_path, status_code=status.HTTP_303_SEE_OTHER)

    deleted = 0
    for show_id in unique_ids:
        show = get_show(db, show_id)
        if show is None:
            continue
        db.delete(show)
        deleted += 1
    db.commit()
    request.session["flash_message"] = {
        "tone": "success",
        "title": "Selected shows deleted.",
        "detail": f"Deleted {deleted} show(s).",
    }
    return RedirectResponse(target_path, status_code=status.HTTP_303_SEE_OTHER)


@router.post("/shows/bulk/smartlead")
def configure_selected_smartlead(
    request: Request,
    show_ids: list[int] = Form(default_factory=list),
    next_path: str = Form("/shows/dashboard"),
    db: Session = Depends(get_db),
):
    require_authenticated(request)
    target_path = _sanitize_next_path(next_path, fallback="/shows/dashboard")
    unique_ids = list(dict.fromkeys(show_ids))
    if not unique_ids:
        request.session["flash_message"] = {
            "tone": "warning",
            "title": "No shows selected.",
            "detail": "Select at least one show first.",
        }
        return RedirectResponse(target_path, status_code=status.HTTP_303_SEE_OTHER)

    linked = 0
    skipped = 0
    failed = 0
    for show_id in unique_ids:
        show = get_show(db, show_id)
        if show is None:
            failed += 1
            continue
        if show.smartlead_campaign_id:
            skipped += 1
            continue

        result = ensure_smartlead_campaign(show)
        if result.status != "success":
            failed += 1
            continue
        show.smartlead_campaign_id = result.campaign_id
        show.smartlead_campaign_name = result.campaign_name
        linked += 1

    db.commit()
    tone = "success" if failed == 0 else "warning"
    request.session["flash_message"] = {
        "tone": tone,
        "title": "Smartlead batch setup finished.",
        "detail": f"Linked {linked}, skipped {skipped}, failed {failed}.",
    }
    return RedirectResponse(target_path, status_code=status.HTTP_303_SEE_OTHER)


@router.post("/shows/scan-upcoming")
def scan_upcoming_trade_shows_route(
    request: Request,
    query_hint: str = Form(""),
    db: Session = Depends(get_db),
):
    require_authenticated(request)
    if manual_trade_show_scan_already_ran_today(db):
        return JSONResponse(
            {
                "status": "locked",
                "message": "Search has already been done today.",
                "candidates": [],
            },
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
        )
    try:
        scan_result = scan_upcoming_trade_shows_with_debug(query_hint=query_hint)
    except ValueError as exc:
        return JSONResponse(
            {
                "status": "error",
                "message": str(exc),
                "candidates": [],
                "debug": None,
            },
            status_code=status.HTTP_400_BAD_REQUEST,
        )
    except TradeShowScanError as exc:
        return JSONResponse(
            {
                "status": "error",
                "message": str(exc),
                "candidates": [],
                "debug": None,
            },
            status_code=exc.status_code,
        )
    except Exception as exc:  # noqa: BLE001
        return JSONResponse(
            {
                "status": "error",
                "message": f"Scan failed: {exc}",
                "candidates": [],
                "debug": None,
            },
            status_code=status.HTTP_502_BAD_GATEWAY,
        )

    candidates = scan_result.candidates
    debug_payload = _serialize_scan_debug(scan_result.debug)
    record_manual_trade_show_scan(db)
    db.commit()

    if not candidates:
        return JSONResponse(
            {
                "status": "empty",
                "message": "No upcoming B2B physical-goods trade shows were found right now.",
                "candidates": [],
                "debug": debug_payload,
            }
        )

    serialized = [_serialize_scan_candidate(candidate) for candidate in candidates]
    return JSONResponse(
        {
            "status": "ready",
            "message": f"Found {len(serialized)} upcoming trade show(s).",
            "count": len(serialized),
            "candidates": serialized,
            "debug": debug_payload,
        }
    )


@router.post("/shows/scan-upcoming/confirm")
def confirm_scanned_trade_shows_route(
    request: Request,
    candidates_json: str = Form(...),
    scrape_after_add: str = Form("false"),
    db: Session = Depends(get_db),
):
    require_authenticated(request)
    try:
        payload = json.loads(candidates_json)
    except json.JSONDecodeError as exc:
        raise HTTPException(status_code=400, detail="Invalid scan payload.") from exc
    if not isinstance(payload, list):
        raise HTTPException(status_code=400, detail="Invalid scan payload.")
    if not payload:
        raise HTTPException(status_code=400, detail="Select at least one scanned show.")

    created = 0
    updated = 0
    skipped = 0
    queued_shows: list[QueuedBulkShow] = []
    for item in payload:
        if not isinstance(item, dict):
            skipped += 1
            continue
        show_name = str(item.get("show_name") or "").strip()
        event_date_raw = str(item.get("event_date_raw") or item.get("event_date") or "").strip()
        place = str(item.get("place") or "").strip()
        link = str(item.get("link") or "").strip()
        if not (show_name and event_date_raw and place and link):
            skipped += 1
            continue
        try:
            show, created_now = upsert_show(
                db,
                show_name=show_name,
                event_date_raw=event_date_raw,
                place=place,
                link=link,
                run_offset_days=get_settings().default_run_offset_days,
            )
        except ValueError:
            skipped += 1
            continue
        if created_now:
            created += 1
        else:
            updated += 1
        queued_shows.append(
            QueuedBulkShow(
                show_id=show.id,
                show_name=show.name,
                event_date_raw=show.event_date.isoformat(),
                place=show.place,
                link=show.source_url,
            )
        )

    db.commit()
    should_scrape = str(scrape_after_add).strip().lower() in {"1", "true", "yes"}
    job_id = ""
    if should_scrape and queued_shows:
        job_id = bulk_scrape_jobs.start_job(
            b"",
            run_offset_days=get_settings().default_run_offset_days,
            queued_shows=queued_shows,
        )
    request.session["flash_message"] = {
        "tone": "success",
        "title": "Trade show scan applied.",
        "detail": (
            f"Added {created}, updated {updated}, skipped {skipped}. "
            f"{'Started scrape for selected shows.' if should_scrape and queued_shows else ''}"
        ).strip(),
    }
    return JSONResponse({"ok": True, "redirect": "/shows/dashboard", "job_id": job_id})


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


@router.post("/shows/{show_id}/outbound/start")
def start_outbound_campaign_route(show_id: int, request: Request, db: Session = Depends(get_db)):
    require_authenticated(request)
    show = _get_show_or_404(db, show_id)
    try:
        plan = start_outbound_campaign(db, show)
    except ValueError as exc:
        request.session["flash_message"] = {
            "tone": "warning",
            "title": "Outbound could not start.",
            "detail": str(exc),
        }
        return RedirectResponse("/shows/dashboard", status_code=status.HTTP_303_SEE_OTHER)

    request.session["flash_message"] = {
        "tone": "success",
        "title": "Outbound campaign started.",
        "detail": (
            f"{show.name} will send {plan.email_count} emails and {plan.linkedin_count} LinkedIn messages "
            f"over the next {plan.weeks} weeks."
        ),
    }
    return RedirectResponse("/shows/dashboard", status_code=status.HTTP_303_SEE_OTHER)


@router.post("/shows/{show_id}/smartlead/setup")
def configure_smartlead_route(
    show_id: int,
    request: Request,
    campaign_mode: str = Form(...),
    existing_campaign_id: str = Form(""),
    db: Session = Depends(get_db),
):
    require_authenticated(request)
    show = _get_show_or_404(db, show_id)
    mode = campaign_mode.strip().lower()

    if mode == "smart":
        result = ensure_smartlead_campaign(show)
        if result.status != "success":
            raise HTTPException(status_code=400, detail=result.message)
        show.smartlead_campaign_id = result.campaign_id
        show.smartlead_campaign_name = result.campaign_name
        db.commit()
        request.session["flash_message"] = {
            "tone": "success",
            "title": "Smartlead campaign configured.",
            "detail": result.message,
        }
        return RedirectResponse(f"/shows/{show_id}", status_code=status.HTTP_303_SEE_OTHER)

    if mode != "existing":
        raise HTTPException(status_code=400, detail="Choose a valid Smartlead setup mode.")
    if not existing_campaign_id.strip().isdigit():
        raise HTTPException(status_code=400, detail="Enter a valid existing Smartlead campaign ID.")

    campaign_id = int(existing_campaign_id.strip())
    campaign = fetch_smartlead_campaign_option(campaign_id)
    if campaign is None:
        raise HTTPException(status_code=404, detail=f"Smartlead campaign {campaign_id} was not found.")

    show.smartlead_campaign_id = campaign_id
    show.smartlead_campaign_name = str(campaign["name"])
    db.commit()
    request.session["flash_message"] = {
        "tone": "success",
        "title": "Smartlead campaign linked.",
        "detail": f"{show.name} will use Smartlead campaign {show.smartlead_campaign_name} ({campaign_id}).",
    }
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


@router.post("/shows/{show_id}/guide/build")
def build_trade_show_guide_route(show_id: int, request: Request, db: Session = Depends(get_db)):
    require_authenticated(request)
    show = _get_show_or_404(db, show_id)
    try:
        company_count, booth_count = rebuild_trade_show_guides(db, show=show)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    request.session["flash_message"] = {
        "tone": "success",
        "title": "Trade show guide built.",
        "detail": f"Created {company_count} company summary rows and {booth_count} booth grouping rows.",
    }
    return RedirectResponse(f"/shows/{show_id}#sheet-company_summary", status_code=status.HTTP_303_SEE_OTHER)


@router.post("/shows/{show_id}/guide/upload")
async def upload_guide_workbook_route(
    show_id: int,
    request: Request,
    workbook: UploadFile = File(...),
    db: Session = Depends(get_db),
):
    require_authenticated(request)
    show = _get_show_or_404(db, show_id)
    filename = (workbook.filename or "").strip()
    if not filename.lower().endswith((".xlsx", ".xlsm")):
        raise HTTPException(status_code=400, detail="Upload an .xlsx or .xlsm workbook.")

    payload = await workbook.read()
    try:
        counts = import_trade_show_guide_workbook(db, show=show, workbook_bytes=payload)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    request.session["flash_message"] = {
        "tone": "success",
        "title": "Guide workbook imported.",
        "detail": (
            f"Imported {counts.get('company_summary', 0)} company rows and "
            f"{counts.get('booth_category_groups', 0)} booth rows from {filename or 'the workbook'}."
        ),
    }
    return RedirectResponse(f"/shows/{show_id}/guide", status_code=status.HTTP_303_SEE_OTHER)


@router.post("/shows/{show_id}/guide/{sheet_key}/add")
async def add_guide_row_route(show_id: int, sheet_key: str, request: Request, db: Session = Depends(get_db)):
    require_authenticated(request)
    show = _get_show_or_404(db, show_id)
    try:
        get_guide_sheet(sheet_key)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    payload = dict(await request.form())
    create_guide_row(db, show=show, sheet_key=sheet_key, payload=payload)
    return RedirectResponse(f"/shows/{show_id}#sheet-{sheet_key}", status_code=status.HTTP_303_SEE_OTHER)


@router.post("/shows/{show_id}/guide/{row_id}/update")
async def update_guide_row_route(show_id: int, row_id: int, request: Request, db: Session = Depends(get_db)):
    require_authenticated(request)
    row = _get_guide_row_or_404(db, show_id, row_id)
    payload = dict(await request.form())
    update_guide_row(db, row=row, payload=payload)
    if request.headers.get("x-guide-autosave") == "1":
        return Response(status_code=status.HTTP_204_NO_CONTENT)
    return RedirectResponse(f"/shows/{show_id}#sheet-{row.sheet_key}", status_code=status.HTTP_303_SEE_OTHER)


@router.post("/shows/{show_id}/guide/{row_id}/delete")
def delete_guide_row_route(show_id: int, row_id: int, request: Request, db: Session = Depends(get_db)):
    require_authenticated(request)
    row = _get_guide_row_or_404(db, show_id, row_id)
    sheet_key = row.sheet_key
    delete_guide_row(db, row=row)
    return RedirectResponse(f"/shows/{show_id}#sheet-{sheet_key}", status_code=status.HTTP_303_SEE_OTHER)
