from __future__ import annotations

from contextlib import asynccontextmanager
from datetime import datetime
import hmac
from pathlib import Path

from fastapi import Depends, FastAPI, File, Form, HTTPException, Request, UploadFile
from fastapi.responses import FileResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from sqlalchemy.orm import Session
from starlette import status
from starlette.middleware.sessions import SessionMiddleware

from app.config import BASE_DIR, get_settings
from app.database import get_db, init_db
from app.services import approve_show, create_or_update_show, get_show, import_shows_from_csv, list_shows, queue_show_now


templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))
settings = get_settings()


@asynccontextmanager
async def lifespan(_app: FastAPI):
    init_db()
    yield


app = FastAPI(title=settings.app_name, lifespan=lifespan)
app.add_middleware(
    SessionMiddleware,
    secret_key=settings.session_secret,
    same_site="lax",
)
app.mount("/static", StaticFiles(directory=str(BASE_DIR / "static")), name="static")


def is_authenticated(request: Request) -> bool:
    return bool(request.session.get("authenticated"))


def require_authenticated(request: Request) -> None:
    if not is_authenticated(request):
        raise HTTPException(
            status_code=status.HTTP_303_SEE_OTHER,
            headers={"Location": "/login"},
        )


def summarize_show_error(error_text: str) -> str:
    if not error_text:
        return ""
    lowered = error_text.lower()
    if "could not resolve host" in lowered or "nodename nor servname provided" in lowered:
        return "An earlier worker run could not reach the site over the network. Retry the show now."
    if "too many requests" in lowered or "http 429" in lowered:
        return "Clay throttled the upload after the scrape finished. Retry later to resend the export."
    if (
        "could not infer company/profile links" in lowered
        and "browser fallback is unavailable" in lowered
    ):
        return "This source failed before browser rendering was enabled. Retry now with browser support turned on."
    if "could not infer company/profile links" in lowered:
        return "We reached the site, but the exhibitor layout still needs a source-specific extractor."
    if "browser fallback is unavailable" in lowered:
        return "This event site may require browser rendering. Retry now that browser support is installed."
    compact = " ".join(error_text.split())
    if len(compact) > 170:
        return compact[:167] + "..."
    return compact


def build_show_notice(show) -> dict[str, str] | None:
    if show.status == "failed":
        return {
            "tone": "danger",
            "title": "Scrape needs attention",
            "detail": summarize_show_error(show.last_error) or "The latest scrape attempt failed.",
        }

    if show.company_count > 0:
        details: list[str] = []
        tone = "success"
        title = "Scrape completed"

        if show.clay_status == "failed":
            tone = "warning"
            title = "Clay upload needs retry"
            details.append("The export finished, but Clay throttled or rejected the row push.")
        elif show.clay_status == "success":
            details.append("Rows were sent to Clay.")

        if show.notification_status == "failed":
            tone = "warning"
            if title == "Scrape completed":
                title = "Email notification needs retry"
            details.append("The export is ready, but the Outlook notification email did not send.")

        if not details:
            details.append(f"{show.company_count} companies were exported and are ready for review.")

        return {
            "tone": tone,
            "title": title,
            "detail": " ".join(details),
        }

    return None


def format_run_at_label(show, now: datetime) -> str:
    run_at = show.run_at
    if run_at is None:
        return "No run time set"

    delta = run_at - now
    total_seconds = int(delta.total_seconds())
    total_hours = abs(total_seconds) // 3600
    days = total_hours // 24
    hours = total_hours % 24

    when_label = run_at.strftime("%b %d, %Y at %-I:%M %p")
    if total_seconds <= 0:
        if days > 0:
            return f"Should have queued {days} day(s) ago · scheduled for {when_label}"
        if hours > 0:
            return f"Should have queued {hours} hour(s) ago · scheduled for {when_label}"
        return f"Should queue now · scheduled for {when_label}"

    if days > 0:
        return f"Queues in {days} day(s) · scheduled for {when_label}"
    if hours > 0:
        return f"Queues in {hours} hour(s) · scheduled for {when_label}"
    return f"Queues in under an hour · scheduled for {when_label}"


def provider_status_summary(show) -> str:
    if show.clay_status == "success":
        return "Clay received the export."
    if show.clay_status == "failed":
        return "Clay push needs a retry."
    if show.company_count > 0:
        return "Clay has not received this export yet."
    return "Clay has nothing to send yet."


def sort_timestamp(value: datetime | None) -> float:
    if value is None:
        return 0.0
    return value.timestamp()


def describe_show_flow(show, now: datetime) -> dict[str, str]:
    if show.status == "waiting":
        if show.run_at and show.run_at <= now:
            return {
                "section": "ready_now",
                "step": "Inside the run window",
                "next_action": "The worker should run this soon. Use Run Immediately only if you want to force it now.",
            }
        return {
            "section": "scheduled_later",
            "step": "Waiting for trigger window",
            "next_action": "No action needed yet. The worker should queue it at the scheduled time.",
        }

    if show.status == "queued":
        return {
            "section": "in_progress",
            "step": "Queued for worker",
            "next_action": "The worker should pick this up next.",
        }

    if show.status == "scraping":
        return {
            "section": "in_progress",
            "step": "Scrape is running",
            "next_action": "Wait for the export and Clay handoff to finish.",
        }

    if show.status == "ready_for_review":
        return {
            "section": "completed",
            "step": "Ready for review",
            "next_action": "Review the export, then approve if it looks good.",
        }

    if show.status == "approved":
        return {
            "section": "completed",
            "step": "Approved",
            "next_action": "Waiting on the downstream outbound sync.",
        }

    if show.status == "live":
        return {
            "section": "completed",
            "step": "Completed",
            "next_action": "This show has already run through the flow.",
        }

    return {
        "section": "in_progress",
        "step": "Needs attention",
        "next_action": "Retry this scrape after reviewing the latest error.",
    }


def sort_key_for_section(item: dict[str, object], section: str) -> tuple[object, ...]:
    show = item["show"]
    if section == "ready_now":
        return (show.run_at or datetime.max, show.event_date, show.id)
    if section == "scheduled_later":
        return (show.run_at or datetime.max, show.event_date, show.id)
    if section == "completed":
        return (show.event_date, show.run_at or datetime.max, show.id)
    return (show.run_at or datetime.max, show.event_date, show.id)


def build_show_card(show, now: datetime) -> dict[str, object]:
    flow = describe_show_flow(show, now)
    return {
        "show": show,
        "error_summary": summarize_show_error(show.last_error),
        "notice": build_show_notice(show),
        "step_label": flow["step"],
        "next_action": flow["next_action"],
        "section": flow["section"],
        "run_timing": format_run_at_label(show, now),
        "provider_summary": provider_status_summary(show),
        "status_label": show.status.replace("_", " "),
    }


def shows_in_section(shows, section: str, now: datetime) -> list:
    matched = []
    for show in shows:
        card = build_show_card(show, now)
        if section == "active" and card["section"] in {"ready_now", "in_progress"}:
            matched.append(show)
            continue
        if card["section"] == section:
            matched.append(show)
    return matched


def lead_total(items: list[dict[str, object]]) -> int:
    return sum(int(item["show"].company_count or 0) for item in items)


def active_sort_key(item: dict[str, object]) -> tuple[object, ...]:
    show = item["show"]
    return (show.run_at or datetime.max, show.event_date, show.id)


@app.get("/healthz")
def healthz() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/login")
def login_page(request: Request):
    if is_authenticated(request):
        return RedirectResponse("/", status_code=status.HTTP_303_SEE_OTHER)
    return templates.TemplateResponse(
        "login.html",
        {
            "request": request,
            "app_name": settings.app_name,
            "error": "",
        },
    )


@app.post("/login")
def login_submit(
    request: Request,
    username: str = Form(...),
    password: str = Form(...),
):
    valid_username = hmac.compare_digest(username, settings.dashboard_username)
    valid_password = hmac.compare_digest(password, settings.dashboard_password)
    if not (valid_username and valid_password):
        return templates.TemplateResponse(
            "login.html",
            {
                "request": request,
                "app_name": settings.app_name,
                "error": "Incorrect username or password.",
            },
            status_code=status.HTTP_400_BAD_REQUEST,
        )

    request.session["authenticated"] = True
    request.session["username"] = username
    return RedirectResponse("/", status_code=status.HTTP_303_SEE_OTHER)


@app.post("/logout")
def logout(request: Request):
    request.session.clear()
    return RedirectResponse("/login", status_code=status.HTTP_303_SEE_OTHER)


@app.get("/")
def dashboard(request: Request, db: Session = Depends(get_db)):
    require_authenticated(request)
    now = datetime.now()
    shows = list_shows(db)
    show_cards = [build_show_card(show, now) for show in shows]
    scheduled_later = sorted(
        [item for item in show_cards if item["section"] == "scheduled_later"],
        key=lambda item: sort_key_for_section(item, "scheduled_later"),
    )
    active = sorted(
        [item for item in show_cards if item["section"] in {"ready_now", "in_progress"}],
        key=active_sort_key,
    )
    completed = sorted(
        [item for item in show_cards if item["section"] == "completed"],
        key=lambda item: sort_key_for_section(item, "completed"),
    )
    return templates.TemplateResponse(
        "index.html",
        {
            "request": request,
            "app_name": settings.app_name,
            "default_offset": settings.default_run_offset_days,
            "active": active,
            "scheduled_later": scheduled_later,
            "completed": completed,
            "show_count": len(shows),
            "ready_count": sum(1 for show in shows if show.status == "ready_for_review"),
            "completed_count": sum(1 for show in shows if show.company_count > 0),
            "active_count": len(active),
            "scheduled_count": len(scheduled_later),
            "completed_section_count": len(completed),
            "active_lead_count": lead_total(active),
            "scheduled_lead_count": lead_total(scheduled_later),
            "completed_lead_count": lead_total(completed),
        },
    )


@app.post("/shows/import")
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
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return RedirectResponse("/", status_code=status.HTTP_303_SEE_OTHER)


@app.post("/shows/add")
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
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    db.commit()
    return RedirectResponse("/", status_code=status.HTTP_303_SEE_OTHER)


@app.post("/shows/delete-all")
def delete_all_shows(
    request: Request,
    section: str = Form(...),
    db: Session = Depends(get_db),
):
    require_authenticated(request)
    now = datetime.now()
    valid_sections = {"active", "scheduled_later", "completed"}
    if section not in valid_sections:
        raise HTTPException(status_code=400, detail="Unknown dashboard section.")

    shows = list_shows(db)
    targets = shows_in_section(shows, section, now)
    for show in targets:
        db.delete(show)
    db.commit()
    return RedirectResponse("/", status_code=status.HTTP_303_SEE_OTHER)


@app.get("/shows/{show_id}")
def show_detail(show_id: int, request: Request, db: Session = Depends(get_db)):
    require_authenticated(request)
    show = get_show(db, show_id)
    if show is None:
        raise HTTPException(status_code=404, detail="Show not found.")
    export_path = Path(show.latest_export_path) if show.latest_export_path else None
    return templates.TemplateResponse(
        "show_detail.html",
        {
            "request": request,
            "app_name": settings.app_name,
            "show": show,
            "export_path": export_path,
            "notice": build_show_notice(show),
            "error_summary": summarize_show_error(show.last_error),
        },
    )


@app.post("/shows/{show_id}/delete")
def delete_show(show_id: int, request: Request, db: Session = Depends(get_db)):
    require_authenticated(request)
    show = get_show(db, show_id)
    if show is None:
        raise HTTPException(status_code=404, detail="Show not found.")
    db.delete(show)
    db.commit()
    return RedirectResponse("/", status_code=status.HTTP_303_SEE_OTHER)


@app.post("/shows/{show_id}/run-now")
def run_show_now(show_id: int, request: Request, db: Session = Depends(get_db)):
    require_authenticated(request)
    show = get_show(db, show_id)
    if show is None:
        raise HTTPException(status_code=404, detail="Show not found.")
    queue_show_now(db, show)
    return RedirectResponse(f"/shows/{show_id}", status_code=status.HTTP_303_SEE_OTHER)


@app.get("/shows/{show_id}/export")
def download_export(show_id: int, request: Request, db: Session = Depends(get_db)):
    require_authenticated(request)
    show = get_show(db, show_id)
    if show is None or not show.latest_export_path:
        raise HTTPException(status_code=404, detail="Export not found.")

    export_path = Path(show.latest_export_path)
    if not export_path.exists():
        raise HTTPException(status_code=404, detail="Export file no longer exists.")

    return FileResponse(export_path, filename=export_path.name, media_type="text/csv")


@app.post("/shows/{show_id}/approve")
def approve_show_route(show_id: int, request: Request, db: Session = Depends(get_db)):
    require_authenticated(request)
    show = get_show(db, show_id)
    if show is None:
        raise HTTPException(status_code=404, detail="Show not found.")
    approve_show(db, show)
    return RedirectResponse(f"/shows/{show_id}", status_code=status.HTTP_303_SEE_OTHER)
