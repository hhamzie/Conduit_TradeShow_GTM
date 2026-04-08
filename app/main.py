from __future__ import annotations

from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import Depends, FastAPI, File, Form, HTTPException, Request, UploadFile
from fastapi.responses import FileResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from sqlalchemy.orm import Session
from starlette import status

from app.config import BASE_DIR, get_settings
from app.database import get_db, init_db
from app.services import approve_show, get_show, import_shows_from_csv, list_shows, queue_show_now


templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))


@asynccontextmanager
async def lifespan(_app: FastAPI):
    init_db()
    yield


app = FastAPI(title=get_settings().app_name, lifespan=lifespan)
app.mount("/static", StaticFiles(directory=str(BASE_DIR / "static")), name="static")


@app.get("/healthz")
def healthz() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/")
def dashboard(request: Request, db: Session = Depends(get_db)):
    return templates.TemplateResponse(
        "index.html",
        {
            "request": request,
            "app_name": get_settings().app_name,
            "default_offset": get_settings().default_run_offset_days,
            "shows": list_shows(db),
        },
    )


@app.post("/shows/import")
async def import_shows(
    file: UploadFile = File(...),
    run_offset_days: int = Form(...),
    db: Session = Depends(get_db),
):
    payload = await file.read()
    if not payload:
        raise HTTPException(status_code=400, detail="The uploaded CSV was empty.")
    try:
        import_shows_from_csv(db, payload, run_offset_days)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return RedirectResponse("/", status_code=status.HTTP_303_SEE_OTHER)


@app.get("/shows/{show_id}")
def show_detail(show_id: int, request: Request, db: Session = Depends(get_db)):
    show = get_show(db, show_id)
    if show is None:
        raise HTTPException(status_code=404, detail="Show not found.")
    export_path = Path(show.latest_export_path) if show.latest_export_path else None
    return templates.TemplateResponse(
        "show_detail.html",
        {
            "request": request,
            "app_name": get_settings().app_name,
            "show": show,
            "export_path": export_path,
        },
    )


@app.post("/shows/{show_id}/run-now")
def run_show_now(show_id: int, db: Session = Depends(get_db)):
    show = get_show(db, show_id)
    if show is None:
        raise HTTPException(status_code=404, detail="Show not found.")
    queue_show_now(db, show)
    return RedirectResponse(f"/shows/{show_id}", status_code=status.HTTP_303_SEE_OTHER)


@app.get("/shows/{show_id}/export")
def download_export(show_id: int, db: Session = Depends(get_db)):
    show = get_show(db, show_id)
    if show is None or not show.latest_export_path:
        raise HTTPException(status_code=404, detail="Export not found.")

    export_path = Path(show.latest_export_path)
    if not export_path.exists():
        raise HTTPException(status_code=404, detail="Export file no longer exists.")

    return FileResponse(export_path, filename=export_path.name, media_type="text/csv")


@app.post("/shows/{show_id}/approve")
def approve_show_route(show_id: int, db: Session = Depends(get_db)):
    show = get_show(db, show_id)
    if show is None:
        raise HTTPException(status_code=404, detail="Show not found.")
    approve_show(db, show)
    return RedirectResponse(f"/shows/{show_id}", status_code=status.HTTP_303_SEE_OTHER)
