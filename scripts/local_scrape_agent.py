from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from tempfile import gettempdir
from threading import Lock, Thread
from uuid import uuid4

import uvicorn
from fastapi import FastAPI, HTTPException, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from pydantic import BaseModel

from app.services import _run_direct_scrape


@dataclass
class LocalScrapeJob:
    id: str
    show_name: str
    place: str
    link: str
    event_date_raw: str
    status: str = "queued"
    output_path: str = ""
    company_count: int = 0
    failure_count: int = 0
    error: str = ""
    created_at: datetime = field(default_factory=datetime.now)
    started_at: datetime | None = None
    finished_at: datetime | None = None


class LocalScrapeRequest(BaseModel):
    show_name: str
    place: str
    link: str
    event_date_raw: str = ""


app = FastAPI(title="Local Scrape Agent")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

_jobs: dict[str, LocalScrapeJob] = {}
_jobs_lock = Lock()


@app.middleware("http")
async def allow_private_network_requests(request: Request, call_next):
    if request.method == "OPTIONS":
        response = Response(status_code=204)
    else:
        response = await call_next(request)
    response.headers["Access-Control-Allow-Origin"] = "*"
    response.headers["Access-Control-Allow-Methods"] = "*"
    response.headers["Access-Control-Allow-Headers"] = "*"
    response.headers["Access-Control-Allow-Private-Network"] = "true"
    return response


def _job_output_path(job: LocalScrapeJob) -> Path:
    slug = "".join(ch.lower() if ch.isalnum() else "-" for ch in job.show_name).strip("-") or "show"
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    return Path(gettempdir()) / "conduit-local-scrapes" / f"{slug}_{timestamp}_{job.id}.csv"


def _run_job(job_id: str) -> None:
    with _jobs_lock:
        job = _jobs[job_id]
        job.status = "running"
        job.started_at = datetime.now()

    try:
        output_path = _job_output_path(job)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        result = _run_direct_scrape(
            show_name=job.show_name,
            place=job.place,
            link=job.link,
            output_path=output_path,
            require_website=True,
            browser_mode="prefer",
            workers=1,
        )
    except Exception as exc:  # noqa: BLE001
        with _jobs_lock:
            job.status = "failed"
            job.error = str(exc)
            job.finished_at = datetime.now()
        return

    with _jobs_lock:
        job.status = "completed"
        job.output_path = str(result.output_path)
        job.company_count = result.company_count
        job.failure_count = result.failure_count
        job.finished_at = datetime.now()


@app.get("/healthz")
def healthz() -> dict[str, str]:
    return {"status": "ok"}


@app.post("/jobs")
def start_job(payload: LocalScrapeRequest) -> dict[str, str]:
    job = LocalScrapeJob(
        id=uuid4().hex,
        show_name=payload.show_name.strip(),
        place=payload.place.strip(),
        link=payload.link.strip(),
        event_date_raw=payload.event_date_raw.strip(),
    )
    if not (job.show_name and job.place and job.link):
        raise HTTPException(status_code=400, detail="Show name, place, and link are required.")

    with _jobs_lock:
        _jobs[job.id] = job

    Thread(target=_run_job, args=(job.id,), daemon=True).start()
    return {"job_id": job.id, "status": job.status}


@app.get("/jobs/{job_id}")
def read_job(job_id: str) -> dict[str, object]:
    with _jobs_lock:
        job = _jobs.get(job_id)
        if job is None:
            raise HTTPException(status_code=404, detail="Job not found.")
        return {
            "job_id": job.id,
            "status": job.status,
            "show_name": job.show_name,
            "company_count": job.company_count,
            "failure_count": job.failure_count,
            "error": job.error,
        }


@app.get("/jobs/{job_id}/file")
def download_job_file(job_id: str) -> FileResponse:
    with _jobs_lock:
        job = _jobs.get(job_id)
        if job is None:
            raise HTTPException(status_code=404, detail="Job not found.")
        if job.status != "completed" or not job.output_path:
            raise HTTPException(status_code=409, detail="Job output is not ready yet.")
        output_path = Path(job.output_path)

    if not output_path.exists():
        raise HTTPException(status_code=404, detail="Job output file is missing.")
    return FileResponse(output_path, filename=output_path.name, media_type="text/csv")


if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=8765)
