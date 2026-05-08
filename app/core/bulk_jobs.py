from __future__ import annotations

from threading import Lock, Thread
from uuid import uuid4

from app.config import get_settings
from app.database import SessionLocal
from app.services import QueuedBulkShow, run_bulk_direct_scrape


class BulkScrapeJobStore:
    def __init__(self) -> None:
        self._jobs: dict[str, dict[str, object]] = {}
        self._lock = Lock()

    def create_job(self) -> str:
        job_id = uuid4().hex
        self.update_job(job_id, status="queued", message="Queued bulk scrape job.")
        return job_id

    def update_job(self, job_id: str, **updates: object) -> dict[str, object]:
        with self._lock:
            job = self._jobs.setdefault(
                job_id,
                {
                    "job_id": job_id,
                    "status": "queued",
                    "completed": 0,
                    "total": 0,
                    "current_show": "",
                    "message": "Queued.",
                    "download_url": "",
                    "error": "",
                },
            )
            job.update(updates)
            return dict(job)

    def get_job(self, job_id: str) -> dict[str, object] | None:
        with self._lock:
            job = self._jobs.get(job_id)
            return dict(job) if job is not None else None

    def start_job(
        self,
        payload: bytes,
        *,
        run_offset_days: int | None = None,
        queued_shows: list[QueuedBulkShow] | None = None,
    ) -> str:
        job_id = self.create_job()
        Thread(target=self._run_job, args=(job_id, payload, run_offset_days, queued_shows), daemon=True).start()
        return job_id

    def _run_job(
        self,
        job_id: str,
        payload: bytes,
        run_offset_days: int | None,
        queued_shows: list[QueuedBulkShow] | None,
    ) -> None:
        def progress_callback(completed: int, total: int, show_name: str, message: str) -> None:
            self.update_job(
                job_id,
                status="running",
                completed=completed,
                total=total,
                current_show=show_name,
                message=message,
            )

        try:
            self.update_job(job_id, status="running", message="Preparing bulk scrape...")
            with SessionLocal() as db:
                result = run_bulk_direct_scrape(
                    payload,
                    progress_callback=progress_callback,
                    db=db,
                    run_offset_days=run_offset_days or get_settings().default_run_offset_days,
                    queued_shows=queued_shows,
                )
            self.update_job(
                job_id,
                status="completed",
                completed=result.show_count,
                total=result.show_count,
                message=(
                    f"Finished {result.success_count} show(s), skipped {result.skipped_count}, "
                    f"with {result.failed_count} failure(s)."
                ),
                download_url=f"/scrape/bulk/download/{job_id}",
                archive_path=str(result.archive_path),
            )
        except Exception as exc:  # noqa: BLE001
            self.update_job(
                job_id,
                status="failed",
                error=str(exc),
                message=str(exc),
            )


bulk_scrape_jobs = BulkScrapeJobStore()
