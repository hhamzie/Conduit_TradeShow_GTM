from __future__ import annotations

import logging
import time

from app.config import get_settings
from app.database import SessionLocal, init_db
from app.services import backfill_queued_runs, queue_due_shows, run_next_campaign, run_weekly_show_sync, sync_approved_shows


logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
logger = logging.getLogger(__name__)


def run_worker_loop() -> None:
    init_db()
    poll_seconds = get_settings().worker_poll_seconds
    logger.info("Worker started. Poll interval=%ss", poll_seconds)

    while True:
        try:
            with SessionLocal() as db:
                weekly_sync = run_weekly_show_sync(db)
                if weekly_sync is not None:
                    logger.info(
                        "Weekly trade show sync finished. created=%s updated=%s skipped=%s filtered_out=%s",
                        weekly_sync.created,
                        weekly_sync.updated,
                        weekly_sync.skipped,
                        weekly_sync.filtered_out,
                    )

                queued = queue_due_shows(db)
                if queued:
                    logger.info("Queued %s due show(s).", queued)

                repaired = backfill_queued_runs(db)
                if repaired:
                    logger.info("Backfilled %s queued show(s) that were missing a scrape run.", repaired)

                campaign_run = run_next_campaign(db)
                if campaign_run is not None:
                    logger.info("Processed campaign run %s with status=%s.", campaign_run.id, campaign_run.status)

                synced = sync_approved_shows(db)
                if synced:
                    logger.info("Touched %s approved show(s) for provider sync.", synced)
        except Exception:  # noqa: BLE001
            logger.exception("Worker loop iteration failed. Continuing after backoff.")

        time.sleep(poll_seconds)


if __name__ == "__main__":
    run_worker_loop()
