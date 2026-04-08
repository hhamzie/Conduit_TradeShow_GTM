from __future__ import annotations

import logging
import time

from app.config import get_settings
from app.database import SessionLocal, init_db
from app.services import queue_due_shows, run_next_campaign, sync_approved_shows


logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
logger = logging.getLogger(__name__)


def run_worker_loop() -> None:
    init_db()
    poll_seconds = get_settings().worker_poll_seconds
    logger.info("Worker started. Poll interval=%ss", poll_seconds)

    while True:
        with SessionLocal() as db:
            queued = queue_due_shows(db)
            if queued:
                logger.info("Queued %s due show(s).", queued)

            campaign_run = run_next_campaign(db)
            if campaign_run is not None:
                logger.info("Processed campaign run %s with status=%s.", campaign_run.id, campaign_run.status)

            synced = sync_approved_shows(db)
            if synced:
                logger.info("Touched %s approved show(s) for provider sync.", synced)

        time.sleep(poll_seconds)


if __name__ == "__main__":
    run_worker_loop()
