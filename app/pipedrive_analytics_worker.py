from __future__ import annotations

import logging

from app.database import SessionLocal, init_db
from app.pipedrive_analytics import refresh_pipedrive_analytics


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
logger = logging.getLogger(__name__)


def main() -> None:
    """Create today's OpenPhone call analytics snapshot, or reuse it if it exists."""

    init_db()
    with SessionLocal() as db:
        payload = refresh_pipedrive_analytics(db)
    logger.info(
        "OpenPhone call analytics snapshot ready. report_date=%s source_count=%s",
        payload["report"]["date"],
        payload["report"]["source_count"],
    )


if __name__ == "__main__":
    main()
