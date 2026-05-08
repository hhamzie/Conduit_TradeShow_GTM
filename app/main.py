from __future__ import annotations

from app.application import create_app
from app.web.routes.auth import login_page, login_submit, logout
from app.web.routes.health import healthz
from app.web.routes.shows import (
    add_guide_row_route,
    approve_show_route,
    build_trade_show_guide_route,
    configure_smartlead_route,
    delete_show,
    delete_guide_row_route,
    download_enriched_export,
    download_export,
    download_smartlead_export,
    launch_show_route,
    pause_show_route,
    run_show_now,
    show_guide,
    show_dashboard,
    show_detail,
    sync_show_from_clay_route,
    upload_guide_workbook_route,
    update_guide_row_route,
    update_show_route,
)
from app.web.routes.workflow import (
    add_single_show,
    bulk_scrape_download,
    bulk_scrape_status,
    dashboard,
    delete_all_shows,
    import_shows,
    scrape_many_shows,
    scrape_single_show,
)


app = create_app()


__all__ = [
    "app",
    "healthz",
    "login_page",
    "login_submit",
    "logout",
    "dashboard",
    "show_dashboard",
    "show_guide",
    "import_shows",
    "add_single_show",
    "scrape_single_show",
    "scrape_many_shows",
    "bulk_scrape_status",
    "bulk_scrape_download",
    "delete_all_shows",
    "show_detail",
    "update_show_route",
    "delete_show",
    "run_show_now",
    "download_export",
    "download_enriched_export",
    "download_smartlead_export",
    "approve_show_route",
    "sync_show_from_clay_route",
    "launch_show_route",
    "configure_smartlead_route",
    "pause_show_route",
    "build_trade_show_guide_route",
    "upload_guide_workbook_route",
    "add_guide_row_route",
    "update_guide_row_route",
    "delete_guide_row_route",
]
