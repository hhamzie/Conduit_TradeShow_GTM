from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta

from app.models import RunStatus, Show


WORKFLOW_SECTIONS = {"active", "scheduled_later", "completed"}
RECENT_COMPLETION_WINDOW = timedelta(minutes=20)
SHOW_STATUS_LABELS = {
    "waiting": "Queued",
    "queued": "Queued",
    "scraping": "Scraping",
    "ready_for_review": "Populated",
    "approved": "Ready to launch",
    "live": "Live",
    "failed": "Failed scrape",
}
RUN_STATUS_LABELS = {
    "queued": "Queued",
    "running": "Running",
    "success": "Completed",
    "failed": "Failed",
}


@dataclass(frozen=True)
class ShowNotice:
    tone: str
    title: str
    detail: str


@dataclass(frozen=True)
class ShowCard:
    show: Show
    error_summary: str
    notice: ShowNotice | None
    step_label: str
    next_action: str
    section: str
    run_timing: str
    provider_summary: str
    status_label: str
    queue_position: int | None
    queue_total: int


@dataclass(frozen=True)
class WorkflowDashboardView:
    active: list[ShowCard]
    scheduled_later: list[ShowCard]
    completed: list[ShowCard]
    show_count: int
    ready_count: int
    completed_count: int
    active_count: int
    scheduled_count: int
    completed_section_count: int
    active_lead_count: int
    scheduled_lead_count: int
    completed_lead_count: int


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
        return "Primary scraper stalled before it could map the exhibitor directory. OpenAI recovery still needs a usable pass."
    if "could not infer company/profile links" in lowered:
        return "Primary scraper stalled before it could map the exhibitor directory. OpenAI recovery did not return a usable exhibitor list yet."
    if "browser fallback is unavailable" in lowered:
        return "This event site may require browser rendering. Retry now that browser support is installed."
    compact = " ".join(error_text.split())
    if len(compact) > 170:
        return compact[:167] + "..."
    return compact


def get_show_status_label(status: str, queue_position: int | None = None) -> str:
    if status == "queued" and queue_position:
        return f"#{queue_position} in line"
    return SHOW_STATUS_LABELS.get(status, status.replace("_", " ").title())


def get_run_status_label(status: str) -> str:
    return RUN_STATUS_LABELS.get(status, status.replace("_", " ").title())


def _has_recent_successful_scrape(show: Show, now: datetime) -> bool:
    for run in show.runs:
        if run.status != RunStatus.success.value:
            continue
        if run.finished_at is None:
            continue
        return now - run.finished_at <= RECENT_COMPLETION_WINDOW
    return False


def build_show_notice(show: Show, now: datetime) -> ShowNotice | None:
    if show.status == "failed":
        return ShowNotice(
            tone="danger",
            title="Scrape needs attention",
            detail=summarize_show_error(show.last_error) or "The latest scrape attempt failed.",
        )

    if not _has_recent_successful_scrape(show, now):
        return None

    if show.company_count > 0:
        details: list[str] = []
        tone = "success"
        title = "Scrape completed"

        if show.clay_status == "failed":
            tone = "warning"
            title = "Clay upload needs retry"
            details.append("The export finished, but Clay throttled or rejected the row push.")
        elif show.clay_status == "polling":
            tone = "warning" if show.status == "approved" else "success"
            title = "Clay enrichment in progress"
            details.append(
                f"Clay has {show.clay_ready_rows}/{show.clay_total_rows or show.company_count} rows ready so far."
            )
        elif show.clay_status == "complete":
            details.append(
                f"Clay resolved all {show.clay_total_rows} rows. Ready: {show.clay_ready_rows}, failed: {show.clay_failed_rows}, skipped: {show.clay_skipped_rows}."
            )
        elif show.clay_status == "success":
            details.append("Rows were sent to Clay.")

        if show.notification_status == "failed":
            tone = "warning"
            if title == "Scrape completed":
                title = "Email notification needs retry"
            details.append("The export is ready, but the Outlook notification email did not send.")

        if show.smartlead_status == "syncing":
            details.append(
                f"Smartlead has processed {show.smartlead_imported_rows} ready row(s) into {show.smartlead_campaign_name or 'the show campaign'}."
            )
        elif show.smartlead_status == "ready_to_launch":
            tone = "success"
            title = "Smartlead campaign ready"
            details.append("All Clay rows are resolved and the Smartlead campaign is ready to launch.")
        elif show.smartlead_status == "active":
            title = "Smartlead campaign live"
            details.append("This show's Smartlead campaign is currently active.")
        elif show.smartlead_status == "paused":
            tone = "warning"
            title = "Smartlead campaign paused"
            details.append("The Smartlead campaign is prepared but currently paused.")
        elif show.smartlead_status == "failed":
            tone = "warning"
            title = "Smartlead sync needs retry"
            details.append("The Clay pull succeeded, but the latest Smartlead sync failed.")

        if not details:
            details.append(f"{show.company_count} companies were exported and the show is populated.")

        return ShowNotice(
            tone=tone,
            title=title,
            detail=" ".join(details),
        )

    return None


def format_run_at_label(show: Show, now: datetime) -> str:
    run_at = show.run_at
    if run_at is None:
        return "No queue time set"

    queued_label = run_at.strftime("%b %d, %Y at %-I:%M %p")
    if show.status == "waiting":
        return f"Queued on {queued_label}"
    if show.status == "failed":
        return f"Last queued on {queued_label}"
    return f"Added on {queued_label}"


def _active_scrape_started_at(show: Show) -> datetime | None:
    running_runs = [run for run in show.runs if run.status == RunStatus.running.value]
    if not running_runs:
        return None
    return min(
        (
            run.started_at
            or run.created_at
            or show.run_at
        )
        for run in running_runs
        if (run.started_at or run.created_at or show.run_at) is not None
    )


def format_scrape_elapsed_label(show: Show, now: datetime) -> str:
    started_at = _active_scrape_started_at(show)
    if started_at is None:
        return "Scrape is running now"

    elapsed_seconds = max(0, int((now - started_at).total_seconds()))
    hours, remainder = divmod(elapsed_seconds, 3600)
    minutes = remainder // 60

    if hours > 0:
        return f"Scraping for {hours}h {minutes}m"
    if minutes > 0:
        return f"Scraping for {minutes}m"
    return "Scraping for under 1m"


def build_scrape_queue_positions(shows: list[Show]) -> tuple[dict[int, int], int]:
    queue_items: list[tuple[datetime, int]] = []
    for show in shows:
        if show.status != "queued":
            continue
        queued_runs = [run for run in show.runs if run.status == RunStatus.queued.value]
        if queued_runs:
            sort_at = min(
                (
                    run.created_at
                    or show.run_at
                    or datetime.max
                )
                for run in queued_runs
            )
        else:
            sort_at = show.run_at or datetime.max
        queue_items.append((sort_at, show.id))

    queue_items.sort(key=lambda item: (item[0], item[1]))
    positions = {show_id: index for index, (_sort_at, show_id) in enumerate(queue_items, start=1)}
    return positions, len(queue_items)


def provider_status_summary(show: Show) -> str:
    if show.clay_status == "complete":
        return f"Clay resolved {show.clay_total_rows} rows and Smartlead processed {show.smartlead_imported_rows}."
    if show.clay_status == "polling":
        total_rows = show.clay_total_rows or show.company_count
        return f"Clay is enriching rows ({show.clay_ready_rows}/{total_rows} ready)."
    if show.smartlead_status == "ready_to_launch":
        return "Smartlead campaign is prepared and ready to launch."
    if show.smartlead_status == "active":
        return "Smartlead campaign is active."
    if show.clay_status == "failed":
        return "Clay push needs a retry."
    if show.company_count > 0:
        return "Clay has not received this export yet."
    return "Clay has nothing to send yet."


def describe_show_flow(show: Show, now: datetime, *, queue_position: int | None = None, queue_total: int = 0) -> dict[str, str]:
    if show.status == "waiting":
        return {
            "section": "in_progress",
            "step": "Queued for scrape",
            "next_action": "The worker should pick this up soon and move it into an active scrape.",
        }

    if show.status == "queued":
        position_label = f"Queue position {queue_position} of {queue_total}" if queue_position else "Queued for scraping"
        return {
            "section": "in_progress",
            "step": position_label,
            "next_action": "The worker scrapes one show at a time and will move to this when earlier queued shows finish.",
        }

    if show.status == "scraping":
        return {
            "section": "in_progress",
            "step": "Scraping now",
            "next_action": "Wait for the export and Clay handoff to finish.",
        }

    if show.status == "ready_for_review":
        if show.clay_status in {"polling", "complete"}:
            return {
                "section": "completed",
                "step": "Reviewing Clay enrichment",
                "next_action": "Clay is syncing back automatically. Approve the show when you want it launch-ready later.",
            }
        return {
            "section": "completed",
            "step": "Ready for review",
            "next_action": "Review the export, then approve if it looks good.",
        }

    if show.status == "approved":
        if show.smartlead_status == "ready_to_launch":
            return {
                "section": "completed",
                "step": "Ready to launch",
                "next_action": "Launch the Smartlead campaign when you want this show to go live.",
            }
        return {
            "section": "completed",
            "step": "Approved",
            "next_action": "Clay and Smartlead are still preparing the campaign.",
        }

    if show.status == "live":
        return {
            "section": "completed",
            "step": "Campaign live",
            "next_action": "This show's Smartlead campaign is active.",
        }

    return {
        "section": "in_progress",
        "step": "Needs attention",
        "next_action": "Retry this scrape after reviewing the latest error.",
    }


def build_show_card(show: Show, now: datetime, *, queue_position: int | None = None, queue_total: int = 0) -> ShowCard:
    flow = describe_show_flow(show, now, queue_position=queue_position, queue_total=queue_total)
    if show.status == "scraping":
        run_timing = format_scrape_elapsed_label(show, now)
    elif show.status in {"queued", "scraping"} and queue_position and queue_total:
        run_timing = f"{queue_position} of {queue_total} in scrape queue"
    else:
        run_timing = format_run_at_label(show, now)

    return ShowCard(
        show=show,
        error_summary=summarize_show_error(show.last_error),
        notice=build_show_notice(show, now),
        step_label=flow["step"],
        next_action=flow["next_action"],
        section=flow["section"],
        run_timing=run_timing,
        provider_summary=provider_status_summary(show),
        status_label=get_show_status_label(show.status, queue_position),
        queue_position=queue_position,
        queue_total=queue_total,
    )


def shows_in_section(shows: list[Show], section: str, now: datetime) -> list[Show]:
    queue_positions, queue_total = build_scrape_queue_positions(shows)
    matched: list[Show] = []
    for show in shows:
        card = build_show_card(show, now, queue_position=queue_positions.get(show.id), queue_total=queue_total)
        if section == "active" and card.section in {"ready_now", "in_progress"}:
            matched.append(show)
            continue
        if card.section == section:
            matched.append(show)
    return matched


def build_workflow_dashboard_view(shows: list[Show], now: datetime) -> WorkflowDashboardView:
    queue_positions, queue_total = build_scrape_queue_positions(shows)
    show_cards = [
        build_show_card(show, now, queue_position=queue_positions.get(show.id), queue_total=queue_total)
        for show in shows
    ]
    scheduled_later: list[ShowCard] = []
    active = sorted(
        [item for item in show_cards if item.section in {"ready_now", "in_progress"}],
        key=lambda item: (item.show.run_at or datetime.max, item.show.event_date, item.show.id),
    )
    completed = sorted(
        [item for item in show_cards if item.section == "completed"],
        key=lambda item: (item.show.event_date, item.show.run_at or datetime.max, item.show.id),
    )
    return WorkflowDashboardView(
        active=active,
        scheduled_later=scheduled_later,
        completed=completed,
        show_count=len(shows),
        ready_count=sum(1 for show in shows if show.status == "ready_for_review"),
        completed_count=sum(1 for show in shows if show.company_count > 0),
        active_count=len(active),
        scheduled_count=len(scheduled_later),
        completed_section_count=len(completed),
        active_lead_count=_lead_total(active),
        scheduled_lead_count=_lead_total(scheduled_later),
        completed_lead_count=_lead_total(completed),
    )


def _lead_total(items: list[ShowCard]) -> int:
    return sum(int(item.show.company_count or 0) for item in items)
