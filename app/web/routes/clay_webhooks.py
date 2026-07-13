from __future__ import annotations

from datetime import datetime, timezone
import json

from fastapi import APIRouter, Header, HTTPException, Request

from app.config import get_settings
from app.trade_show_ingestion import export_completed_clay_webhook_payload_to_csv


router = APIRouter(prefix="/webhooks/clay", tags=["clay-webhooks"])


@router.post("/tradeshow-completed")
async def clay_tradeshow_completed(
    request: Request,
    x_conduit_secret: str = Header(default=""),
) -> dict[str, object]:
    settings = get_settings()
    if not settings.trade_show_completed_webhook_secret:
        raise HTTPException(status_code=503, detail="TRADE_SHOW_COMPLETED_WEBHOOK_SECRET is not configured.")

    try:
        payload = await request.json()
    except ValueError as exc:
        raise HTTPException(status_code=400, detail="Request body must be valid JSON.") from exc

    payload_secret = payload.get("secret", "") if isinstance(payload, dict) else ""
    if x_conduit_secret != settings.trade_show_completed_webhook_secret and payload_secret != settings.trade_show_completed_webhook_secret:
        raise HTTPException(status_code=401, detail="Invalid webhook secret.")
    if isinstance(payload, dict) and "secret" in payload:
        payload = {key: value for key, value in payload.items() if key != "secret"}

    try:
        log_dir = settings.trade_show_ingestion_dir / "logs"
        log_dir.mkdir(parents=True, exist_ok=True)
        (log_dir / "last_clay_completed_webhook_payload.json").write_text(
            json.dumps(
                {"received_at": datetime.now(timezone.utc).isoformat(), "payload": payload},
                indent=2,
                sort_keys=True,
            ),
            encoding="utf-8",
        )
    except OSError:
        pass

    try:
        summary = export_completed_clay_webhook_payload_to_csv(payload)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    return {"ok": True, "summary": summary}
