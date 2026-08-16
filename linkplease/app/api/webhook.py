import hashlib
import hmac
from typing import Annotated, Any

from fastapi import APIRouter, Body, Depends, Header, HTTPException, Request
from sqlalchemy.orm import Session

from app.config import settings
from app.db import get_db
from app.schemas import WebhookEvent
from app.services.webhook_service import process_webhook

router = APIRouter(tags=["webhook"])


def _sign(body: bytes) -> str:
    digest = hmac.new(
        settings.pseudogram_api_key.encode(),
        body,
        hashlib.sha256,
    ).hexdigest()
    return f"sha256={digest}"


@router.post("/webhook")
async def webhook(
    request: Request,
    signature: Annotated[
        str | None,
        Header(alias="X-PseudoGram-Signature"),
    ] = None,
    db: Session = Depends(get_db),
):
    body = await request.body()

    if not signature:
        raise HTTPException(status_code=401, detail="Missing webhook signature")

    expected_header = _sign(body)

    if not hmac.compare_digest(signature, expected_header):
        raise HTTPException(status_code=401, detail="Invalid webhook signature")

    try:
        event = WebhookEvent.model_validate_json(body)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=f"Invalid webhook payload: {exc}")

    result = process_webhook(event, db)

    return {"status": "ok", "result": result}


@router.post("/webhook/sign")
async def generate_signature(request: Request):
    body = await request.body()
    signature = _sign(body)
    return {"signature": signature}