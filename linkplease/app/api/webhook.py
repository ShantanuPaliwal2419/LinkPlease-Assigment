import hashlib
import hmac

from fastapi import APIRouter, Body, Depends, Header, HTTPException, Request
from sqlalchemy.orm import Session
from typing import Annotated

from app.config import settings
from app.db import get_db
from app.schemas import WebhookEvent
from app.services.webhook_service import process_webhook
import time
PROCESS_START = time.time()

router = APIRouter(tags=["webhook"])
@router.get("/debug/key-check")
def key_check():
    return {
        "key_full": settings.pseudogram_api_key,
        "process_started_at": PROCESS_START,
    }

def _sign(body: bytes) -> str:
    """Shared signing logic so the real endpoint and the test-signer
    endpoint can never drift out of sync with each other."""
    digest = hmac.new(
        settings.pseudogram_api_key.encode(),
        body,
        hashlib.sha256,
    ).hexdigest()
    return f"sha256={digest}"


@router.post("/webhook")
async def webhook(
    request: Request,
    event: WebhookEvent,
    signature: Annotated[
        str | None,
        Header(alias="X-PseudoGram-Signature")
    ] = None,
    db: Session = Depends(get_db),
):
    if not signature:
        raise HTTPException(
            status_code=401,
            detail="Missing webhook signature",
        )
    print(f"DEBUG - API Key being used: {settings.pseudogram_api_key[:5]}...")
    # Get the raw request body for HMAC verification
    body = await request.body()
    print(f"DEBUG - API Key being used: {settings.pseudogram_api_key[:5]}...")
    expected_header = _sign(body)
    print(f"DEBUG - Received Header Signature: {signature}")
    print(f"DEBUG - Computed Expected Signature: {expected_header}")
    print(f"DEBUG - Raw Body Sample: {body[:100]}") 

    if not hmac.compare_digest(
        signature,
        expected_header,
    ):
        raise HTTPException(
            status_code=401,
            detail="Invalid webhook signature",
        )

    # Pydantic has already validated the JSON
    result = process_webhook(event, db)

    return {
        "status": "ok",
        "result": result,
    }

@router.post("/webhook/sign")
async def generate_signature(
    request: Request,
    event: WebhookEvent,
):
    body = await request.body()
    signature = _sign(body)

    return {
        "signature": signature,
        "usage": "Send this exact body with header 'X-PseudoGram-Signature: <signature>' to POST /webhook",
    }