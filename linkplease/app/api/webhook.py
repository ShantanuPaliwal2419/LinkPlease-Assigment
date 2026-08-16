import hashlib
import hmac

from fastapi import APIRouter, Body, Depends, Header, HTTPException, Request
from sqlalchemy.orm import Session
from typing import Annotated

from app.config import settings
from app.db import get_db
from app.schemas import WebhookEvent
from app.services.webhook_service import process_webhook

router = APIRouter(tags=["webhook"])


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

    # Get the raw request body for HMAC verification
    body = await request.body()

    expected_header = _sign(body)

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
async def generate_signature(request: Request):
    """
    Dev/testing helper only — NOT part of the real PseudoGram contract.

    Takes whatever raw JSON body you send it and returns the matching
    X-PseudoGram-Signature value, computed with the exact same secret and
    logic as the real /webhook route. Point Postman/curl at this first to
    get a valid signature for a payload, then send that same payload + the
    returned signature to /webhook.

    In production this endpoint should not exist / should be disabled --
    anyone who can call it can forge a valid signature for any payload,
    which defeats the point of signature verification.
    """
    body = await request.body()
    signature = _sign(body)

    return {
        "signature": signature,
        "usage": "Send this exact body with header 'X-PseudoGram-Signature: <signature>' to POST /webhook",
    }