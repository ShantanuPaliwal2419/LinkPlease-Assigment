import hashlib
import hmac

from fastapi import APIRouter, Depends, Header, HTTPException, Request
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
    signature: Annotated[
        str | None,
        Header(alias="X-PseudoGram-Signature")
    ] = None,
    db: Session = Depends(get_db),
):
    # ---------------------------------------------------------
    # 1. Read RAW request body
    # ---------------------------------------------------------

    body = await request.body()

    # ---------------------------------------------------------
    # 2. Signature must exist
    # ---------------------------------------------------------

    if not signature:
        raise HTTPException(
            status_code=401,
            detail="Missing webhook signature",
        )

    # ---------------------------------------------------------
    # 3. Calculate HMAC-SHA256
    # ---------------------------------------------------------

    expected_header = _sign(body)

    # ---------------------------------------------------------
    # 4. Constant-time comparison
    # ---------------------------------------------------------

    if not hmac.compare_digest(
        signature,
        expected_header,
    ):
        raise HTTPException(
            status_code=401,
            detail="Invalid webhook signature",
        )

    # ---------------------------------------------------------
    # 5. Only now parse JSON
    # ---------------------------------------------------------

    try:
        event = WebhookEvent.model_validate_json(body)
    except ValueError as exc:
        raise HTTPException(
            status_code=422,
            detail=f"Invalid webhook payload: {exc}",
        )

    # ---------------------------------------------------------
    # 6. Process webhook
    # ---------------------------------------------------------

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