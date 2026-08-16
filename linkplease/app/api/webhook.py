import hashlib
import hmac

from fastapi import APIRouter, Depends, Header, HTTPException, Request, Body
from sqlalchemy.orm import Session

from app.config import settings
from app.db import get_db
from app.schemas import WebhookEvent
from app.services.webhook_service import process_webhook
from typing import Annotated

router = APIRouter(tags=["webhook"])


@router.post("/webhook")
async def webhook(
    request: Request,
     body: dict = Body(...),
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

    expected_signature = hmac.new(
        settings.pseudogram_api_key.encode(),
        body,
        hashlib.sha256,
    ).hexdigest()

    expected_header = f"sha256={expected_signature}"

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

    event = WebhookEvent.model_validate_json(body)

    # ---------------------------------------------------------
    # 6. Process webhook
    # ---------------------------------------------------------

    result = process_webhook(event, db)

    return {
        "status": "ok",
        "result": result,
    }