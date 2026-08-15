from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from app.db import get_db
from app.schemas import WebhookEvent
from app.services.webhook_service import process_webhook


router = APIRouter(tags=["webhook"])


@router.post("/webhook")
def webhook(
    event: WebhookEvent,
    db: Session = Depends(get_db),
):
    result = process_webhook(event, db)

    return {
        "status": "ok",
        "result": result,
    }