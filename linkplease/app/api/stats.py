from fastapi import APIRouter
from sqlalchemy import func, select

from app.db import SessionLocal
from app.models.blocked_duplicate import BlockedDuplicateEvent
from app.models.dm_job import DMJob

router = APIRouter()


@router.get("/stats")
def get_stats():
    db = SessionLocal()

    try:
        sent = db.scalar(
            select(func.count())
            .select_from(DMJob)
            .where(DMJob.status == "delivered")
        )

        failed = db.scalar(
            select(func.count())
            .select_from(DMJob)
            .where(DMJob.status == "failed")
        )

        queued = db.scalar(
            select(func.count())
            .select_from(DMJob)
            .where(
                DMJob.status.in_(
                    ["queued", "waiting"]
                )
            )
        )

        duplicates_blocked = (
         db.scalar(select(func.count()).select_from(BlockedDuplicateEvent))
         + db.scalar(select(func.count()).select_from(BlockedDuplicateEvent))
)

        return {
            "sent": sent or 0,
            "failed": failed or 0,
            "queued": queued or 0,
            "duplicates_blocked": duplicates_blocked or 0,
        }

    finally:
        db.close()