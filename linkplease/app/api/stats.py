from fastapi import APIRouter, Depends
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.db import get_db
from app.models.blocked_duplicate import BlockedDuplicateEvent
from app.models.dm_job import DMJob

router = APIRouter()


@router.get("/stats")
async def get_stats(db: AsyncSession = Depends(get_db)):
    sent = await db.scalar(
        select(func.count())
        .select_from(DMJob)
        .where(DMJob.status == "delivered")
    )

    failed = await db.scalar(
        select(func.count())
        .select_from(DMJob)
        .where(DMJob.status == "failed")
    )

    queued = await db.scalar(
        select(func.count())
        .select_from(DMJob)
        .where(
            DMJob.status.in_(
                ["queued", "waiting"]
            )
        )
    )

    duplicates_blocked = await db.scalar(
        select(func.count()).select_from(BlockedDuplicateEvent)
    )

    return {
        "sent": sent or 0,
        "failed": failed or 0,
        "queued": queued or 0,
        "duplicates_blocked": duplicates_blocked or 0,
    }