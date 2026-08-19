import asyncio
import logging
from datetime import datetime, timedelta, timezone

from sqlalchemy import select, or_
from sqlalchemy.ext.asyncio import AsyncSession
import httpx

from app.db import AsyncSessionLocal
from app.models.dm_job import DMJob
from app.services.dm_service import send_dm_async, get_dm_status_async

logger = logging.getLogger(__name__)

MAX_ATTEMPTS = 5
RECONCILE_POLL_SECONDS = 10
FIRST_RECONCILE_DELAY_SECONDS = 3


async def fetch_and_lock_next_job(db: AsyncSession) -> DMJob | None:
    """
    Atomically grabs the next queued job and locks it to 'sending' 
    to prevent duplicate execution across concurrent runs.
    """
    stuck_cutoff = datetime.now(timezone.utc) - timedelta(minutes=2)

    stmt = (
        select(DMJob)
        .where(
            or_(
                DMJob.status == "queued",
                (DMJob.status == "sending") & (DMJob.updated_at <= stuck_cutoff),
            ),
            DMJob.next_attempt_at <= datetime.now(timezone.utc),
        )
        .order_by(DMJob.id)
        .limit(1)
        .with_for_update(skip_locked=True)
    )

    result = await db.execute(stmt)
    job = result.scalar_one_or_none()

    if job:
        job.status = "sending"
        job.attempts += 1
        job.updated_at = datetime.now(timezone.utc)
        await db.commit()  # Lock status in DB immediately

    return job


async def get_waiting_job(db: AsyncSession) -> DMJob | None:
    stmt = (
        select(DMJob)
        .where(
            DMJob.status == "waiting",
            DMJob.dm_id.is_not(None),
            DMJob.next_attempt_at <= datetime.now(timezone.utc),
        )
        .order_by(DMJob.id)
        .limit(1)
        .with_for_update(skip_locked=True)
    )
    result = await db.execute(stmt)
    return result.scalar_one_or_none()


async def _schedule_retry_or_fail(db: AsyncSession, job: DMJob, reason: str):
    if job.attempts >= MAX_ATTEMPTS:
        job.status = "failed"
        job.updated_at = datetime.now(timezone.utc)
        await db.commit()
        logger.warning("Job %s exhausted retries (%s)", job.id, reason)
        return

    retry_seconds = min(2 ** job.attempts, 60)
    job.status = "queued"
    job.next_attempt_at = datetime.now(timezone.utc) + timedelta(seconds=retry_seconds)
    job.updated_at = datetime.now(timezone.utc)
    await db.commit()


async def process_job(db: AsyncSession, client: httpx.AsyncClient, job: DMJob):
    # NON-BLOCKING PACING: Rest 6.5s to respect the 10 req/60s rate limit
    await asyncio.sleep(6.5)

    try:
        response = await send_dm_async(
            client=client,
            recipient_user_id=job.user_id,
            message=job.message,
            comment_id=job.comment_id,
            idempotency_key=f"dm-job-{job.id}",
        )
    except Exception:
        logger.exception("Job %s: send_dm exception", job.id)
        await _schedule_retry_or_fail(db, job, "send_dm exception")
        return

    if response.status_code in (200, 202):
        try:
            data = response.json()
        except ValueError:
            await _schedule_retry_or_fail(db, job, "invalid JSON")
            return

        job.dm_id = data.get("dm_id")
        dm_status = data.get("status")

        if dm_status == "failed":
            job.status = "failed"
            job.updated_at = datetime.now(timezone.utc)
            await db.commit()
            return

        if dm_status == "queued":
            if not job.dm_id:
                await _schedule_retry_or_fail(db, job, "queued missing dm_id")
                return

            job.status = "waiting"
            job.next_attempt_at = datetime.now(timezone.utc) + timedelta(seconds=FIRST_RECONCILE_DELAY_SECONDS)
            job.updated_at = datetime.now(timezone.utc)
            await db.commit()
            return

        if dm_status in ("delivered", "sent"):
            job.status = "delivered"
            job.updated_at = datetime.now(timezone.utc)
            await db.commit()
            return

        job.status = "failed"
        job.updated_at = datetime.now(timezone.utc)
        await db.commit()
        return

    if response.status_code == 400:
        job.status = "failed"
        job.updated_at = datetime.now(timezone.utc)
        await db.commit()
        return

    if response.status_code == 429:
        retry_after = response.headers.get("Retry-After", "10")
        try:
            retry_seconds = int(retry_after)
        except ValueError:
            retry_seconds = 10

        job.status = "queued"
        job.next_attempt_at = datetime.now(timezone.utc) + timedelta(seconds=retry_seconds)
        job.updated_at = datetime.now(timezone.utc)
        await db.commit()

        # Pause without blocking the event loop
        await asyncio.sleep(retry_seconds)
        return

    if response.status_code >= 500:
        await _schedule_retry_or_fail(db, job, f"HTTP {response.status_code}")
        return

    job.status = "failed"
    job.updated_at = datetime.now(timezone.utc)
    await db.commit()


async def reconcile_job(db: AsyncSession, client: httpx.AsyncClient, job: DMJob):
    try:
        response = await get_dm_status_async(client, job.dm_id)
    except Exception:
        job.next_attempt_at = datetime.now(timezone.utc) + timedelta(seconds=RECONCILE_POLL_SECONDS)
        await db.commit()
        return

    if response.status_code in (404, 400):
        job.status = "failed"
        job.updated_at = datetime.now(timezone.utc)
        await db.commit()
        return

    if response.status_code != 200:
        job.next_attempt_at = datetime.now(timezone.utc) + timedelta(seconds=RECONCILE_POLL_SECONDS)
        await db.commit()
        return

    try:
        data = response.json()
    except ValueError:
        job.next_attempt_at = datetime.now(timezone.utc) + timedelta(seconds=RECONCILE_POLL_SECONDS)
        await db.commit()
        return

    dm_status = data.get("status")

    if dm_status == "delivered":
        job.status = "delivered"
        job.updated_at = datetime.now(timezone.utc)
        await db.commit()
        return

    if dm_status == "failed":
        await _schedule_retry_or_fail(db, job, "Failed during reconciliation")
        return

    job.next_attempt_at = datetime.now(timezone.utc) + timedelta(seconds=RECONCILE_POLL_SECONDS)
    await db.commit()


async def _handle_new_job(db: AsyncSession, client: httpx.AsyncClient) -> bool:
    job = await fetch_and_lock_next_job(db)
    if job is None:
        return False

    try:
        await process_job(db, client, job)
    except Exception:
        await db.rollback()
        logger.exception("Unhandled error processing job %s", job.id)
        await _schedule_retry_or_fail(db, job, "unhandled error")

    return True


async def _handle_waiting_job(db: AsyncSession, client: httpx.AsyncClient) -> bool:
    waiting_job = await get_waiting_job(db)
    if waiting_job is None:
        return False

    try:
        await reconcile_job(db, client, waiting_job)
    except Exception:
        await db.rollback()
        logger.exception("Unhandled error reconciling job %s", waiting_job.id)
        waiting_job.next_attempt_at = datetime.now(timezone.utc) + timedelta(seconds=RECONCILE_POLL_SECONDS)
        await db.commit()

    return True


async def run_worker():
    """Continuous async worker loop."""
    logger.info("Async DM background worker started.")
    check_waiting_first = False

    async with httpx.AsyncClient(timeout=10.0) as client:
        while True:
            async with AsyncSessionLocal() as db:
                try:
                    check_waiting_first = not check_waiting_first

                    if check_waiting_first:
                        if await _handle_waiting_job(db, client) or await _handle_new_job(db, client):
                            continue
                    else:
                        if await _handle_new_job(db, client) or await _handle_waiting_job(db, client):
                            continue

                except Exception as e:
                    logger.error("Error in worker loop: %s", e)

            # Sleep non-blockingly when queue is idle
            await asyncio.sleep(1)


if __name__ == "__main__":
    asyncio.run(run_worker())