import asyncio
import logging
from datetime import datetime, timedelta, timezone

import httpx
from sqlalchemy import or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.db import AsyncSessionLocal
from app.models.dm_job import DMJob
from app.services.dm_service import (
    get_dm_status_async,
    send_dm_async,
)


# ============================================================
# LOGGING
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

logger = logging.getLogger(__name__)

print(
    "🔥🔥🔥 CONCURRENT DM WORKER PROCESS STARTED 🔥🔥🔥",
    flush=True,
)


# ============================================================
# CONFIG
# ============================================================

# Maximum number of DMs sent concurrently.
MAX_CONCURRENT_SENDS = 5

# Number of jobs fetched from DB at once.
BATCH_SIZE = 5

# Small pacing delay before each batch starts.
# This is NOT 6.5 seconds per job.
SEND_PACING_SECONDS = 1.0

# Maximum attempts for transient failures.
MAX_ATTEMPTS = 5

# Reconciliation polling.
RECONCILE_POLL_SECONDS = 5

# First reconciliation check after API says queued.
FIRST_RECONCILE_DELAY_SECONDS = 2

# A job stuck in "sending" for this long can be recovered.
STUCK_JOB_MINUTES = 2

# Worker idle backoff.
IDLE_BACKOFF_INITIAL = 1
IDLE_BACKOFF_MAX = 5

# HTTP timeout.
HTTP_TIMEOUT = 15.0


# ============================================================
# HELPERS
# ============================================================

def utcnow() -> datetime:
    return datetime.now(timezone.utc)


# ============================================================
# FETCH + LOCK NEW JOBS
# ============================================================

async def fetch_and_lock_jobs(
    db: AsyncSession,
    limit: int = BATCH_SIZE,
) -> list[DMJob]:

    now = utcnow()

    stuck_cutoff = (
        now
        - timedelta(minutes=STUCK_JOB_MINUTES)
    )

    print(
        f"🔍 FETCHING JOB BATCH | limit={limit}",
        flush=True,
    )

    stmt = (
        select(DMJob)
        .where(
            or_(
                DMJob.status == "queued",

                (
                    (DMJob.status == "sending")
                    & (
                        DMJob.updated_at
                        <= stuck_cutoff
                    )
                ),
            ),
            DMJob.next_attempt_at <= now,
        )
        .order_by(DMJob.id)
        .limit(limit)
        .with_for_update(
            skip_locked=True
        )
        .execution_options(
            populate_existing=True
        )
    )

    result = await db.execute(stmt)

    jobs = list(
        result.scalars().all()
    )

    if not jobs:
        return []

    for job in jobs:

        old_status = job.status

        job.status = "sending"
        job.attempts += 1
        job.updated_at = now

        print(
            f"🔒 JOB LOCKED | "
            f"id={job.id} | "
            f"old_status={old_status} | "
            f"attempt={job.attempts} | "
            f"user={job.user_id}",
            flush=True,
        )

    await db.commit()

    print(
        f"📦 BATCH LOCKED | "
        f"count={len(jobs)}",
        flush=True,
    )

    return jobs


# ============================================================
# FETCH WAITING RECONCILIATION JOBS
# ============================================================

async def fetch_waiting_jobs(
    db: AsyncSession,
    limit: int = BATCH_SIZE,
) -> list[DMJob]:

    now = utcnow()

    stmt = (
        select(DMJob)
        .where(
            DMJob.status == "waiting",
            DMJob.dm_id.is_not(None),
            DMJob.next_attempt_at <= now,
        )
        .order_by(DMJob.id)
        .limit(limit)
        .with_for_update(
            skip_locked=True
        )
        .execution_options(
            populate_existing=True
        )
    )

    result = await db.execute(stmt)

    jobs = list(
        result.scalars().all()
    )

    if jobs:

        print(
            f"🔄 WAITING BATCH FOUND | "
            f"count={len(jobs)}",
            flush=True,
        )

    return jobs


# ============================================================
# RETRY / FAIL
# ============================================================

async def schedule_retry_or_fail(
    db: AsyncSession,
    job: DMJob,
    reason: str,
):

    now = utcnow()

    if job.attempts >= MAX_ATTEMPTS:

        job.status = "failed"
        job.updated_at = now

        await db.commit()

        print(
            f"❌ JOB FAILED PERMANENTLY | "
            f"id={job.id} | "
            f"attempts={job.attempts} | "
            f"reason={reason}",
            flush=True,
        )

        return

    retry_seconds = min(
        2 ** job.attempts,
        60,
    )

    job.status = "queued"

    job.next_attempt_at = (
        now
        + timedelta(
            seconds=retry_seconds
        )
    )

    job.updated_at = now

    await db.commit()

    print(
        f"🔁 RETRY SCHEDULED | "
        f"id={job.id} | "
        f"attempt={job.attempts} | "
        f"retry={retry_seconds}s | "
        f"reason={reason}",
        flush=True,
    )


# ============================================================
# PROCESS ONE DM JOB
# ============================================================

async def process_job(
    job_id: int,
    client: httpx.AsyncClient,
):

    # --------------------------------------------------------
    # IMPORTANT:
    # Every concurrent job gets its OWN DB SESSION.
    # AsyncSession must not be shared between concurrent tasks.
    # --------------------------------------------------------

    async with AsyncSessionLocal() as db:

        job = await db.get(
            DMJob,
            job_id,
        )

        if job is None:

            print(
                f"⚠️ JOB NOT FOUND | id={job_id}",
                flush=True,
            )

            return

        # ----------------------------------------------------
        # Safety check
        # ----------------------------------------------------

        if job.status != "sending":

            print(
                f"⚠️ JOB NO LONGER SENDING | "
                f"id={job.id} | "
                f"status={job.status}",
                flush=True,
            )

            return

        print(
            f"🚀 SENDING DM | "
            f"job={job.id} | "
            f"user={job.user_id} | "
            f"attempt={job.attempts}",
            flush=True,
        )

        try:

            response = await send_dm_async(
                client=client,
                recipient_user_id=job.user_id,
                message=job.message,
                comment_id=job.comment_id,
                idempotency_key=f"dm-job-{job.id}",
            )

        except Exception:

            logger.exception(
                "send_dm exception | job=%s",
                job.id,
            )

            print(
                f"💥 SEND EXCEPTION | "
                f"job={job.id}",
                flush=True,
            )

            await schedule_retry_or_fail(
                db,
                job,
                "send_dm exception",
            )

            return

        print(
            f"📡 DM API RESPONSE | "
            f"job={job.id} | "
            f"http={response.status_code}",
            flush=True,
        )

        # ====================================================
        # SUCCESS / ACCEPTED
        # ====================================================

        if response.status_code in (
            200,
            202,
        ):

            try:

                data = response.json()

            except ValueError:

                await schedule_retry_or_fail(
                    db,
                    job,
                    "invalid JSON",
                )

                return

            job.dm_id = data.get(
                "dm_id"
            )

            dm_status = data.get(
                "status"
            )

            print(
                f"📨 DM RESULT | "
                f"job={job.id} | "
                f"dm_id={job.dm_id} | "
                f"status={dm_status}",
                flush=True,
            )

            # ------------------------------------------------
            # API SAYS FAILED
            # ------------------------------------------------

            if dm_status == "failed":

                job.status = "failed"
                job.updated_at = utcnow()

                await db.commit()

                print(
                    f"❌ DM FAILED | "
                    f"job={job.id}",
                    flush=True,
                )

                return

            # ------------------------------------------------
            # API SAYS QUEUED
            # ------------------------------------------------

            if dm_status == "queued":

                if not job.dm_id:

                    await schedule_retry_or_fail(
                        db,
                        job,
                        "queued response missing dm_id",
                    )

                    return

                job.status = "waiting"

                job.next_attempt_at = (
                    utcnow()
                    + timedelta(
                        seconds=FIRST_RECONCILE_DELAY_SECONDS
                    )
                )

                job.updated_at = utcnow()

                await db.commit()

                print(
                    f"⏳ DM WAITING | "
                    f"job={job.id} | "
                    f"dm_id={job.dm_id}",
                    flush=True,
                )

                return

            # ------------------------------------------------
            # API SAYS DELIVERED / SENT
            # ------------------------------------------------

            if dm_status in (
                "delivered",
                "sent",
            ):

                job.status = "delivered"
                job.updated_at = utcnow()

                await db.commit()

                print(
                    f"✅ DM DELIVERED | "
                    f"job={job.id} | "
                    f"dm_id={job.dm_id}",
                    flush=True,
                )

                return

            # ------------------------------------------------
            # UNKNOWN STATUS
            # ------------------------------------------------

            print(
                f"⚠️ UNKNOWN DM STATUS | "
                f"job={job.id} | "
                f"status={dm_status}",
                flush=True,
            )

            job.status = "failed"
            job.updated_at = utcnow()

            await db.commit()

            return

        # ====================================================
        # BAD REQUEST
        # ====================================================

        if response.status_code == 400:

            job.status = "failed"
            job.updated_at = utcnow()

            await db.commit()

            print(
                f"❌ HTTP 400 | "
                f"job={job.id} | "
                f"permanent failure",
                flush=True,
            )

            return

        # ====================================================
        # RATE LIMIT
        # ====================================================

        if response.status_code == 429:

            retry_after = response.headers.get(
                "Retry-After",
                "10",
            )

            try:

                retry_seconds = int(
                    retry_after
                )

            except ValueError:

                retry_seconds = 10

            job.status = "queued"

            job.next_attempt_at = (
                utcnow()
                + timedelta(
                    seconds=retry_seconds
                )
            )

            job.updated_at = utcnow()

            await db.commit()

            print(
                f"🚦 RATE LIMITED | "
                f"job={job.id} | "
                f"retry={retry_seconds}s",
                flush=True,
            )

            return

        # ====================================================
        # SERVER ERROR
        # ====================================================

        if response.status_code >= 500:

            await schedule_retry_or_fail(
                db,
                job,
                f"HTTP {response.status_code}",
            )

            return

        # ====================================================
        # UNKNOWN HTTP RESPONSE
        # ====================================================

        job.status = "failed"
        job.updated_at = utcnow()

        await db.commit()

        print(
            f"❌ UNKNOWN HTTP RESPONSE | "
            f"job={job.id} | "
            f"http={response.status_code}",
            flush=True,
        )


# ============================================================
# PROCESS ONE RECONCILIATION JOB
# ============================================================

async def reconcile_job(
    job_id: int,
    client: httpx.AsyncClient,
):

    async with AsyncSessionLocal() as db:

        job = await db.get(
            DMJob,
            job_id,
        )

        if job is None:

            print(
                f"⚠️ RECONCILE JOB NOT FOUND | "
                f"id={job_id}",
                flush=True,
            )

            return

        if job.status != "waiting":

            return

        if not job.dm_id:

            job.status = "failed"
            job.updated_at = utcnow()

            await db.commit()

            return

        print(
            f"🔎 RECONCILING | "
            f"job={job.id} | "
            f"dm_id={job.dm_id}",
            flush=True,
        )

        try:

            response = await get_dm_status_async(
                job.dm_id,
                client,
            )

        except Exception:

            logger.exception(
                "Reconciliation exception | job=%s",
                job.id,
            )

            job.next_attempt_at = (
                utcnow()
                + timedelta(
                    seconds=RECONCILE_POLL_SECONDS
                )
            )

            job.updated_at = utcnow()

            await db.commit()

            return

        print(
            f"📡 RECONCILE RESPONSE | "
            f"job={job.id} | "
            f"http={response.status_code}",
            flush=True,
        )

        # ----------------------------------------------------
        # PERMANENT FAILURE
        # ----------------------------------------------------

        if response.status_code in (
            400,
            404,
        ):

            job.status = "failed"
            job.updated_at = utcnow()

            await db.commit()

            print(
                f"❌ RECONCILIATION FAILED | "
                f"job={job.id}",
                flush=True,
            )

            return

        # ----------------------------------------------------
        # TEMPORARY HTTP ERROR
        # ----------------------------------------------------

        if response.status_code != 200:

            job.next_attempt_at = (
                utcnow()
                + timedelta(
                    seconds=RECONCILE_POLL_SECONDS
                )
            )

            job.updated_at = utcnow()

            await db.commit()

            return

        # ----------------------------------------------------
        # JSON
        # ----------------------------------------------------

        try:

            data = response.json()

        except ValueError:

            job.next_attempt_at = (
                utcnow()
                + timedelta(
                    seconds=RECONCILE_POLL_SECONDS
                )
            )

            job.updated_at = utcnow()

            await db.commit()

            return

        dm_status = data.get(
            "status"
        )

        print(
            f"📋 RECONCILIATION STATUS | "
            f"job={job.id} | "
            f"dm_id={job.dm_id} | "
            f"status={dm_status}",
            flush=True,
        )

        # ----------------------------------------------------
        # DELIVERED
        # ----------------------------------------------------

        if dm_status in (
            "delivered",
            "sent",
        ):

            job.status = "delivered"
            job.updated_at = utcnow()

            await db.commit()

            print(
                f"✅ RECONCILIATION DELIVERED | "
                f"job={job.id}",
                flush=True,
            )

            return

        # ----------------------------------------------------
        # FAILED
        # ----------------------------------------------------

        if dm_status == "failed":

            await schedule_retry_or_fail(
                db,
                job,
                "failed during reconciliation",
            )

            return

        # ----------------------------------------------------
        # STILL QUEUED / PROCESSING
        # ----------------------------------------------------

        job.next_attempt_at = (
            utcnow()
            + timedelta(
                seconds=RECONCILE_POLL_SECONDS
            )
        )

        job.updated_at = utcnow()

        await db.commit()

        print(
            f"⏳ STILL WAITING | "
            f"job={job.id} | "
            f"status={dm_status}",
            flush=True,
        )


# ============================================================
# RUN NEW JOB BATCH
# ============================================================

async def process_new_batch(
    client: httpx.AsyncClient,
) -> int:

    # --------------------------------------------
    # Fetch and lock jobs.
    # --------------------------------------------

    async with AsyncSessionLocal() as db:

        jobs = await fetch_and_lock_jobs(
            db,
            BATCH_SIZE,
        )

    if not jobs:
        return 0

    job_ids = [
        job.id
        for job in jobs
    ]

    print(
        f"🚀 STARTING SEND BATCH | "
        f"jobs={job_ids}",
        flush=True,
    )

    # --------------------------------------------
    # Small batch pacing.
    #
    # This means we wait only once per batch,
    # NOT 6.5 seconds per DM.
    # --------------------------------------------

    if SEND_PACING_SECONDS > 0:

        await asyncio.sleep(
            SEND_PACING_SECONDS
        )

    # --------------------------------------------
    # Process concurrently.
    # --------------------------------------------

    semaphore = asyncio.Semaphore(
        MAX_CONCURRENT_SENDS
    )

    async def worker(job_id: int):

        async with semaphore:

            try:

                await process_job(
                    job_id,
                    client,
                )

            except Exception:

                logger.exception(
                    "Unexpected processing error | "
                    "job=%s",
                    job_id,
                )

                # Recover the job.
                async with AsyncSessionLocal() as db:

                    job = await db.get(
                        DMJob,
                        job_id,
                    )

                    if job:

                        try:

                            await schedule_retry_or_fail(
                                db,
                                job,
                                "unexpected processing error",
                            )

                        except Exception:

                            await db.rollback()

                            logger.exception(
                                "Failed to schedule retry | "
                                "job=%s",
                                job_id,
                            )

    await asyncio.gather(
        *(
            worker(job_id)
            for job_id in job_ids
        )
    )

    print(
        f"🏁 SEND BATCH FINISHED | "
        f"count={len(job_ids)}",
        flush=True,
    )

    return len(job_ids)


# ============================================================
# RUN RECONCILIATION BATCH
# ============================================================

async def process_reconciliation_batch(
    client: httpx.AsyncClient,
) -> int:

    async with AsyncSessionLocal() as db:

        jobs = await fetch_waiting_jobs(
            db,
            BATCH_SIZE,
        )

        if not jobs:
            return 0

        job_ids = [
            job.id
            for job in jobs
        ]

    print(
        f"🔄 STARTING RECONCILIATION BATCH | "
        f"jobs={job_ids}",
        flush=True,
    )

    semaphore = asyncio.Semaphore(
        MAX_CONCURRENT_SENDS
    )

    async def worker(job_id: int):

        async with semaphore:

            try:

                await reconcile_job(
                    job_id,
                    client,
                )

            except Exception:

                logger.exception(
                    "Unexpected reconciliation error | "
                    "job=%s",
                    job_id,
                )

                async with AsyncSessionLocal() as db:

                    job = await db.get(
                        DMJob,
                        job_id,
                    )

                    if job:

                        job.next_attempt_at = (
                            utcnow()
                            + timedelta(
                                seconds=RECONCILE_POLL_SECONDS
                            )
                        )

                        job.updated_at = utcnow()

                        await db.commit()

    await asyncio.gather(
        *(
            worker(job_id)
            for job_id in job_ids
        )
    )

    print(
        f"🏁 RECONCILIATION BATCH FINISHED | "
        f"count={len(job_ids)}",
        flush=True,
    )

    return len(job_ids)


# ============================================================
# WORKER LOOP
# ============================================================

async def run_worker():

    print(
        "🚀 ASYNC CONCURRENT DM WORKER STARTING...",
        flush=True,
    )

    logger.info(
        "Concurrent DM background worker started."
    )

    idle_backoff = IDLE_BACKOFF_INITIAL

    last_heartbeat = (
        asyncio.get_running_loop().time()
    )

    async with httpx.AsyncClient(
        timeout=httpx.Timeout(
            HTTP_TIMEOUT
        ),
        limits=httpx.Limits(
            max_connections=20,
            max_keepalive_connections=10,
        ),
    ) as client:

        print(
            "🌐 HTTP CLIENT CREATED | "
            f"concurrency={MAX_CONCURRENT_SENDS}",
            flush=True,
        )

        while True:

            # =================================================
            # HEARTBEAT
            # =================================================

            now = (
                asyncio.get_running_loop().time()
            )

            if (
                now - last_heartbeat
                >= 10
            ):

                print(
                    "💓 DM WORKER ALIVE | "
                    f"concurrency={MAX_CONCURRENT_SENDS}",
                    flush=True,
                )

                last_heartbeat = now

            processed = 0

            # =================================================
            # RECONCILIATION FIRST
            # =================================================

            try:

                processed = (
                    await process_reconciliation_batch(
                        client
                    )
                )

            except Exception:

                logger.exception(
                    "Reconciliation batch error"
                )

                print(
                    "💥 RECONCILIATION BATCH ERROR",
                    flush=True,
                )

            if processed > 0:

                idle_backoff = (
                    IDLE_BACKOFF_INITIAL
                )

                continue

            # =================================================
            # NEW DM JOBS
            # =================================================

            try:

                processed = (
                    await process_new_batch(
                        client
                    )
                )

            except Exception:

                logger.exception(
                    "New job batch error"
                )

                print(
                    "💥 NEW JOB BATCH ERROR",
                    flush=True,
                )

            if processed > 0:

                idle_backoff = (
                    IDLE_BACKOFF_INITIAL
                )

                continue

            # =================================================
            # NOTHING TO DO
            # =================================================

            print(
                f"😴 WORKER IDLE | "
                f"sleeping={idle_backoff}s",
                flush=True,
            )

            await asyncio.sleep(
                idle_backoff
            )

            idle_backoff = min(
                idle_backoff * 2,
                IDLE_BACKOFF_MAX,
            )


# ============================================================
# ENTRY POINT
# ============================================================

if __name__ == "__main__":

    print(
        "🏁 EXECUTING dm_worker.py",
        flush=True,
    )

    try:

        asyncio.run(
            run_worker()
        )

    except KeyboardInterrupt:

        print(
            "🛑 DM WORKER STOPPED",
            flush=True,
        )

    except Exception as e:

        print(
            f"💀 DM WORKER CRASHED | {e}",
            flush=True,
        )

        logger.exception(
            "DM worker process crashed"
        )

        raise