import asyncio
import logging
from datetime import datetime, timedelta, timezone

import httpx
from sqlalchemy import or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.db import AsyncSessionLocal
from app.models.dm_job import DMJob
from app.services.dm_service import (
    send_dm_async,
    get_dm_status_async,
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
    "🔥🔥🔥 FAST ASYNC DM WORKER STARTED 🔥🔥🔥",
    flush=True,
)


# ============================================================
# CONFIG
# ============================================================

MAX_ATTEMPTS = 5

# Number of DMs sent concurrently.
SEND_CONCURRENCY = 5

# Number of reconciliation requests concurrently.
RECONCILE_CONCURRENCY = 10

# How many jobs to claim from DB at once.
BATCH_SIZE = 5

# How frequently queued DMs are checked.
RECONCILE_POLL_SECONDS = 3

# First reconciliation after send.
FIRST_RECONCILE_DELAY_SECONDS = 2

# Worker idle polling.
IDLE_SLEEP_MIN = 0.5
IDLE_SLEEP_MAX = 3

# Jobs stuck in "sending" longer than this are recovered.
STUCK_JOB_MINUTES = 2

# HTTP timeout.
HTTP_TIMEOUT = 15.0


# ============================================================
# TIME HELPER
# ============================================================

def utcnow():
    return datetime.now(timezone.utc)


# ============================================================
# FETCH + LOCK NEW JOBS
# ============================================================

async def fetch_and_lock_jobs(
    db: AsyncSession,
    limit: int = BATCH_SIZE,
):
    """
    Atomically claim a batch of queued/stuck jobs.

    Important:
    We only increment attempts when a job is actually being
    sent again.
    """

    now = utcnow()

    stuck_cutoff = (
        now
        - timedelta(minutes=STUCK_JOB_MINUTES)
    )

    stmt = (
        select(DMJob)
        .where(
            or_(
                (
                    (DMJob.status == "queued")
                    & (DMJob.attempts < MAX_ATTEMPTS)
                ),
                (
                    (DMJob.status == "sending")
                    & (DMJob.updated_at <= stuck_cutoff)
                    & (DMJob.attempts < MAX_ATTEMPTS)
                ),
            ),
            DMJob.next_attempt_at <= now,
        )
        .order_by(DMJob.id)
        .limit(limit)
        .with_for_update(skip_locked=True)
    )

    result = await db.execute(stmt)

    jobs = result.scalars().all()

    if not jobs:
        return []

    # --------------------------------------------------------
    # Capture everything needed BEFORE commit.
    # --------------------------------------------------------

    job_data = []

    for job in jobs:

        job.status = "sending"
        job.attempts += 1
        job.updated_at = now

        job_data.append(
            {
                "id": job.id,
                "user_id": job.user_id,
                "message": job.message,
                "comment_id": job.comment_id,
                "attempts": job.attempts,
            }
        )

    await db.commit()

    print(
        f"🔒 BATCH LOCKED | count={len(job_data)} | "
        f"jobs={[j['id'] for j in job_data]}",
        flush=True,
    )

    return job_data


# ============================================================
# FETCH WAITING JOBS
# ============================================================

async def fetch_waiting_jobs(
    db: AsyncSession,
    limit: int = BATCH_SIZE,
):
    """
    Fetch DMs that need reconciliation.
    """

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
        .with_for_update(skip_locked=True)
    )

    result = await db.execute(stmt)

    jobs = result.scalars().all()

    if not jobs:
        return []

    job_data = []

    for job in jobs:

        job_data.append(
            {
                "id": job.id,
                "dm_id": job.dm_id,
                "attempts": job.attempts,
            }
        )

        # Temporarily move the job forward so another worker
        # doesn't immediately pick it up.
        job.next_attempt_at = (
            now
            + timedelta(seconds=RECONCILE_POLL_SECONDS)
        )

        job.updated_at = now

    await db.commit()

    print(
        f"🔄 WAITING BATCH FOUND | count={len(job_data)}",
        flush=True,
    )

    return job_data


# ============================================================
# UPDATE JOB
# ============================================================

async def update_job(
    job_id: int,
    **values,
):
    """
    Every concurrent task gets its own DB session.

    This is important:
    AsyncSession should NOT be shared between concurrent
    asyncio tasks.
    """

    async with AsyncSessionLocal() as db:

        job = await db.get(DMJob, job_id)

        if job is None:
            return

        for key, value in values.items():
            setattr(job, key, value)

        job.updated_at = utcnow()

        await db.commit()


# ============================================================
# RETRY / FAIL
# ============================================================

async def schedule_retry_or_fail(
    job_id: int,
    attempts: int,
    reason: str,
):
    """
    Retry only when we genuinely need another send attempt.

    Never allow attempts to exceed MAX_ATTEMPTS.
    """

    async with AsyncSessionLocal() as db:

        job = await db.get(DMJob, job_id)

        if job is None:
            return

        # ----------------------------------------------------
        # HARD ATTEMPT LIMIT
        # ----------------------------------------------------

        if attempts >= MAX_ATTEMPTS:

            job.status = "failed"
            job.updated_at = utcnow()

            await db.commit()

            print(
                f"❌ PERMANENT FAILURE | "
                f"job={job_id} | "
                f"attempts={attempts} | "
                f"reason={reason}",
                flush=True,
            )

            return

        # ----------------------------------------------------
        # Exponential backoff
        # ----------------------------------------------------

        retry_seconds = min(
            2 ** attempts,
            30,
        )

        job.status = "queued"

        job.next_attempt_at = (
            utcnow()
            + timedelta(seconds=retry_seconds)
        )

        job.updated_at = utcnow()

        await db.commit()

        print(
            f"🔁 RETRY | "
            f"job={job_id} | "
            f"attempt={attempts} | "
            f"retry={retry_seconds}s | "
            f"reason={reason}",
            flush=True,
        )


# ============================================================
# PROCESS ONE SEND JOB
# ============================================================

async def process_send_job(
    client: httpx.AsyncClient,
    job: dict,
    semaphore: asyncio.Semaphore,
):
    """
    Process one DM.

    Network calls happen concurrently.
    """

    async with semaphore:

        job_id = job["id"]
        user_id = job["user_id"]
        message = job["message"]
        comment_id = job["comment_id"]
        attempts = job["attempts"]

        print(
            f"🚀 SENDING DM | "
            f"job={job_id} | "
            f"user={user_id} | "
            f"attempt={attempts}",
            flush=True,
        )

        try:

            response = await send_dm_async(
                client=client,
                recipient_user_id=user_id,
                message=message,
                comment_id=comment_id,
                idempotency_key=f"dm-job-{job_id}",
            )

        except Exception:

            logger.exception(
                "SEND EXCEPTION | job=%s",
                job_id,
            )

            await schedule_retry_or_fail(
                job_id,
                attempts,
                "send exception",
            )

            return

        print(
            f"📡 DM RESPONSE | "
            f"job={job_id} | "
            f"http={response.status_code}",
            flush=True,
        )

        # ====================================================
        # SUCCESS / ACCEPTED
        # ====================================================

        if response.status_code in (200, 202):

            try:
                data = response.json()

            except ValueError:

                await schedule_retry_or_fail(
                    job_id,
                    attempts,
                    "invalid JSON",
                )

                return

            dm_id = data.get("dm_id")
            dm_status = data.get("status")

            print(
                f"📨 DM RESULT | "
                f"job={job_id} | "
                f"dm_id={dm_id} | "
                f"status={dm_status}",
                flush=True,
            )

            # ------------------------------------------------
            # API ACCEPTED AND QUEUED
            # ------------------------------------------------

            if dm_status == "queued":

                if not dm_id:

                    await schedule_retry_or_fail(
                        job_id,
                        attempts,
                        "queued without dm_id",
                    )

                    return

                await update_job(
                    job_id,
                    dm_id=dm_id,
                    status="waiting",
                    next_attempt_at=(
                        utcnow()
                        + timedelta(
                            seconds=FIRST_RECONCILE_DELAY_SECONDS
                        )
                    ),
                )

                print(
                    f"⏳ DM WAITING | "
                    f"job={job_id} | "
                    f"dm_id={dm_id}",
                    flush=True,
                )

                return

            # ------------------------------------------------
            # IMMEDIATELY DELIVERED
            # ------------------------------------------------

            if dm_status in ("sent", "delivered"):

                await update_job(
                    job_id,
                    dm_id=dm_id,
                    status="delivered",
                )

                print(
                    f"✅ DM DELIVERED | job={job_id}",
                    flush=True,
                )

                return

            # ------------------------------------------------
            # API IMMEDIATELY FAILED
            # ------------------------------------------------

            if dm_status == "failed":

                # Important:
                # Don't blindly retry if API has already
                # created a DM and explicitly reports failure.
                await update_job(
                    job_id,
                    dm_id=dm_id,
                    status="failed",
                )

                print(
                    f"❌ DM API FAILED | job={job_id}",
                    flush=True,
                )

                return

            # ------------------------------------------------
            # UNKNOWN STATUS
            # ------------------------------------------------

            await schedule_retry_or_fail(
                job_id,
                attempts,
                f"unknown DM status: {dm_status}",
            )

            return

        # ====================================================
        # BAD REQUEST
        # ====================================================

        if response.status_code == 400:

            await update_job(
                job_id,
                status="failed",
            )

            print(
                f"❌ HTTP 400 | job={job_id}",
                flush=True,
            )

            return

        # ====================================================
        # RATE LIMIT
        # ====================================================

        if response.status_code == 429:

            retry_after = response.headers.get(
                "Retry-After"
            )

            try:
                retry_seconds = int(
                    retry_after
                ) if retry_after else 10

            except ValueError:
                retry_seconds = 10

            async with AsyncSessionLocal() as db:

                db_job = await db.get(
                    DMJob,
                    job_id,
                )

                if db_job:

                    db_job.status = "queued"

                    db_job.next_attempt_at = (
                        utcnow()
                        + timedelta(
                            seconds=retry_seconds
                        )
                    )

                    db_job.updated_at = utcnow()

                    await db.commit()

            print(
                f"🚦 RATE LIMITED | "
                f"job={job_id} | "
                f"retry={retry_seconds}s",
                flush=True,
            )

            return

        # ====================================================
        # SERVER ERROR
        # ====================================================

        if response.status_code >= 500:

            await schedule_retry_or_fail(
                job_id,
                attempts,
                f"HTTP {response.status_code}",
            )

            return

        # ====================================================
        # UNKNOWN HTTP STATUS
        # ====================================================

        await update_job(
            job_id,
            status="failed",
        )

        print(
            f"❌ UNKNOWN HTTP STATUS | "
            f"job={job_id} | "
            f"http={response.status_code}",
            flush=True,
        )


# ============================================================
# RECONCILE ONE DM
# ============================================================

async def reconcile_one(
    client: httpx.AsyncClient,
    job: dict,
    semaphore: asyncio.Semaphore,
):

    async with semaphore:

        job_id = job["id"]
        dm_id = job["dm_id"]
        attempts = job["attempts"]

        print(
            f"🔎 RECONCILING | "
            f"job={job_id} | "
            f"dm_id={dm_id}",
            flush=True,
        )

        try:

            response = await get_dm_status_async(
                dm_id,
                client,
            )

        except Exception:

            logger.exception(
                "RECONCILIATION EXCEPTION | job=%s",
                job_id,
            )

            await update_job(
                job_id,
                next_attempt_at=(
                    utcnow()
                    + timedelta(
                        seconds=RECONCILE_POLL_SECONDS
                    )
                ),
            )

            return

        print(
            f"📡 RECONCILE RESPONSE | "
            f"job={job_id} | "
            f"http={response.status_code}",
            flush=True,
        )

        # ====================================================
        # PERMANENT API ERROR
        # ====================================================

        if response.status_code in (400, 404):

            await update_job(
                job_id,
                status="failed",
            )

            print(
                f"❌ RECONCILIATION FAILED | "
                f"job={job_id}",
                flush=True,
            )

            return

        # ====================================================
        # TEMPORARY ERROR
        # ====================================================

        if response.status_code != 200:

            await update_job(
                job_id,
                next_attempt_at=(
                    utcnow()
                    + timedelta(
                        seconds=RECONCILE_POLL_SECONDS
                    )
                ),
            )

            return

        # ====================================================
        # PARSE
        # ====================================================

        try:

            data = response.json()

        except ValueError:

            await update_job(
                job_id,
                next_attempt_at=(
                    utcnow()
                    + timedelta(
                        seconds=RECONCILE_POLL_SECONDS
                    )
                ),
            )

            return

        status = data.get("status")

        print(
            f"📋 DM STATUS | "
            f"job={job_id} | "
            f"status={status}",
            flush=True,
        )

        # ====================================================
        # DELIVERED
        # ====================================================

        if status == "delivered":

            await update_job(
                job_id,
                status="delivered",
            )

            print(
                f"✅ DELIVERED | job={job_id}",
                flush=True,
            )

            return

        # ====================================================
        # SENT
        # ====================================================

        if status == "sent":

            await update_job(
                job_id,
                status="delivered",
            )

            print(
                f"✅ SENT | job={job_id}",
                flush=True,
            )

            return

        # ====================================================
        # FAILED
        # ====================================================

        if status == "failed":

            # IMPORTANT:
            #
            # The send request already succeeded and produced
            # a dm_id.
            #
            # This is a delivery failure, NOT a new send
            # attempt.
            #
            # Therefore don't increment attempts and don't
            # resend the same DM automatically.

            await update_job(
                job_id,
                status="failed",
            )

            print(
                f"❌ DELIVERY FAILED | "
                f"job={job_id} | "
                f"dm_id={dm_id}",
                flush=True,
            )

            return

        # ====================================================
        # STILL QUEUED
        # ====================================================

        await update_job(
            job_id,
            status="waiting",
            next_attempt_at=(
                utcnow()
                + timedelta(
                    seconds=RECONCILE_POLL_SECONDS
                )
            ),
        )

        print(
            f"⏳ STILL WAITING | "
            f"job={job_id} | "
            f"status={status}",
            flush=True,
        )


# ============================================================
# PROCESS SEND BATCH
# ============================================================

async def process_send_batch(
    client: httpx.AsyncClient,
    jobs: list,
):

    if not jobs:
        return

    print(
        f"🚀 STARTING SEND BATCH | "
        f"jobs={[j['id'] for j in jobs]}",
        flush=True,
    )

    semaphore = asyncio.Semaphore(
        SEND_CONCURRENCY
    )

    tasks = [
        process_send_job(
            client,
            job,
            semaphore,
        )
        for job in jobs
    ]

    await asyncio.gather(
        *tasks,
        return_exceptions=True,
    )

    print(
        f"🏁 SEND BATCH FINISHED | "
        f"count={len(jobs)}",
        flush=True,
    )


# ============================================================
# PROCESS RECONCILIATION BATCH
# ============================================================

async def process_reconcile_batch(
    client: httpx.AsyncClient,
    jobs: list,
):

    if not jobs:
        return

    print(
        f"🔄 STARTING RECONCILIATION BATCH | "
        f"jobs={[j['id'] for j in jobs]}",
        flush=True,
    )

    semaphore = asyncio.Semaphore(
        RECONCILE_CONCURRENCY
    )

    tasks = [
        reconcile_one(
            client,
            job,
            semaphore,
        )
        for job in jobs
    ]

    await asyncio.gather(
        *tasks,
        return_exceptions=True,
    )

    print(
        f"🏁 RECONCILIATION BATCH FINISHED | "
        f"count={len(jobs)}",
        flush=True,
    )


# ============================================================
# WORKER
# ============================================================

async def run_worker():

    print(
        "🚀 FAST ASYNC DM WORKER STARTING",
        flush=True,
    )

    print(
        f"⚙️ SEND_CONCURRENCY={SEND_CONCURRENCY}",
        flush=True,
    )

    print(
        f"⚙️ RECONCILE_CONCURRENCY="
        f"{RECONCILE_CONCURRENCY}",
        flush=True,
    )

    print(
        f"⚙️ BATCH_SIZE={BATCH_SIZE}",
        flush=True,
    )

    print(
        f"⚙️ MAX_ATTEMPTS={MAX_ATTEMPTS}",
        flush=True,
    )

    # --------------------------------------------------------
    # Persistent HTTP client
    # --------------------------------------------------------

    limits = httpx.Limits(
        max_connections=20,
        max_keepalive_connections=20,
    )

    timeout = httpx.Timeout(
        connect=5.0,
        read=15.0,
        write=15.0,
        pool=5.0,
    )

    async with httpx.AsyncClient(
        timeout=timeout,
        limits=limits,
    ) as client:

        print(
            "🌐 HTTP CLIENT READY",
            flush=True,
        )

        idle_sleep = IDLE_SLEEP_MIN

        last_heartbeat = (
            asyncio.get_running_loop().time()
        )

        while True:

            found_work = False

            # =================================================
            # HEARTBEAT
            # =================================================

            now = (
                asyncio.get_running_loop().time()
            )

            if now - last_heartbeat >= 10:

                print(
                    f"💓 WORKER ALIVE | "
                    f"send_concurrency={SEND_CONCURRENCY} | "
                    f"reconcile_concurrency="
                    f"{RECONCILE_CONCURRENCY}",
                    flush=True,
                )

                last_heartbeat = now

            # =================================================
            # 1. RECONCILIATION FIRST
            # =================================================

            try:

                async with AsyncSessionLocal() as db:

                    waiting_jobs = (
                        await fetch_waiting_jobs(
                            db,
                            RECONCILE_CONCURRENCY,
                        )
                    )

                if waiting_jobs:

                    found_work = True

                    await process_reconcile_batch(
                        client,
                        waiting_jobs,
                    )

            except Exception:

                logger.exception(
                    "Waiting batch failed"
                )

            # =================================================
            # 2. NEW SEND JOBS
            # =================================================

            try:

                async with AsyncSessionLocal() as db:

                    new_jobs = (
                        await fetch_and_lock_jobs(
                            db,
                            BATCH_SIZE,
                        )
                    )

                if new_jobs:

                    found_work = True

                    await process_send_batch(
                        client,
                        new_jobs,
                    )

            except Exception:

                logger.exception(
                    "Send batch failed"
                )

            # =================================================
            # RESET BACKOFF WHEN WORK EXISTS
            # =================================================

            if found_work:

                idle_sleep = IDLE_SLEEP_MIN

                # Immediately loop again.
                continue

            # =================================================
            # NO WORK
            # =================================================

            print(
                f"😴 WORKER IDLE | "
                f"sleeping={idle_sleep}s",
                flush=True,
            )

            await asyncio.sleep(
                idle_sleep
            )

            idle_sleep = min(
                idle_sleep * 2,
                IDLE_SLEEP_MAX,
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