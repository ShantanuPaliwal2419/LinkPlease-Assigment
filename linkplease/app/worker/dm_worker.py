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

# Number of DMs that can be sent concurrently.
SEND_CONCURRENCY = 5

# Number of reconciliation requests concurrently.
RECONCILE_CONCURRENCY = 10

# Number of new send jobs claimed at once.
BATCH_SIZE = 5

# How often waiting DMs are checked.
RECONCILE_POLL_SECONDS = 3

# First reconciliation after send.
FIRST_RECONCILE_DELAY_SECONDS = 2

# Worker idle polling.
IDLE_SLEEP_MIN = 0.5
IDLE_SLEEP_MAX = 3

# Recover jobs stuck in "sending".
STUCK_JOB_MINUTES = 2


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
    Atomically claim queued jobs.

    Also recovers jobs that were stuck in "sending"
    because the worker crashed/restarted.

    IMPORTANT:
    attempts are incremented ONLY when we actually
    start a send attempt.
    """

    now = utcnow()

    stuck_cutoff = (
        now - timedelta(minutes=STUCK_JOB_MINUTES)
    )

    stmt = (
        select(DMJob)
        .where(
            or_(
                # ------------------------------------------------
                # Normal queued jobs
                # ------------------------------------------------
                (
                    (DMJob.status == "queued")
                    & (DMJob.attempts < MAX_ATTEMPTS)
                ),

                # ------------------------------------------------
                # Recover crashed/stuck send jobs
                # ------------------------------------------------
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

    job_data = []

    for job in jobs:

        job.status = "sending"

        # Increment ONLY when actually sending.
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
        f"🔒 BATCH LOCKED | "
        f"count={len(job_data)} | "
        f"jobs={[j['id'] for j in job_data]}",
        flush=True,
    )

    return job_data


# ============================================================
# FETCH WAITING JOBS
# ============================================================

async def fetch_waiting_jobs(
    db: AsyncSession,
    limit: int = RECONCILE_CONCURRENCY,
):
    """
    Fetch DMs waiting for delivery reconciliation.
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

        # Prevent immediate re-selection.
        job.next_attempt_at = (
            now
            + timedelta(
                seconds=RECONCILE_POLL_SECONDS
            )
        )

        job.updated_at = now

    await db.commit()

    print(
        f"🔄 WAITING BATCH FOUND | "
        f"count={len(job_data)}",
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
    """

    async with AsyncSessionLocal() as db:

        job = await db.get(
            DMJob,
            job_id,
        )

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
    Put the job back into queue if attempts remain.

    Otherwise permanently fail it.
    """

    async with AsyncSessionLocal() as db:

        job = await db.get(
            DMJob,
            job_id,
        )

        if job is None:
            return

        # ========================================================
        # MAX ATTEMPTS REACHED
        # ========================================================

        if attempts >= MAX_ATTEMPTS:

            job.status = "failed"
            job.next_attempt_at = None
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

        # ========================================================
        # EXPONENTIAL BACKOFF
        # ========================================================

        retry_seconds = min(
            2 ** attempts,
            30,
        )

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

        # ========================================================
        # SUCCESS / ACCEPTED
        # ========================================================

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

            # ----------------------------------------------------
            # QUEUED
            # ----------------------------------------------------

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

            # ----------------------------------------------------
            # IMMEDIATELY DELIVERED
            # ----------------------------------------------------

            if dm_status in (
                "sent",
                "delivered",
            ):

                await update_job(
                    job_id,
                    dm_id=dm_id,
                    status="delivered",
                    next_attempt_at=None,
                )

                print(
                    f"✅ DM DELIVERED | "
                    f"job={job_id}",
                    flush=True,
                )

                return

            # ----------------------------------------------------
            # API FAILED
            # ----------------------------------------------------

            if dm_status == "failed":

                await update_job(
                    job_id,
                    dm_id=dm_id,
                    status="failed",
                    next_attempt_at=None,
                )

                print(
                    f"❌ DM API FAILED | "
                    f"job={job_id}",
                    flush=True,
                )

                return

            # ----------------------------------------------------
            # UNKNOWN API STATUS
            # ----------------------------------------------------

            await schedule_retry_or_fail(
                job_id,
                attempts,
                f"unknown DM status: {dm_status}",
            )

            return

        # ========================================================
        # BAD REQUEST
        # ========================================================

        if response.status_code == 400:

            await update_job(
                job_id,
                status="failed",
                next_attempt_at=None,
            )

            print(
                f"❌ HTTP 400 | "
                f"job={job_id}",
                flush=True,
            )

            return

        # ========================================================
        # RATE LIMIT
        # ========================================================

        if response.status_code == 429:

            retry_after = response.headers.get(
                "Retry-After"
            )

            try:

                retry_seconds = (
                    int(retry_after)
                    if retry_after
                    else 10
                )

            except ValueError:

                retry_seconds = 10

            # ----------------------------------------------------
            # IMPORTANT FIX
            #
            # Never requeue if MAX_ATTEMPTS has already
            # been reached.
            # ----------------------------------------------------

            if attempts >= MAX_ATTEMPTS:

                await update_job(
                    job_id,
                    status="failed",
                    next_attempt_at=None,
                )

                print(
                    f"❌ RATE LIMIT + MAX ATTEMPTS | "
                    f"job={job_id} | "
                    f"attempts={attempts}",
                    flush=True,
                )

                return

            await update_job(
                job_id,
                status="queued",
                next_attempt_at=(
                    utcnow()
                    + timedelta(
                        seconds=retry_seconds
                    )
                ),
            )

            print(
                f"🚦 RATE LIMITED | "
                f"job={job_id} | "
                f"attempt={attempts} | "
                f"retry={retry_seconds}s",
                flush=True,
            )

            return

        # ========================================================
        # SERVER ERROR
        # ========================================================

        if response.status_code >= 500:

            await schedule_retry_or_fail(
                job_id,
                attempts,
                f"HTTP {response.status_code}",
            )

            return

        # ========================================================
        # UNKNOWN HTTP STATUS
        # ========================================================

        await update_job(
            job_id,
            status="failed",
            next_attempt_at=None,
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

        # ========================================================
        # PERMANENT API ERROR
        # ========================================================

        if response.status_code in (
            400,
            404,
        ):

            await update_job(
                job_id,
                status="failed",
                next_attempt_at=None,
            )

            print(
                f"❌ RECONCILIATION FAILED | "
                f"job={job_id}",
                flush=True,
            )

            return

        # ========================================================
        # TEMPORARY ERROR
        # ========================================================

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

        # ========================================================
        # PARSE
        # ========================================================

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

        # ========================================================
        # DELIVERED
        # ========================================================

        if status == "delivered":

            await update_job(
                job_id,
                status="delivered",
                next_attempt_at=None,
            )

            print(
                f"✅ DELIVERED | "
                f"job={job_id}",
                flush=True,
            )

            return

        # ========================================================
        # SENT
        # ========================================================

        if status == "sent":

            await update_job(
                job_id,
                status="delivered",
                next_attempt_at=None,
            )

            print(
                f"✅ SENT | "
                f"job={job_id}",
                flush=True,
            )

            return

        # ========================================================
        # FAILED
        # ========================================================

        if status == "failed":

            # DM already exists.
            # DO NOT increment send attempts.
            # DO NOT resend automatically.

            await update_job(
                job_id,
                status="failed",
                next_attempt_at=None,
            )

            print(
                f"❌ DELIVERY FAILED | "
                f"job={job_id} | "
                f"dm_id={dm_id}",
                flush=True,
            )

            return

        # ========================================================
        # STILL QUEUED
        # ========================================================

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

    results = await asyncio.gather(
        *tasks,
        return_exceptions=True,
    )

    # Log unexpected task-level exceptions.
    for job, result in zip(jobs, results):

        if isinstance(result, Exception):

            logger.error(
                "SEND TASK EXCEPTION | "
                "job=%s | error=%r",
                job["id"],
                result,
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

    results = await asyncio.gather(
        *tasks,
        return_exceptions=True,
    )

    for job, result in zip(jobs, results):

        if isinstance(result, Exception):

            logger.error(
                "RECONCILE TASK EXCEPTION | "
                "job=%s | error=%r",
                job["id"],
                result,
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

    # ========================================================
    # PERSISTENT HTTP CLIENT
    # ========================================================

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
            # 1. RECONCILIATION
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
            # 2. SEND NEW JOBS
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
            # RESET BACKOFF
            # =================================================

            if found_work:

                idle_sleep = IDLE_SLEEP_MIN

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