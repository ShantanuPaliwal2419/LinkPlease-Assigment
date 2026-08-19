import asyncio
import logging
from datetime import datetime, timedelta, timezone

from sqlalchemy import select, or_
from sqlalchemy.ext.asyncio import AsyncSession
import httpx

from app.db import AsyncSessionLocal
from app.models.dm_job import DMJob
from app.services.dm_service import send_dm_async, get_dm_status_async


# ============================================================
# LOGGING
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

logger = logging.getLogger(__name__)

print("🔥🔥🔥 DM WORKER PROCESS STARTED 🔥🔥🔥", flush=True)


# ============================================================
# CONFIG
# ============================================================

MAX_ATTEMPTS = 5
RECONCILE_POLL_SECONDS = 10
FIRST_RECONCILE_DELAY_SECONDS = 3


# ============================================================
# FETCH + LOCK NEXT JOB
# ============================================================

async def fetch_and_lock_next_job(
    db: AsyncSession,
) -> DMJob | None:

    stuck_cutoff = (
        datetime.now(timezone.utc)
        - timedelta(minutes=2)
    )

    print("🔍 Checking for queued/stuck DM jobs...", flush=True)

    stmt = (
        select(DMJob)
        .where(
            or_(
                DMJob.status == "queued",

                (
                    (DMJob.status == "sending")
                    & (DMJob.updated_at <= stuck_cutoff)
                ),
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

        print(
            f"📦 JOB PICKED | "
            f"id={job.id} | "
            f"status={job.status} | "
            f"attempts={job.attempts} | "
            f"user={job.user_id}",
            flush=True,
        )

        job.status = "sending"
        job.attempts += 1
        job.updated_at = datetime.now(timezone.utc)

        await db.commit()

        print(
            f"🔒 JOB LOCKED | "
            f"id={job.id} | "
            f"status=sending | "
            f"attempt={job.attempts}",
            flush=True,
        )

    return job


# ============================================================
# GET WAITING JOB
# ============================================================

async def get_waiting_job(
    db: AsyncSession,
) -> DMJob | None:

    print("🔍 Checking for waiting reconciliation jobs...", flush=True)

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

    job = result.scalar_one_or_none()

    if job:
        print(
            f"🔄 RECONCILE JOB PICKED | "
            f"id={job.id} | "
            f"dm_id={job.dm_id}",
            flush=True,
        )

    return job


# ============================================================
# RETRY / FAIL
# ============================================================

async def _schedule_retry_or_fail(
    db: AsyncSession,
    job: DMJob,
    reason: str,
):

    if job.attempts >= MAX_ATTEMPTS:

        job.status = "failed"
        job.updated_at = datetime.now(timezone.utc)

        await db.commit()

        print(
            f"❌ JOB FAILED PERMANENTLY | "
            f"id={job.id} | "
            f"attempts={job.attempts} | "
            f"reason={reason}",
            flush=True,
        )

        logger.warning(
            "Job %s exhausted retries (%s)",
            job.id,
            reason,
        )

        return

    retry_seconds = min(
        2 ** job.attempts,
        60,
    )

    job.status = "queued"

    job.next_attempt_at = (
        datetime.now(timezone.utc)
        + timedelta(seconds=retry_seconds)
    )

    job.updated_at = datetime.now(timezone.utc)

    await db.commit()

    print(
        f"🔁 JOB RETRY SCHEDULED | "
        f"id={job.id} | "
        f"attempt={job.attempts} | "
        f"retry_in={retry_seconds}s | "
        f"reason={reason}",
        flush=True,
    )


# ============================================================
# PROCESS NEW DM JOB
# ============================================================

async def process_job(
    db: AsyncSession,
    client: httpx.AsyncClient,
    job: DMJob,
):

    print(
        f"⏳ JOB PACING | "
        f"id={job.id} | "
        f"sleeping=6.5s",
        flush=True,
    )

    # NON-BLOCKING PACING
    await asyncio.sleep(6.5)

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

        print(
            f"💥 SEND DM EXCEPTION | "
            f"job={job.id}",
            flush=True,
        )

        logger.exception(
            "Job %s: send_dm exception",
            job.id,
        )

        await _schedule_retry_or_fail(
            db,
            job,
            "send_dm exception",
        )

        return

    print(
        f"📡 DM API RESPONSE | "
        f"job={job.id} | "
        f"http_status={response.status_code}",
        flush=True,
    )

    # ========================================================
    # SUCCESS / ACCEPTED
    # ========================================================

    if response.status_code in (200, 202):

        try:
            data = response.json()

        except ValueError:

            print(
                f"❌ INVALID JSON | job={job.id}",
                flush=True,
            )

            await _schedule_retry_or_fail(
                db,
                job,
                "invalid JSON",
            )

            return

        job.dm_id = data.get("dm_id")

        dm_status = data.get("status")

        print(
            f"📨 DM RESULT | "
            f"job={job.id} | "
            f"dm_id={job.dm_id} | "
            f"status={dm_status}",
            flush=True,
        )

        # ----------------------------------------------------
        # API SAYS FAILED
        # ----------------------------------------------------

        if dm_status == "failed":

            job.status = "failed"
            job.updated_at = datetime.now(timezone.utc)

            await db.commit()

            print(
                f"❌ DM FAILED IMMEDIATELY | "
                f"job={job.id}",
                flush=True,
            )

            return

        # ----------------------------------------------------
        # API SAYS QUEUED
        # ----------------------------------------------------

        if dm_status == "queued":

            if not job.dm_id:

                await _schedule_retry_or_fail(
                    db,
                    job,
                    "queued missing dm_id",
                )

                return

            job.status = "waiting"

            job.next_attempt_at = (
                datetime.now(timezone.utc)
                + timedelta(
                    seconds=FIRST_RECONCILE_DELAY_SECONDS
                )
            )

            job.updated_at = datetime.now(timezone.utc)

            await db.commit()

            print(
                f"⏳ DM WAITING FOR RECONCILIATION | "
                f"job={job.id} | "
                f"dm_id={job.dm_id}",
                flush=True,
            )

            return

        # ----------------------------------------------------
        # ALREADY DELIVERED
        # ----------------------------------------------------

        if dm_status in ("delivered", "sent"):

            job.status = "delivered"
            job.updated_at = datetime.now(timezone.utc)

            await db.commit()

            print(
                f"✅ DM DELIVERED | "
                f"job={job.id} | "
                f"dm_id={job.dm_id}",
                flush=True,
            )

            return

        # ----------------------------------------------------
        # UNKNOWN STATUS
        # ----------------------------------------------------

        print(
            f"⚠️ UNKNOWN DM STATUS | "
            f"job={job.id} | "
            f"status={dm_status}",
            flush=True,
        )

        job.status = "failed"
        job.updated_at = datetime.now(timezone.utc)

        await db.commit()

        return

    # ========================================================
    # BAD REQUEST
    # ========================================================

    if response.status_code == 400:

        print(
            f"❌ HTTP 400 | "
            f"job={job.id} | "
            f"permanent failure",
            flush=True,
        )

        job.status = "failed"
        job.updated_at = datetime.now(timezone.utc)

        await db.commit()

        return

    # ========================================================
    # RATE LIMITED
    # ========================================================

    if response.status_code == 429:

        retry_after = response.headers.get(
            "Retry-After",
            "10",
        )

        try:
            retry_seconds = int(retry_after)

        except ValueError:
            retry_seconds = 10

        print(
            f"🚦 RATE LIMITED | "
            f"job={job.id} | "
            f"retry_after={retry_seconds}s",
            flush=True,
        )

        job.status = "queued"

        job.next_attempt_at = (
            datetime.now(timezone.utc)
            + timedelta(seconds=retry_seconds)
        )

        job.updated_at = datetime.now(timezone.utc)

        await db.commit()

        await asyncio.sleep(retry_seconds)

        return

    # ========================================================
    # SERVER ERROR
    # ========================================================

    if response.status_code >= 500:

        print(
            f"🔥 SERVER ERROR | "
            f"job={job.id} | "
            f"http_status={response.status_code}",
            flush=True,
        )

        await _schedule_retry_or_fail(
            db,
            job,
            f"HTTP {response.status_code}",
        )

        return

    # ========================================================
    # UNKNOWN HTTP RESPONSE
    # ========================================================

    print(
        f"❌ UNKNOWN HTTP RESPONSE | "
        f"job={job.id} | "
        f"http_status={response.status_code}",
        flush=True,
    )

    job.status = "failed"
    job.updated_at = datetime.now(timezone.utc)

    await db.commit()


# ============================================================
# RECONCILE DM
# ============================================================

async def reconcile_job(
    db: AsyncSession,
    client: httpx.AsyncClient,
    job: DMJob,
):

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

        print(
            f"💥 RECONCILIATION EXCEPTION | "
            f"job={job.id}",
            flush=True,
        )

        job.next_attempt_at = (
            datetime.now(timezone.utc)
            + timedelta(
                seconds=RECONCILE_POLL_SECONDS
            )
        )

        await db.commit()

        return

    print(
        f"📡 RECONCILE RESPONSE | "
        f"job={job.id} | "
        f"http_status={response.status_code}",
        flush=True,
    )

    if response.status_code in (404, 400):

        print(
            f"❌ RECONCILIATION PERMANENT FAILURE | "
            f"job={job.id}",
            flush=True,
        )

        job.status = "failed"
        job.updated_at = datetime.now(timezone.utc)

        await db.commit()

        return

    if response.status_code != 200:

        print(
            f"⚠️ RECONCILIATION RETRY | "
            f"job={job.id} | "
            f"http_status={response.status_code}",
            flush=True,
        )

        job.next_attempt_at = (
            datetime.now(timezone.utc)
            + timedelta(
                seconds=RECONCILE_POLL_SECONDS
            )
        )

        await db.commit()

        return

    try:
        data = response.json()

    except ValueError:

        print(
            f"⚠️ INVALID RECONCILIATION JSON | "
            f"job={job.id}",
            flush=True,
        )

        job.next_attempt_at = (
            datetime.now(timezone.utc)
            + timedelta(
                seconds=RECONCILE_POLL_SECONDS
            )
        )

        await db.commit()

        return

    dm_status = data.get("status")

    print(
        f"📋 RECONCILIATION STATUS | "
        f"job={job.id} | "
        f"dm_id={job.dm_id} | "
        f"status={dm_status}",
        flush=True,
    )

    # --------------------------------------------------------
    # DELIVERED
    # --------------------------------------------------------

    if dm_status == "delivered":

        job.status = "delivered"
        job.updated_at = datetime.now(timezone.utc)

        await db.commit()

        print(
            f"✅ RECONCILIATION DELIVERED | "
            f"job={job.id}",
            flush=True,
        )

        return

    # --------------------------------------------------------
    # FAILED
    # --------------------------------------------------------

    if dm_status == "failed":

        print(
            f"❌ API REPORTS DM FAILED | "
            f"job={job.id}",
            flush=True,
        )

        await _schedule_retry_or_fail(
            db,
            job,
            "Failed during reconciliation",
        )

        return

    # --------------------------------------------------------
    # STILL QUEUED
    # --------------------------------------------------------

    print(
        f"⏳ DM STILL NOT DELIVERED | "
        f"job={job.id} | "
        f"status={dm_status}",
        flush=True,
    )

    job.next_attempt_at = (
        datetime.now(timezone.utc)
        + timedelta(
            seconds=RECONCILE_POLL_SECONDS
        )
    )

    await db.commit()


# ============================================================
# HANDLE NEW JOB
# ============================================================

async def _handle_new_job(
    db: AsyncSession,
    client: httpx.AsyncClient,
) -> bool:

    job = await fetch_and_lock_next_job(db)

    if job is None:
        return False

    try:

        await process_job(
            db,
            client,
            job,
        )

    except Exception:

        await db.rollback()

        logger.exception(
            "Unhandled error processing job %s",
            job.id,
        )

        print(
            f"💥 UNHANDLED PROCESS JOB ERROR | "
            f"job={job.id}",
            flush=True,
        )

        await _schedule_retry_or_fail(
            db,
            job,
            "unhandled error",
        )

    return True


# ============================================================
# HANDLE WAITING JOB
# ============================================================

async def _handle_waiting_job(
    db: AsyncSession,
    client: httpx.AsyncClient,
) -> bool:

    waiting_job = await get_waiting_job(db)

    if waiting_job is None:
        return False

    try:

        await reconcile_job(
            db,
            client,
            waiting_job,
        )

    except Exception:

        await db.rollback()

        logger.exception(
            "Unhandled error reconciling job %s",
            waiting_job.id,
        )

        print(
            f"💥 UNHANDLED RECONCILIATION ERROR | "
            f"job={waiting_job.id}",
            flush=True,
        )

        waiting_job.next_attempt_at = (
            datetime.now(timezone.utc)
            + timedelta(
                seconds=RECONCILE_POLL_SECONDS
            )
        )

        await db.commit()

    return True


# ============================================================
# MAIN WORKER
# ============================================================

async def run_worker():

    print(
        "🚀 ASYNC DM BACKGROUND WORKER STARTING...",
        flush=True,
    )

    logger.info(
        "Async DM background worker started."
    )

    check_waiting_first = False

    last_heartbeat = (
        asyncio.get_running_loop().time()
    )

    async with httpx.AsyncClient(
        timeout=10.0
    ) as client:

        print(
            "🌐 HTTP CLIENT CREATED",
            flush=True,
        )

        while True:

            # =================================================
            # HEARTBEAT
            # =================================================

            now = (
                asyncio.get_running_loop().time()
            )

            if now - last_heartbeat >= 10:

                print(
                    "💓 DM WORKER ALIVE | "
                    "worker loop is running",
                    flush=True,
                )

                last_heartbeat = now

            # =================================================
            # PROCESS JOBS
            # =================================================

            async with AsyncSessionLocal() as db:

                try:

                    check_waiting_first = (
                        not check_waiting_first
                    )

                    if check_waiting_first:

                        print(
                            "🔄 LOOP | checking WAITING first",
                            flush=True,
                        )

                        if (
                            await _handle_waiting_job(
                                db,
                                client,
                            )
                            or
                            await _handle_new_job(
                                db,
                                client,
                            )
                        ):
                            continue

                    else:

                        print(
                            "📦 LOOP | checking QUEUED first",
                            flush=True,
                        )

                        if (
                            await _handle_new_job(
                                db,
                                client,
                            )
                            or
                            await _handle_waiting_job(
                                db,
                                client,
                            )
                        ):
                            continue

                except Exception as e:

                    logger.exception(
                        "Error in worker loop"
                    )

                    print(
                        f"💥 WORKER LOOP ERROR | {e}",
                        flush=True,
                    )

            # =================================================
            # IDLE
            # =================================================

            print(
                "😴 WORKER IDLE | "
                "no eligible jobs, sleeping 1s",
                flush=True,
            )

            await asyncio.sleep(1)


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

    except Exception as e:

        print(
            f"💀 DM WORKER PROCESS CRASHED | {e}",
            flush=True,
        )

        logger.exception(
            "DM worker process crashed"
        )

        raise