import logging
import time
from datetime import datetime, timedelta, timezone

from sqlalchemy import select

from app.db import SessionLocal
from app.models.dm_job import DMJob
from app.services.dm_service import send_dm, get_dm_status


logger = logging.getLogger(__name__)

MAX_ATTEMPTS = 5
RECONCILE_POLL_SECONDS = 10
FIRST_RECONCILE_DELAY_SECONDS = 3


def get_next_job(db):
    return db.scalar(
        select(DMJob)
        .where(
            DMJob.status == "queued",
            DMJob.next_attempt_at <= datetime.now(timezone.utc),
        )
        .order_by(DMJob.id)
        .limit(1)
        # Row-level lock so two worker processes can't grab the same job.
        # Note: skip_locked requires Postgres/MySQL — drop it if you're on SQLite.
        .with_for_update(skip_locked=True)
    )


def get_waiting_job(db):
    return db.scalar(
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


def _schedule_retry_or_fail(db, job, reason):
    """Shared backoff/retry logic for transient failures: 5xx, network
    errors, bad payloads. Keeps the exhausted-retries behavior in one place
    instead of duplicating it at every call site."""
    if job.attempts >= MAX_ATTEMPTS:
        job.status = "failed"
        db.commit()
        logger.warning(
            "Job %s exhausted retries after %s attempts (%s)",
            job.id, job.attempts, reason,
        )
        return

    retry_seconds = min(2 ** job.attempts, 60)
    job.status = "queued"
    job.next_attempt_at = datetime.now(timezone.utc) + timedelta(seconds=retry_seconds)
    db.commit()

    logger.info(
        "Job %s hit a transient failure (%s). Retrying in %ss",
        job.id, reason, retry_seconds,
    )


def process_job(db, job: DMJob):
    job.status = "sending"
    job.attempts += 1
    db.commit()

    try:
        response = send_dm(
            recipient_user_id=job.user_id,
            message=job.message,
            comment_id=job.comment_id,
            idempotency_key=f"dm-job-{job.id}",
        )
    except Exception:
        # Network error / timeout — job was left in "sending" above, so we
        # MUST resolve it here or it's stuck forever.
        logger.exception("Job %s: send_dm raised an exception", job.id)
        _schedule_retry_or_fail(db, job, "send_dm exception")
        return

    # ---------------------------------------------------------
    # 200 / 202 = inspect the response body
    # ---------------------------------------------------------

    if response.status_code in (200, 202):
        try:
            data = response.json()
        except ValueError:
            logger.exception("Job %s: could not parse send_dm response as JSON", job.id)
            _schedule_retry_or_fail(db, job, "invalid JSON in send_dm response")
            return

        job.dm_id = data.get("dm_id")
        dm_status = data.get("status")

        if dm_status == "failed":
            job.status = "failed"
            db.commit()

            print(f"Job {job.id} failed. dm_id={job.dm_id}")
            return

        if dm_status == "queued":
            if not job.dm_id:
                # Can't ever reconcile a DM with no id — retry the send
                # instead of parking it in "waiting" where it would be
                # invisible to get_waiting_job forever.
                _schedule_retry_or_fail(db, job, "queued response missing dm_id")
                return

            job.status = "waiting"
            # Give PseudoGram a moment before the first status check instead
            # of reconciling immediately against the still-stale next_attempt_at.
            job.next_attempt_at = (
                datetime.now(timezone.utc)
                + timedelta(seconds=FIRST_RECONCILE_DELAY_SECONDS)
            )
            db.commit()

            print(f"Job {job.id} accepted. PseudoGram dm_id={job.dm_id}")
            return

        if dm_status in ("delivered", "sent"):
            job.status = "delivered"
            db.commit()

            print(f"Job {job.id} delivered immediately. dm_id={job.dm_id}")
            return

        # Any other/unknown dm_status. Do NOT let this fall through to the
        # "unexpected HTTP status" branch below — that branch is for
        # unexpected status *codes*, and would misreport 200/202 as the
        # failure reason. Fail explicitly with the real cause instead.
        job.status = "failed"
        db.commit()

        logger.error(
            "Job %s got HTTP %s with unrecognized dm_status=%r",
            job.id, response.status_code, dm_status,
        )
        return

    # ---------------------------------------------------------
    # 400 = permanent failure
    # ---------------------------------------------------------

    if response.status_code == 400:
        job.status = "failed"
        db.commit()

        print(f"Job {job.id} permanently failed: 400")
        return

    # ---------------------------------------------------------
    # 429 = rate limited
    # ---------------------------------------------------------

    if response.status_code == 429:
        if job.attempts >= MAX_ATTEMPTS:
            job.status = "failed"
            db.commit()

            print(f"Job {job.id} exhausted retries after {job.attempts} attempts")
            return

        retry_after = response.headers.get("Retry-After", "10")

        try:
            retry_seconds = int(retry_after)
        except ValueError:
            retry_seconds = 10

        job.status = "queued"
        job.next_attempt_at = datetime.now(timezone.utc) + timedelta(seconds=retry_seconds)
        db.commit()

        print(f"Job {job.id} rate limited. Retrying in {retry_seconds}s")
        return

    # ---------------------------------------------------------
    # 500+ = temporary failure
    # ---------------------------------------------------------

    if response.status_code >= 500:
        _schedule_retry_or_fail(db, job, f"HTTP {response.status_code}")
        return

    # ---------------------------------------------------------
    # Unexpected response
    # ---------------------------------------------------------

    job.status = "failed"
    db.commit()

    print(f"Job {job.id} failed with unexpected status {response.status_code}")


def reconcile_job(db, job: DMJob):
    try:
        response = get_dm_status(job.dm_id)
    except Exception:
        logger.exception("Job %s: get_dm_status raised an exception", job.id)
        job.next_attempt_at = datetime.now(timezone.utc) + timedelta(seconds=RECONCILE_POLL_SECONDS)
        db.commit()
        return

    # ---------------------------------------------------------
    # 404 = DM not found
    # ---------------------------------------------------------

    if response.status_code == 404:
        job.status = "failed"
        db.commit()

        print(f"Job {job.id} reconciliation failed: DM {job.dm_id} not found.")
        return

    # ---------------------------------------------------------
    # Unexpected HTTP response
    # ---------------------------------------------------------

    if response.status_code != 200:
        job.next_attempt_at = datetime.now(timezone.utc) + timedelta(seconds=RECONCILE_POLL_SECONDS)
        db.commit()

        print(f"Could not reconcile job {job.id}: HTTP {response.status_code}")
        return

    try:
        data = response.json()
    except ValueError:
        logger.exception("Job %s: could not parse reconciliation response as JSON", job.id)
        job.next_attempt_at = datetime.now(timezone.utc) + timedelta(seconds=RECONCILE_POLL_SECONDS)
        db.commit()
        return

    dm_status = data.get("status")

    # ---------------------------------------------------------
    # Delivered
    # ---------------------------------------------------------

    if dm_status == "delivered":
        job.status = "delivered"
        db.commit()

        print(f"Job {job.id} delivered. dm_id={job.dm_id}")
        return

    # ---------------------------------------------------------
    # Failed after being accepted
    # ---------------------------------------------------------

    if dm_status == "failed":
        if job.attempts >= MAX_ATTEMPTS:
            job.status = "failed"
            db.commit()

            print(f"Job {job.id} failed permanently after reconciliation.")
            return

        job.status = "queued"
        job.next_attempt_at = datetime.now(timezone.utc) + timedelta(seconds=2 ** job.attempts)
        db.commit()

        print(f"Job {job.id} was accepted but later failed. Retrying.")
        return

    # ---------------------------------------------------------
    # Still queued at PseudoGram
    # ---------------------------------------------------------

    if dm_status == "queued":
        job.next_attempt_at = datetime.now(timezone.utc) + timedelta(seconds=RECONCILE_POLL_SECONDS)
        db.commit()

        print(f"Job {job.id} is still queued at PseudoGram. Checking again in {RECONCILE_POLL_SECONDS}s.")
        return

    # ---------------------------------------------------------
    # Unrecognized dm_status
    # ---------------------------------------------------------

    logger.error("Job %s: unrecognized dm_status=%r during reconciliation", job.id, dm_status)
    job.next_attempt_at = datetime.now(timezone.utc) + timedelta(seconds=RECONCILE_POLL_SECONDS)
    db.commit()


def _handle_new_job(db):
    """Check for a queued/retry-ready job and process it. Returns True if a
    job was found (caller should `continue` the loop), False otherwise."""
    job = get_next_job(db)

    if job is None:
        return False

    try:
        process_job(db, job)
    except Exception:
        # Last-resort safety net: an unhandled exception here must never
        # crash the worker loop or leave the job in "sending".
        db.rollback()
        logger.exception("Unhandled error processing job %s", job.id)
        _schedule_retry_or_fail(db, job, "unhandled exception in process_job")

    return True


def _handle_waiting_job(db):
    """Check for a waiting job ready to reconcile and reconcile it. Returns
    True if a job was found (caller should `continue` the loop), False
    otherwise."""
    waiting_job = get_waiting_job(db)

    if waiting_job is None:
        return False

    try:
        reconcile_job(db, waiting_job)
    except Exception:
        db.rollback()
        logger.exception("Unhandled error reconciling job %s", waiting_job.id)
        waiting_job.next_attempt_at = (
            datetime.now(timezone.utc) + timedelta(seconds=RECONCILE_POLL_SECONDS)
        )
        db.commit()

    return True


def run_worker():
    print("DM worker started.")

    # Alternate which of the two checks goes first each iteration. A fixed
    # order (always sends-first or always reconcile-first) lets a sustained
    # burst on one side starve the other completely, since a hit on the
    # first check always `continue`s straight back to itself.
    check_waiting_first = False

    while True:
        db = SessionLocal()

        try:
            check_waiting_first = not check_waiting_first
            checks = (
                (_handle_waiting_job, _handle_new_job)
                if check_waiting_first
                else (_handle_new_job, _handle_waiting_job)
            )

            if checks[0](db):
                continue
            if checks[1](db):
                continue

            time.sleep(1)

        finally:
            db.close()


if __name__ == "__main__":
    run_worker()