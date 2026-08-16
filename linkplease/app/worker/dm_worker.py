import time
from datetime import datetime, timedelta, timezone

from sqlalchemy import select

from app.db import SessionLocal
from app.models.dm_job import DMJob
from app.services.dm_service import send_dm, get_dm_status


MAX_ATTEMPTS = 5


def get_next_job(db):
    return db.scalar(
        select(DMJob)
        .where(
            DMJob.status == "queued",
            DMJob.next_attempt_at <= datetime.now(timezone.utc),
        )
        .order_by(DMJob.id)
        .limit(1)
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
    )
def process_job(db, job: DMJob):
    job.status = "sending"
    job.attempts += 1
    db.commit()

    response = send_dm(
        recipient_user_id=job.user_id,
        message=job.message,
        comment_id=job.comment_id,
        idempotency_key=f"dm-job-{job.id}",
    )

    # ---------------------------------------------------------
    # 200 / 202 = inspect the response body
    # ---------------------------------------------------------

    if response.status_code in (200, 202):
        data = response.json()

        job.dm_id = data.get("dm_id")
        dm_status = data.get("status")

        if dm_status == "failed":
            job.status = "failed"
            db.commit()

            print(
                f"Job {job.id} failed. "
                f"dm_id={job.dm_id}"
            )
            return

        if dm_status == "queued":
            job.status = "waiting"
            db.commit()

            print(
                f"Job {job.id} accepted. "
                f"PseudoGram dm_id={job.dm_id}"
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

            print(
                f"Job {job.id} exhausted retries "
                f"after {job.attempts} attempts"
            )
            return

        retry_after = response.headers.get(
            "Retry-After",
            "10",
        )

        try:
            retry_seconds = int(retry_after)
        except ValueError:
            retry_seconds = 10

        job.status = "queued"
        job.next_attempt_at = (
            datetime.now(timezone.utc)
            + timedelta(seconds=retry_seconds)
        )

        db.commit()

        print(
            f"Job {job.id} rate limited. "
            f"Retrying in {retry_seconds}s"
        )
        return

    # ---------------------------------------------------------
    # 500+ = temporary failure
    # ---------------------------------------------------------

    if response.status_code >= 500:
        if job.attempts >= MAX_ATTEMPTS:
            job.status = "failed"
            db.commit()

            print(
                f"Job {job.id} exhausted retries "
                f"after {job.attempts} attempts"
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

        db.commit()

        print(
            f"Job {job.id} failed with "
            f"{response.status_code}. "
            f"Retrying in {retry_seconds}s"
        )
        return

    # ---------------------------------------------------------
    # Unexpected response
    # ---------------------------------------------------------

    job.status = "failed"
    db.commit()

    print(
        f"Job {job.id} failed with unexpected "
        f"status {response.status_code}"
    )


def reconcile_job(db, job: DMJob):
    response = get_dm_status(job.dm_id)

    # ---------------------------------------------------------
    # 404 = DM not found yet
    # ---------------------------------------------------------

    if response.status_code == 404:
      job.status = "failed"

    db.commit()

    print(
        f"Job {job.id} reconciliation failed: "
        f"DM {job.dm_id} not found."
    )

    
    return

    # ---------------------------------------------------------
    # Unexpected HTTP response
    # ---------------------------------------------------------

    if response.status_code != 200:
        print(
            f"Could not reconcile job {job.id}: "
            f"HTTP {response.status_code}"
        )
        return

    data = response.json()
    dm_status = data.get("status")

    # ---------------------------------------------------------
    # Delivered
    # ---------------------------------------------------------

    if dm_status == "delivered":
        job.status = "delivered"
        db.commit()

        print(
            f"Job {job.id} delivered. "
            f"dm_id={job.dm_id}"
        )

        return

    # ---------------------------------------------------------
    # Failed after being accepted
    # ---------------------------------------------------------

    if dm_status == "failed":
        if job.attempts >= MAX_ATTEMPTS:
            job.status = "failed"
            db.commit()

            print(
                f"Job {job.id} failed permanently "
                f"after reconciliation."
            )

            return

        job.status = "queued"
        job.next_attempt_at = (
            datetime.now(timezone.utc)
            + timedelta(seconds=2 ** job.attempts)
        )

        db.commit()

        print(
            f"Job {job.id} was accepted but later failed. "
            f"Retrying."
        )

        return

    # ---------------------------------------------------------
    # Still queued at PseudoGram
    # ---------------------------------------------------------

    if dm_status == "queued":
        job.next_attempt_at = (
            datetime.now(timezone.utc)
            + timedelta(seconds=10)
        )

        db.commit()

        print(
            f"Job {job.id} is still queued at PseudoGram. "
            f"Checking again in 10s."
        )

        return


def run_worker():
    print("DM worker started.")

    while True:
        db = SessionLocal()

        try:
            # First process new/retry jobs
            job = get_next_job(db)

            if job is not None:
                process_job(db, job)
                continue

            # Then reconcile accepted DMs
            waiting_job = get_waiting_job(db)

            if waiting_job is not None:
                reconcile_job(db, waiting_job)
                continue

            time.sleep(1)

        finally:
            db.close()


if __name__ == "__main__":
    run_worker()