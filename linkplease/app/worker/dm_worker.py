import time
from datetime import datetime, timedelta, timezone

from sqlalchemy import select

from app.db import SessionLocal
from app.models.dm_job import DMJob
from app.services.dm_service import send_dm


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
    # 202 = PseudoGram accepted the DM
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
    # 500 = temporary failure
    # ---------------------------------------------------------

    if response.status_code >= 500:
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
            f"Job {job.id} failed with {response.status_code}. "
            f"Retrying in {retry_seconds}s"
        )

        return

    # ---------------------------------------------------------
    # Anything unexpected
    # ---------------------------------------------------------

    job.status = "failed"

    db.commit()

    print(
        f"Job {job.id} failed with unexpected "
        f"status {response.status_code}"
    )


def run_worker():
    print("DM worker started.")

    while True:
        db = SessionLocal()

        try:
            job = get_next_job(db)

            if job is None:
                time.sleep(1)
                continue

            process_job(db, job)

        finally:
            db.close()


if __name__ == "__main__":
    run_worker()