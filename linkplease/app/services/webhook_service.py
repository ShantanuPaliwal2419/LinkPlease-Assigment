from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session

from app.models.comment import Comment
from app.models.dm_job import DMJob
from app.models.event import Event
from app.models.rule import Rule


def process_webhook(event, db: Session) -> str:
    # ---------------------------------------------------------
    # 1. Deduplicate the webhook event
    # ---------------------------------------------------------

    event_insert = insert(Event).values(
        event_id=event.event_id,
        event_type=event.event_type,
        comment_id=event.data.comment_id,
    )

    event_insert = event_insert.on_conflict_do_nothing(
        index_elements=[Event.event_id]
    )

    result = db.execute(event_insert)

    if result.rowcount == 0:
        return "duplicate"

    # ---------------------------------------------------------
    # 2. Part A only handles comment.created
    # ---------------------------------------------------------

    if event.event_type != "comment.created":
        db.commit()
        return "ignored"

    user = event.data.from_

    if user is None or event.data.text is None:
        db.commit()
        return "ignored"

    comment_id = event.data.comment_id

    # ---------------------------------------------------------
    # 3. Store the comment
    # ---------------------------------------------------------

    comment_insert = insert(Comment).values(
        comment_id=comment_id,
        user_id=user.user_id,
        post_id=event.data.post_id,
        text=event.data.text,
        state="active",
    )

    comment_insert = comment_insert.on_conflict_do_nothing(
        index_elements=[Comment.comment_id]
    )

    db.execute(comment_insert)

    # ---------------------------------------------------------
    # 4. Find matching rules
    # ---------------------------------------------------------

    rules = db.scalars(select(Rule)).all()

    comment_text = event.data.text.lower()

    for rule in rules:

        if rule.keyword.lower() not in comment_text:
            continue

        # -----------------------------------------------------
        # 5. Create DM job
        #
        # UNIQUE(user_id, rule_id) prevents duplicate DMs.
        # -----------------------------------------------------

        job_insert = insert(DMJob).values(
            rule_id=rule.id,
            comment_id=comment_id,
            user_id=user.user_id,
            message=rule.dm_message,
            status="queued",
        )

        job_insert = job_insert.on_conflict_do_nothing(
            index_elements=[
                DMJob.user_id,
                DMJob.rule_id,
            ]
        )

        job_result = db.execute(job_insert)

        if job_result.rowcount == 0:
            print(
                f"Duplicate blocked: "
                f"user={user.user_id}, rule={rule.id}"
            )

    # ---------------------------------------------------------
    # 6. Commit everything atomically
    # ---------------------------------------------------------

    db.commit()

    return "processed"