from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.blocked_duplicate import BlockedDuplicateEvent
from app.models.comment import Comment
from app.models.dm_job import DMJob
from app.models.event import Event
from app.models.rule import Rule


async def process_webhook(event, db: AsyncSession) -> str:

    # ---------------------------------------------------------
    # 1. Deduplicate webhook event
    # ---------------------------------------------------------

    event_insert = insert(Event).values(
        event_id=event.event_id,
        event_type=event.event_type,
        comment_id=event.data.comment_id,
    )

    event_insert = event_insert.on_conflict_do_nothing(
        index_elements=[Event.event_id]
    )

    result = await db.execute(event_insert)

    if result.rowcount == 0:
        await db.commit()

        print(
            f"Duplicate webhook blocked: "
            f"event_id={event.event_id}"
        )

        return "duplicate"

    comment_id = event.data.comment_id

    # ---------------------------------------------------------
    # 2. Handle comment.deleted
    # ---------------------------------------------------------

    if event.event_type == "comment.deleted":

        delete_insert = insert(Comment).values(
            comment_id=comment_id,
            state="deleted",
        )

        delete_insert = delete_insert.on_conflict_do_update(
            index_elements=[Comment.comment_id],
            set_={
                "state": "deleted",
            },
        )

        await db.execute(delete_insert)
        await db.commit()

        print(
            f"Comment {comment_id} marked as deleted."
        )

        return "deleted"

    # ---------------------------------------------------------
    # 3. Ignore events other than comment.created
    # ---------------------------------------------------------

    if event.event_type != "comment.created":
        await db.commit()
        return "ignored"

    user = event.data.from_

    if user is None or event.data.text is None:
        await db.commit()
        return "ignored"

    # ---------------------------------------------------------
    # 4. Create comment if it doesn't exist
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

    await db.execute(comment_insert)

    # ---------------------------------------------------------
    # 5. Read actual comment state
    # ---------------------------------------------------------

    comment = await db.scalar(
        select(Comment).where(
            Comment.comment_id == comment_id
        )
    )

    # ---------------------------------------------------------
    # 6. Tombstone check
    # ---------------------------------------------------------

    if comment is None:
        await db.commit()
        return "ignored"

    if comment.state == "deleted":
        await db.commit()

        print(
            f"Comment {comment_id} was already deleted. "
            f"Skipping DM."
        )

        return "deleted"

    # ---------------------------------------------------------
    # 7. Find matching rules
    # ---------------------------------------------------------

    rules_result = await db.scalars(select(Rule))
    rules = rules_result.all()

    comment_text = event.data.text.lower()

    for rule in rules:

        if rule.keyword.lower() not in comment_text:
            continue

        # -----------------------------------------------------
        # 8. Create DM job
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

        job_result = await db.execute(job_insert)

        # -----------------------------------------------------
        # 9. Record duplicate DM attempt
        # -----------------------------------------------------

        if job_result.rowcount == 0:

            await db.execute(
                insert(BlockedDuplicateEvent).values(
                    user_id=user.user_id,
                    rule_id=rule.id,
                    comment_id=comment_id,
                )
            )

            print(
                f"Duplicate DM blocked: "
                f"user={user.user_id}, "
                f"rule={rule.id}"
            )

    # ---------------------------------------------------------
    # 10. Commit everything atomically
    # ---------------------------------------------------------

    await db.commit()

    return "processed"