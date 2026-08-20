import re

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.blocked_duplicate import BlockedDuplicateEvent
from app.models.comment import Comment
from app.models.dm_job import DMJob
from app.models.event import Event
from app.models.rule import Rule


# ============================================================
# KEYWORD MATCHING
# ============================================================

def keyword_matches(keyword: str, text: str) -> bool:
    """
    Match keywords intelligently.

    Examples:
        PRICE   -> price
        PRICE   -> pricing
        PRICE   -> prices
        PRICE   -> priced

    Also supports normal substring matching for phrases.
    """

    if not keyword or not text:
        return False

    keyword = keyword.strip().lower()
    text = text.strip().lower()

    # --------------------------------------------------------
    # Direct match
    # --------------------------------------------------------

    if keyword in text:
        return True

    # --------------------------------------------------------
    # Word-based matching
    # --------------------------------------------------------

    words = re.findall(r"[a-zA-Z0-9']+", text)

    # Exact word
    if keyword in words:
        return True

    # --------------------------------------------------------
    # Handle words like:
    #
    # price -> pricing
    # price -> prices
    # price -> priced
    #
    # "price" itself doesn't appear literally inside
    # "pricing", so create a simple stem.
    # --------------------------------------------------------

    stem = keyword

    if keyword.endswith("e"):
        stem = keyword[:-1]

    for word in words:

        if word.startswith(stem):
            return True

    return False


# ============================================================
# WEBHOOK PROCESSOR
# ============================================================

async def process_webhook(
    event,
    db: AsyncSession,
) -> str:

    # ========================================================
    # 1. DEDUPLICATE WEBHOOK EVENT
    # ========================================================

    event_insert = insert(Event).values(
        event_id=event.event_id,
        event_type=event.event_type,
        comment_id=event.data.comment_id,
    )

    event_insert = event_insert.on_conflict_do_nothing(
        index_elements=[Event.event_id]
    )

    result = await db.execute(event_insert)

    # Event already processed
    if result.rowcount == 0:

        await db.commit()

        print(
            f"[DUPLICATE EVENT] "
            f"event_id={event.event_id}"
        )

        return "duplicate"

    # ========================================================
    # COMMENT ID
    # ========================================================

    comment_id = event.data.comment_id

    # ========================================================
    # 2. HANDLE comment.deleted
    # ========================================================

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
            f"[COMMENT DELETED] "
            f"comment_id={comment_id}"
        )

        return "deleted"

    # ========================================================
    # 3. IGNORE OTHER EVENT TYPES
    # ========================================================

    if event.event_type != "comment.created":

        await db.commit()

        print(
            f"[IGNORED EVENT] "
            f"type={event.event_type}"
        )

        return "ignored"

    # ========================================================
    # 4. VALIDATE USER + TEXT
    # ========================================================

    user = event.data.from_

    if user is None or not event.data.text:

        await db.commit()

        print(
            f"[IGNORED COMMENT] "
            f"comment_id={comment_id}"
        )

        return "ignored"

    user_id = user.user_id
    comment_text = event.data.text.strip()

    # ========================================================
    # 5. CREATE COMMENT
    # ========================================================

    comment_insert = insert(Comment).values(
        comment_id=comment_id,
        user_id=user_id,
        post_id=event.data.post_id,
        text=comment_text,
        state="active",
    )

    comment_insert = comment_insert.on_conflict_do_nothing(
        index_elements=[Comment.comment_id]
    )

    await db.execute(comment_insert)

    # ========================================================
    # 6. READ COMMENT STATE
    #
    # This protects against a comment that was already
    # tombstoned/deleted before another event arrived.
    # ========================================================

    comment = await db.scalar(
        select(Comment)
        .where(Comment.comment_id == comment_id)
    )

    if comment is None:

        await db.commit()

        print(
            f"[COMMENT NOT FOUND] "
            f"comment_id={comment_id}"
        )

        return "ignored"

    # ========================================================
    # 7. TOMBSTONE CHECK
    # ========================================================

    if comment.state == "deleted":

        await db.commit()

        print(
            f"[TOMBSTONE] "
            f"comment_id={comment_id}"
        )

        return "deleted"

    # ========================================================
    # 8. LOAD ALL RULES ONCE
    # ========================================================

    rules_result = await db.scalars(
        select(Rule)
    )

    rules = rules_result.all()

    # ========================================================
    # 9. MATCH RULES
    # ========================================================

    matched_rules = []

    for rule in rules:

        if keyword_matches(
            rule.keyword,
            comment_text,
        ):
            matched_rules.append(rule)

            print(
                f"[RULE MATCH] "
                f"user={user_id} "
                f"comment={comment_id} "
                f"keyword={rule.keyword} "
                f"text={comment_text}"
            )

    # ========================================================
    # NO RULE MATCH
    # ========================================================

    if not matched_rules:

        await db.commit()

        print(
            f"[NO MATCH] "
            f"user={user_id} "
            f"comment={comment_id} "
            f"text={comment_text}"
        )

        return "no_match"

    # ========================================================
    # 10. CREATE DM JOBS
    # ========================================================

    jobs_created = 0
    duplicates_blocked = 0

    for rule in matched_rules:

        job_insert = insert(DMJob).values(
            rule_id=rule.id,
            comment_id=comment_id,
            user_id=user_id,
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

        # ====================================================
        # NEW JOB
        # ====================================================

        if job_result.rowcount == 1:

            jobs_created += 1

            print(
                f"[DM QUEUED] "
                f"user={user_id} "
                f"rule={rule.id}"
            )

        # ====================================================
        # DUPLICATE JOB
        # ====================================================

        else:

            duplicates_blocked += 1

            duplicate_insert = (
                insert(BlockedDuplicateEvent)
                .values(
                    user_id=user_id,
                    rule_id=rule.id,
                    comment_id=comment_id,
                )
                .on_conflict_do_nothing()
            )

            await db.execute(duplicate_insert)

            print(
                f"[DM DUPLICATE BLOCKED] "
                f"user={user_id} "
                f"rule={rule.id}"
            )

    # ========================================================
    # 11. COMMIT ATOMICALLY
    # ========================================================

    await db.commit()

    # ========================================================
    # 12. FINAL LOG
    # ========================================================

    print(
        f"[WEBHOOK PROCESSED] "
        f"user={user_id} "
        f"comment={comment_id} "
        f"matched_rules={len(matched_rules)} "
        f"jobs_created={jobs_created} "
        f"duplicates_blocked={duplicates_blocked}"
    )

    return "processed"