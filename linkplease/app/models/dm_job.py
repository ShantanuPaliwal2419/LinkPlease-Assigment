from datetime import datetime

from sqlalchemy import (
    BigInteger,
    DateTime,
    ForeignKey,
    Integer,
    String,
    UniqueConstraint,
    func,
)
from sqlalchemy.orm import Mapped, mapped_column

from app.db import Base


class DMJob(Base):
    __tablename__ = "dm_jobs"

    id: Mapped[int] = mapped_column(
        BigInteger,
        primary_key=True,
        autoincrement=True,
    )

    rule_id: Mapped[str] = mapped_column(
        String,
        ForeignKey("rules.id"),
        nullable=False,
    )

    comment_id: Mapped[str] = mapped_column(
        String,
        ForeignKey("comments.comment_id"),
        nullable=False,
    )

    user_id: Mapped[str] = mapped_column(
        String,
        nullable=False,
    )

    message: Mapped[str] = mapped_column(
        String,
        nullable=False,
    )

    status: Mapped[str] = mapped_column(
        String,
        nullable=False,
        default="queued",
    )

    attempts: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        default=0,
    )

    dm_id: Mapped[str | None] = mapped_column(
        String,
        nullable=True,
    )

    next_attempt_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )

    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
        onupdate=func.now(),
    )

    __table_args__ = (
        UniqueConstraint(
            "user_id",
            "rule_id",
            name="uq_dm_jobs_user_rule",
        ),
    )