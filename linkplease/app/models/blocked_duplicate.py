from datetime import datetime

from sqlalchemy import BigInteger, DateTime, String, func
from sqlalchemy.orm import Mapped, mapped_column

from app.db import Base


class BlockedDuplicateEvent(Base):
    __tablename__ = "blocked_duplicates"

    id: Mapped[int] = mapped_column(
        BigInteger,
        primary_key=True,
        autoincrement=True,
    )

    user_id: Mapped[str] = mapped_column(
        String,
        nullable=False,
    )

    rule_id: Mapped[str] = mapped_column(
        String,
        nullable=False,
    )

    comment_id: Mapped[str] = mapped_column(
        String,
        nullable=False,
    )

    blocked_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )