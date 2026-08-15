from datetime import datetime

from sqlalchemy import DateTime, String, func
from sqlalchemy.orm import Mapped, mapped_column

from app.db import Base


class Comment(Base):
    __tablename__ = "comments"

    comment_id: Mapped[str] = mapped_column(String, primary_key=True)
    user_id: Mapped[str | None] = mapped_column(String, nullable=True)
    post_id: Mapped[str | None] = mapped_column(String, nullable=True)
    text: Mapped[str | None] = mapped_column(String, nullable=True)

    state: Mapped[str] = mapped_column(
        String,
        nullable=False,
        default="active",
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