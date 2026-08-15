from datetime import datetime

from sqlalchemy import DateTime, String, func
from sqlalchemy.orm import Mapped, mapped_column

from app.db import Base


class Rule(Base):
    __tablename__ = "rules"

    id: Mapped[str] = mapped_column(String, primary_key=True)
    keyword: Mapped[str] = mapped_column(String, nullable=False)
    dm_message: Mapped[str] = mapped_column(String, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )