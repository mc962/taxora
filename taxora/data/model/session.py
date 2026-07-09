"""ORM model for browser UI login sessions."""

import datetime

from sqlalchemy import BigInteger, DateTime, ForeignKey, Identity, Integer, String
from sqlalchemy.orm import Mapped, mapped_column

from taxora.data.model.base import Base


class Session(Base):
    __tablename__ = "sessions"

    id: Mapped[int] = mapped_column(
        Integer().with_variant(BigInteger(), "postgresql"),
        Identity(),
        primary_key=True,
    )
    session_hash: Mapped[str] = mapped_column(
        String, unique=True, index=True, nullable=False
    )
    api_key_id: Mapped[int] = mapped_column(
        Integer().with_variant(BigInteger(), "postgresql"),
        ForeignKey("api_keys.id", ondelete="CASCADE"),
        nullable=False,
    )
    expires_at: Mapped[datetime.datetime] = mapped_column(DateTime, nullable=False)
