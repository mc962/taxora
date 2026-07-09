"""ORM model for API keys."""

import datetime

from sqlalchemy import BigInteger, Boolean, DateTime, Identity, Integer, String
from sqlalchemy.orm import Mapped, mapped_column

from taxora.data.model.base import Base


class ApiKey(Base):
    __tablename__ = "api_keys"

    id: Mapped[int] = mapped_column(
        Integer().with_variant(BigInteger(), "postgresql"),
        Identity(),
        primary_key=True,
    )
    name: Mapped[str] = mapped_column(String, nullable=False)
    key_prefix: Mapped[str] = mapped_column(String(8), index=True, nullable=False)
    key_hash: Mapped[str] = mapped_column(String, nullable=False)
    is_superuser: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    last_used_at: Mapped[datetime.datetime | None] = mapped_column(
        DateTime, nullable=True
    )
