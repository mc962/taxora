import datetime

from sqlalchemy import BigInteger, DateTime, Float, String, Text
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.types import JSON

from taxora.data.model.base import Base


class Occurrence(Base):
    __tablename__ = "occurrences"

    gbif_key: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    scientific_name: Mapped[str | None] = mapped_column(Text)
    country_code: Mapped[str | None] = mapped_column(String(2), index=True)
    decimal_lat: Mapped[float | None] = mapped_column(Float)
    decimal_lon: Mapped[float | None] = mapped_column(Float)
    event_date: Mapped[datetime.datetime | None] = mapped_column(
        DateTime(timezone=True)
    )
    basis_of_record: Mapped[str | None] = mapped_column(Text)
    raw: Mapped[dict] = mapped_column(JSON, nullable=False)
