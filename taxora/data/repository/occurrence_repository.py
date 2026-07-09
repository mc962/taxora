"""Repository for GBIF occurrence records."""

import datetime
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from taxora.data.model.occurrence import Occurrence


def _record_to_row(record: dict[str, Any]) -> dict[str, Any]:
    event_date_raw = record.get("eventDate")
    event_date = None
    if event_date_raw:
        try:
            event_date = datetime.datetime.fromisoformat(
                event_date_raw.replace("Z", "+00:00")
            )
        except ValueError:
            event_date = None

    return {
        "gbif_key": record["key"],
        "scientific_name": record.get("scientificName"),
        "country_code": record.get("countryCode"),
        "decimal_lat": record.get("decimalLatitude"),
        "decimal_lon": record.get("decimalLongitude"),
        "event_date": event_date,
        "basis_of_record": record.get("basisOfRecord"),
        "raw": record,
    }


class OccurrenceRepository:
    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def upsert_batch(self, records: list[dict[str, Any]]) -> int:
        """Upsert a batch of raw GBIF occurrence records by gbif_key. Returns rows written."""
        if not records:
            return 0

        rows = [_record_to_row(record) for record in records]
        update_cols = [col for col in rows[0] if col != "gbif_key"]

        stmt = insert(Occurrence).values(rows)
        await self.session.execute(
            stmt.on_conflict_do_update(
                index_elements=["gbif_key"],
                set_={col: getattr(stmt.excluded, col) for col in update_cols},
            )
        )
        await self.session.commit()
        return len(rows)

    async def count(self, country_code: str | None = None) -> int:
        query = select(func.count(Occurrence.gbif_key))
        if country_code:
            query = query.where(Occurrence.country_code == country_code)
        result = await self.session.execute(query)
        return result.scalar_one()
