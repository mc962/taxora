from taxora.data.repository.occurrence_repository import _record_to_row

RECORD = {
    "key": 1,
    "scientificName": "Lagopus muta",
    "countryCode": "IS",
    "decimalLatitude": 64.1,
    "decimalLongitude": -21.6,
    "eventDate": "2026-01-04T00:00:00Z",
    "basisOfRecord": "HUMAN_OBSERVATION",
}


def test_record_to_row_maps_fields():
    row = _record_to_row(RECORD)
    assert row["gbif_key"] == 1
    assert row["scientific_name"] == "Lagopus muta"
    assert row["country_code"] == "IS"
    assert row["decimal_lat"] == 64.1
    assert row["decimal_lon"] == -21.6
    assert row["event_date"].isoformat() == "2026-01-04T00:00:00+00:00"
    assert row["basis_of_record"] == "HUMAN_OBSERVATION"
    assert row["raw"] == RECORD


def test_record_to_row_handles_missing_event_date():
    record = dict(RECORD, eventDate=None)
    row = _record_to_row(record)
    assert row["event_date"] is None


def test_record_to_row_handles_malformed_event_date():
    record = dict(RECORD, eventDate="not-a-date")
    row = _record_to_row(record)
    assert row["event_date"] is None
