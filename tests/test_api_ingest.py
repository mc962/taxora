from unittest.mock import AsyncMock, MagicMock, patch

from temporalio.client import WorkflowExecutionStatus

import taxora.temporal_client as temporal_client


async def test_start_ingest_calls_temporal_client(api_client):
    mock_client = MagicMock()
    mock_client.start_workflow = AsyncMock()

    with patch.object(temporal_client, "_client", mock_client):
        response = await api_client.post("/ingest", json={"country_code": "is"})

    assert response.status_code == 200
    body = response.json()
    assert body["workflow_id"].startswith("ingest-is-")
    mock_client.start_workflow.assert_awaited_once()
    _, args, kwargs = mock_client.start_workflow.mock_calls[0]
    params = args[1]
    assert params.country_code == "is"
    assert params.event_date_range is None


async def test_start_ingest_builds_event_date_range(api_client):
    mock_client = MagicMock()
    mock_client.start_workflow = AsyncMock()

    with patch.object(temporal_client, "_client", mock_client):
        response = await api_client.post(
            "/ingest",
            json={
                "country_code": "IS",
                "start_date": "2020-01-01",
                "end_date": "2020-12-31",
            },
        )

    assert response.status_code == 200
    _, args, kwargs = mock_client.start_workflow.mock_calls[0]
    params = args[1]
    assert params.event_date_range == "2020-01-01,2020-12-31"


async def test_get_status_completed(api_client):
    mock_client = MagicMock()
    mock_handle = MagicMock()

    mock_description = MagicMock()
    mock_description.status = WorkflowExecutionStatus.COMPLETED
    mock_handle.describe = AsyncMock(return_value=mock_description)
    mock_handle.result = AsyncMock(
        return_value={"country_code": "IS", "total": 5, "pages": 1}
    )
    mock_client.get_workflow_handle = MagicMock(return_value=mock_handle)

    with patch.object(temporal_client, "_client", mock_client):
        response = await api_client.get("/status/ingest-IS-abc123")

    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "COMPLETED"
    assert body["result"]["total"] == 5


async def test_records_count(api_client):
    response = await api_client.get("/records/count?country_code=IS")
    assert response.status_code == 200
    assert response.json() == {"country_code": "IS", "count": 0}
