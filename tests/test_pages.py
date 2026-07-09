async def test_index_page_renders(api_client):
    response = await api_client.get("/")
    assert response.status_code == 200
    assert "Load GBIF occurrence data" in response.text
    assert "ingest-form" in response.text
