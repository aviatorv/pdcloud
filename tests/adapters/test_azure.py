# tests/adapters/test_azure.py
from unittest.mock import AsyncMock, MagicMock, patch

import pandas as pd
import pytest
from azure.storage.blob.aio import BlobServiceClient

from pdcloud.adapters.azure import AzureStorageAdapter


@pytest.fixture
def mock_blob_service_client():
    client = AsyncMock(spec=BlobServiceClient)
    return client


@pytest.fixture
def azure_adapter(mock_blob_service_client):
    with patch(
        "azure.storage.blob.aio.BlobServiceClient.from_connection_string",
        return_value=mock_blob_service_client,
    ):
        return AzureStorageAdapter("dummy_connection_string")


@pytest.mark.asyncio
async def test_read_data(azure_adapter, mock_blob_service_client):
    # Mocking BlobServiceClient's methods
    mock_blob_client = AsyncMock()
    mock_container_client = MagicMock()
    mock_blob_service_client.get_container_client.return_value = mock_container_client
    mock_container_client.get_blob_client.return_value = mock_blob_client

    # Mocking the download blob method
    mock_stream = AsyncMock()
    mock_blob_client.download_blob.return_value = mock_stream
    dummy_data = pd.DataFrame({"test": [1, 2, 3]}).to_parquet(index=False)
    mock_stream.readall.return_value = dummy_data

    # Act
    result = await azure_adapter.read_data("dummy_container", "dummy_blob")

    # Assert
    assert isinstance(result, pd.DataFrame)
    assert not result.empty
    assert len(result) == 3
    assert "test" in result.columns
