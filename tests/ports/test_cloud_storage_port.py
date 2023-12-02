# tests/ports/test_cloud_storage_port.py
from abc import ABC
from unittest.mock import AsyncMock

import pandas as pd
import pytest

from pdcloud.ports.cloud_storage_port import CloudStoragePort


class MockCloudStoragePort(CloudStoragePort, ABC):
    async def read_data(self, container: str, data_object: str) -> pd.DataFrame:
        pass

    async def write_data(self, container: str, data_object: str, data: pd.DataFrame, overwrite: bool = False) -> None:
        pass

    def list_objects(self, container: str) -> list:
        pass


def test_cloud_storage_port_instantiation():
    try:
        mock_port = MockCloudStoragePort()
    except TypeError:
        pytest.fail("Instantiation of MockCloudStoragePort failed. The abstract methods may not be correctly defined.")


@pytest.mark.asyncio
async def test_cloud_storage_port_abstract_methods():
    mock_port = MockCloudStoragePort()

    # Testing if abstract methods can be called (although they do nothing)
    try:
        await mock_port.read_data("container", "data_object")
        await mock_port.write_data("container", "data_object", pd.DataFrame(), True)
        mock_port.list_objects("container")
    except Exception as e:
        pytest.fail(f"Abstract method call failed: {e}")
