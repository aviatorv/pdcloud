# tests/core/test_model.py
from unittest.mock import AsyncMock, patch

import pandas as pd
import pytest
from pandas.testing import assert_frame_equal

from pdcloud.core.model import Lib
from pdcloud.ports.cloud_storage_port import CloudStoragePort


class FakeStoragePort(CloudStoragePort):
    async def read_data(self, container: str, data_object: str) -> pd.DataFrame:
        # Return a dummy DataFrame for testing
        return pd.DataFrame({"data": [1, 2, 3]})

    async def write_data(self, container: str, data_object: str, data: pd.DataFrame, overwrite: bool = False) -> None:
        pass

    def list_objects(self, container: str) -> list:
        return ["file1", "file2"]


@pytest.fixture
def lib():
    # Fixture to create a Lib instance with a fake storage port
    storage = FakeStoragePort()
    return Lib(container="test_container", storage=storage)


@pytest.mark.asyncio
async def test_read_all(lib):
    # Test the read_all method
    actual_df = lib.read_all()
    assert isinstance(actual_df, pd.DataFrame)
    assert not actual_df.empty
    assert actual_df.shape == (6, 1)


@pytest.mark.asyncio
async def test_read_single_object(lib):
    # Test the read method for a single object
    data_object = "file1"
    expected_df = pd.DataFrame({"data": [1, 2, 3]})
    actual_df = lib.read(data_object)
    assert isinstance(actual_df, pd.DataFrame)
    assert not actual_df.empty
    assert_frame_equal(actual_df, expected_df)


def test_get_objects(lib):
    # Test the get_objects method
    objects = lib.get_objects()
    assert isinstance(objects, list)
    assert len(objects) == 2
    assert sorted(["file1", "file2"]) == sorted(objects)
