import pandas as pd
from pdcloud import AzureStorage, Lib
import os
from pandas.testing import assert_frame_equal

# Constants
CONNECTION_STRING = os.environ["CONNECTION_STRING"]
CONTAINER_NAME = "testcontainer"
DATA_OBJECT_NAME = "testdataframe"


def test_azure_lib():
    azure_storage = AzureStorage(CONNECTION_STRING)
    lib = Lib(container=CONTAINER_NAME, storage=azure_storage)
    df = pd.DataFrame({"column1": [4, 5, 6], "column2": ["d", "e", "f"]})
    df1 = pd.DataFrame({"column1": [1, 2, 3], "column2": ["a", "b", "c"]})

    assert not lib.container_exists()
    lib.blob_exists(DATA_OBJECT_NAME)
    lib.write(DATA_OBJECT_NAME, data=df1)

    assert lib.container_exists()
    assert lib.blob_exists(DATA_OBJECT_NAME)

    df2 = lib.read_all()
    assert isinstance(df2, pd.DataFrame)
    assert_frame_equal(df1, df2)

    df3 = lib.read(DATA_OBJECT_NAME)
    assert isinstance(df3, pd.DataFrame)
    assert_frame_equal(df1, df3)

    lib.delete(DATA_OBJECT_NAME)
    assert not lib.blob_exists(DATA_OBJECT_NAME)

    lib.write("1", data=df)
    lib.write("2", data=df1)
    df_all = lib.read_all()
    expected = pd.concat([df, df1])
    assert_frame_equal(df_all, expected)

    assert lib.container_exists()
    assert lib.get_objects() == ["1", "2"]
    lib.delete_container()
    assert not lib.container_exists()
