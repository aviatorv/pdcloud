import asyncio
from dataclasses import dataclass
from io import BytesIO
from typing import List, Optional

import nest_asyncio
import pandas as pd

from pdcloud.ports.cloud_storage_port import CloudStoragePort

nest_asyncio.apply()


@dataclass
class Lib:
    """
    A library class for reading data from cloud storage and processing it.

    Attributes:
        storage (CloudStoragePort): The storage adapter instance.
        container (str): The name of the storage container.
    """

    container: str
    storage: CloudStoragePort

    async def _fetch_all(self) -> List[pd.DataFrame]:
        """
        Asynchronously fetches and processes data from all objects in a specified container.

        Returns:
            List[pd.DayaFrame]: A list of processed data as Pandas DataFrames.
        """
        tasks = []
        for data_object in self.get_objects():
            try:
                tasks.append(asyncio.ensure_future(self.storage.read_data(self.container, data_object)))
            except Exception as e:
                print(f"Error fetching {data_object}: {e}")
        return await asyncio.gather(*tasks)

    async def _fetch_data_object(self, data_object: str) -> Optional[pd.DataFrame]:
        """
        Asynchronously fetches and processes data from a specific object in a container.

        Args:
            container (str): The name of the container.
            data_object (str): The specific data object to fetch.

        Returns:
            Optional[pd.DataFrame]: The processed data as a Pandas DataFrame, or None if an error occurs.
        """
        try:
            return await asyncio.ensure_future(self.storage.read_data(self.container, data_object))
        except Exception as e:
            print(f"Error fetching {data_object}: {e}")
            return None

    def read_all(self) -> pd.DataFrame:
        """
        Reads and processes all data objects from a specified container.

        Args:
            container (str): The name of the container to read from.

        Returns:
            pd.DataFrame: Concatenates all data obkjects to a Dataframe.
        """
        return pd.concat(asyncio.run(self._fetch_all()))

    def read(self, data_object: str) -> pd.DataFrame:
        """
        Reads and processes a specific data object from a container.

        Args:
            data_object (str): The specific data object to read.

        Returns:
            pd.DataFrame: The processed data object, or None if an error occurs.
        """
        return asyncio.run(self._fetch_data_object(data_object))

    def write(self, data_object: str, data: pd.DataFrame, container: str = None, overwrite: bool = False):
        container = container or self.container
        return asyncio.run(asyncio.ensure_future(self.storage.write_data(container, data_object, data, overwrite)))

    def get_objects(self) -> List[str]:
        """
        Retrieves a list of all data object names in the container.

        Returns:
            List[str]: A list of data object names.
        """
        return self.storage.list_objects(self.container)
