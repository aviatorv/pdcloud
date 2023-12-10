from abc import ABC, abstractmethod
from typing import List, Optional, AsyncGenerator
import pandas as pd
from aiohttp import StreamReader


class CloudStoragePort(ABC):
    """
    Abstract base class for cloud storage operations. This class defines the interface
    for reading and writing data to a cloud storage service.
    """

    @abstractmethod
    async def read_data(self, container: str, data_object: str) -> pd.DataFrame:
        """
        Asynchronously read data from a specified container and data object (file) in the cloud storage.

        Args:
            container (str): The name of the cloud storage container.
            data_object (str): The name of the object (file) to read.

        Returns:
            pd.DataFrame: Data read from the cloud storage, returned as a DataFrame.
        """
        pass

    @abstractmethod
    async def write_data(
        self,
        container: str,
        data_object: str,
        data: pd.DataFrame,
        overwrite: bool = False,
    ) -> None:
        """
        Asynchronously write a Pandas DataFrame to a specified container and data object (file) in the cloud storage.

        Args:
            container (str): The name of the cloud storage container.
            data_object (str): The name of the object (file) to write.
            data (pd.DataFrame): The DataFrame to write to the cloud storage.
            overwrite (bool, optional): Flag to overwrite existing data. Defaults to False.
        """
        pass

    @abstractmethod
    async def get_blob_stream(self, container: str, data_object: str) -> StreamReader:
        """
        Asynchronously gets the stream of a blob from cloud storage.

        Args:
            container (str): The name of the cloud storage container.
            data_object (str): The name of the blob to get the stream for.

        Returns:
            StreamReader: The stream of the blob.
        """
        pass

    @abstractmethod
    async def get_all_blob_streams(
        self, container: str
    ) -> AsyncGenerator[StreamReader, None]:
        """
        Asynchronously retrieves streams for all blobs within a specified container.

        Args:
            container (str): The name of the cloud storage container from which to retrieve blob streams.

        Yields:
            AsyncGenerator[StreamReader, None]: An asynchronous generator that yields stream readers for each blob.
        """
        pass

    @abstractmethod
    def list_objects(
        self, container: str, regex_pattern: Optional[str] = None
    ) -> List[str]:
        """
        Lists all objects (blobs) in a specified container, optionally filtered by a regex pattern.

        Args:
            container (str): The name of the container from which to list objects.
            regex_pattern (str, optional): The regex pattern to filter object names. If None, all objects are listed. Defaults to None.

        Returns:
            List[str]: A list of object names in the specified container that match the regex pattern, if provided; otherwise, all object names.
        """
        pass

    @abstractmethod
    async def list_all_containers(self) -> List[str]:
        """
        Asynchronously lists all containers in the cloud storage service.

        Returns:
            List[str]: A list of all container names in the cloud storage service.
        """
        pass

    @abstractmethod
    async def delete_blob(self, container: str, data_object: str) -> None:
        """
        Asynchronously deletes a blob from a specified container in cloud storage.

        Args:
            container (str): The name of the cloud storage container.
            data_object (str): The name of the blob to delete.
        """
        pass

    @abstractmethod
    async def delete_container(self, container: str) -> None:
        """
        Asynchronously deletes an entire container from cloud storage.

        Args:
            container (str): The name of the container to delete.
        """
        pass

    @abstractmethod
    def blob_exists(self, container: str, data_object: str) -> bool:
        """
        Checks if a specified blob exists in a given container in cloud storage.

        Args:
            container (str): The name of the cloud storage container.
            data_object (str): The name of the blob to check for existence.

        Returns:
            bool: True if the blob exists, False otherwise.
        """
        pass

    @abstractmethod
    async def container_exists(self, container: str) -> bool:
        """
        Checks if a specified container exists in cloud storage.

        Args:
            container (str): The name of the cloud storage container.

        Returns:
            bool: True if the container exists, False otherwise.
        """
        pass
