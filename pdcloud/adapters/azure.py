import logging
import re
from io import BytesIO
from typing import List, Optional
from aiohttp import StreamReader
from typing import AsyncGenerator

import pandas as pd
from azure.storage.blob import ContainerClient
from azure.storage.blob.aio import BlobServiceClient

from pdcloud.ports.cloud_storage_port import CloudStoragePort
from azure.core.exceptions import ResourceNotFoundError


class AzureStorage(CloudStoragePort):
    """
    Adapter for Azure Blob Storage implementing the CloudStoragePort interface.

    Attributes:
        connection_string (str): Connection string for Azure Blob Storage.
        overwrite (bool): Flag to overwrite existing blobs. Defaults to True.
        parquet_engine (str): Engine for reading/writing Parquet files. Defaults to 'pyarrow'.
    """

    def __init__(self, connection_string: str, parquet_engine: str = "pyarrow"):
        """
        Initializes the AzureStorage with the given settings.
        """
        self.connection_string = connection_string
        self.blob_service_client = BlobServiceClient.from_connection_string(
            self.connection_string
        )
        self.parquet_engine = parquet_engine
        logging.basicConfig(level=logging.INFO)

    async def get_blob_stream(self, container: str, data_object: str) -> StreamReader:
        """
        Asynchronously gets the stream of a blob from Azure Blob Storage.

        Args:
            container (str): The name of the Azure storage container.
            data_object (str): The name of the blob to get the stream for.

        Returns:
            StreamReader: The stream of the blob.
        """
        container_client = self.blob_service_client.get_container_client(container)
        blob_client = container_client.get_blob_client(data_object)
        stream = await blob_client.download_blob()
        return stream

    async def get_all_blob_streams(
        self, container: str
    ) -> AsyncGenerator[StreamReader, None]:
        """
        Asynchronously retrieves streams for all blobs within a specified container.

        This method lists all blobs in the given container and then asynchronously fetches
        each blob's stream, yielding each stream as it's retrieved.

        Args:
            container (str): The name of the Azure storage container from which to retrieve blob streams.

        Yields:
            StreamReader: A stream reader corresponding to a blob in the container.
        """
        container_client = self.blob_service_client.get_container_client(container)
        blob_list = await container_client.list_blobs()
        for blob in blob_list:
            yield await self.get_blob_stream(container, blob.name)

    async def read_data(self, container: str, data_object: str) -> pd.DataFrame:
        """
        Asynchronously reads data from Azure Blob Storage and returns it as a Pandas DataFrame.

        Args:
            container (str): The name of the Azure storage container.
            data_object (str): The name of the blob to read.

        Returns:
            pd.DataFrame: The data read from the blob, returned as a DataFrame.
        """
        stream = await self.get_blob_stream(container, data_object)
        data = await stream.readall()
        return pd.read_parquet(BytesIO(data), engine=self.parquet_engine)

    async def _prepare_container_and_blob_client(
        self, container: str, data_object: str, overwrite: bool
    ) -> Optional[BlobServiceClient]:
        """
        Prepares the container and blob client for writing. It creates the container
        if it does not exist and checks if the blob exists based on the overwrite flag.

        Args:
            container (str): The name of the container.
            data_object (str): The name of the data object (blob).
            overwrite (bool): Flag to overwrite existing blob.

        Returns:
            BlobClient: The blob client if ready to write, else None.
        """
        container_client = self.blob_service_client.get_container_client(container)
        if not await container_client.exists():
            await container_client.create_container()

        blob_client = container_client.get_blob_client(data_object)
        if await blob_client.exists() and not overwrite:
            logging.info(
                f"Blob '{data_object}' in container '{container}' already exists and overwrite is not allowed."
            )
            return None
        return blob_client

    async def write_stream(
        self,
        stream: StreamReader,
        target_container: str,
        target_data_object: str,
        overwrite: bool = False,
    ) -> None:
        """
        Asynchronously writes a stream to a specified container and data object (blob) in Azure Blob Storage.

        If the target container does not exist, it creates a new container.

        Args:
            stream: The stream to write to the cloud storage.
            target_container (str): The name of the target Azure Blob Storage container.
            target_data_object (str): The name of the target blob (file) where the data is to be written.
            overwrite (bool, optional): Flag to overwrite existing blob. Defaults to False.
        """
        blob_client = await self._prepare_container_and_blob_client(
            target_container, target_data_object, overwrite
        )
        if blob_client:
            await blob_client.upload_blob(stream, overwrite=overwrite)
            logging.info(
                f"Written stream to Blob '{target_data_object}' in container '{target_container}'"
            )

    async def write_data(
        self,
        container: str,
        data_object: str,
        data: pd.DataFrame,
        overwrite: bool = False,
    ) -> None:
        """
        Asynchronously writes a Pandas DataFrame to a specified container and data object (blob) in Azure Blob Storage.

        This method first checks if the specified container exists, creating it if necessary. It then checks if the specified
        blob exists in the container. If the blob exists and overwriting is not allowed, the method logs an info message and
        returns without writing the data. If overwriting is allowed, or if the blob does not exist, the method writes the
        DataFrame to the blob.

        Args:
            container (str): The name of the Azure Blob Storage container.
            data_object (str): The name of the blob (file) where the data is to be written.
            data (pd.DataFrame): The Pandas DataFrame to write to the blob.
            overwrite (bool): If True, will overwrite the blob if it exists. Defaults to False.

        Raises:
            Exception: If any error occurs during the writing process.
        """
        blob_client = await self._prepare_container_and_blob_client(
            container, data_object, overwrite
        )
        if blob_client:
            buffer = BytesIO()
            data.to_parquet(buffer, engine=self.parquet_engine)
            buffer.seek(0)
            await blob_client.upload_blob(buffer, overwrite=overwrite)
            logging.info(
                f"Written dataframe to Blob '{data_object}' in container '{container}'"
            )

    def list_objects(self, container: str, regex_pattern: str = None) -> List[str]:
        """
        Lists all objects (blobs) in a specified container, optionally filtered by a regex pattern.

        Args:
            container (str): The name of the container from which to list objects.
            regex_pattern (str, optional): The regex pattern to filter object names. Defaults to None.

        Returns:
            List[str]: A list of object names in the specified container that match the regex pattern.
        """
        container_client = ContainerClient.from_connection_string(
            conn_str=self.connection_string, container_name=container
        )
        blobs = container_client.list_blobs()

        if regex_pattern is not None:
            pattern = re.compile(regex_pattern)
            return [blob.name for blob in blobs if pattern.match(blob.name)]
        else:
            return [blob.name for blob in blobs]

    async def list_all_containers(self) -> List[str]:
        """
        Asynchronously lists all containers in the Azure Blob Storage account.

        Returns:
            List[str]: A list of container names in the Azure Blob Storage account.
        """
        container_names = []
        async for container in self.blob_service_client.list_containers():
            container_names.append(container.name)
        return container_names

    def blob_exists(self, container: str, data_object: str) -> bool:
        """
        Checks if a specified blob exists in a given container in Azure Blob Storage.

        Args:
            container (str): The name of the Azure Blob Storage container.
            data_object (str): The name of the blob to check.

        Returns:
            bool: True if the blob exists, False otherwise.
        """
        blob_client = self.blob_service_client.get_blob_client(container, data_object)
        return blob_client.exists()

    async def container_exists(self, container: str) -> bool:
        """
        Checks if a specified container exists in Azure Blob Storage.

        Args:
            container (str): The name of the Azure Blob Storage container.

        Returns:
            bool: True if the blob exists, False otherwise.
        """
        container_client = self.blob_service_client.get_container_client(container)
        return await container_client.exists()

    async def delete_blob(self, container: str, data_object: str) -> None:
        """
        Asynchronously deletes a blob from a specified container in Azure Blob Storage.

        Args:
            container (str): The name of the Azure Blob Storage container.
            data_object (str): The name of the blob to delete.

        Raises:
            ResourceNotFoundError: If the specified blob does not exist.
        """
        try:
            container_client = self.blob_service_client.get_container_client(container)
            blob_client = container_client.get_blob_client(data_object)
            await blob_client.delete_blob()
            logging.info(
                f"Blob '{data_object}' in container '{container}' has been deleted."
            )
        except ResourceNotFoundError as e:
            logging.error(
                f"Blob '{data_object}' not found in container '{container}': {e}"
            )
            raise

    async def delete_container(self, container: str) -> None:
        """
        Asynchronously deletes an entire container from Azure Blob Storage.

        Args:
            container (str): The name of the Azure Blob Storage container to delete.

        Raises:
            ResourceNotFoundError: If the specified container does not exist.
        """
        try:
            container_client = self.blob_service_client.get_container_client(container)
            await container_client.delete_container()
            logging.info(f"Container '{container}' has been deleted.")
        except ResourceNotFoundError as e:
            logging.error(f"Container '{container}' not found: {e}")
            raise
