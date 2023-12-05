import logging
import re
from io import BytesIO
from typing import List

import pandas as pd
from azure.storage.blob import ContainerClient
from azure.storage.blob.aio import BlobServiceClient

from pdcloud.ports.cloud_storage_port import CloudStoragePort


class AzureStorageAdapter(CloudStoragePort):
    """
    Adapter for Azure Blob Storage implementing the CloudStoragePort interface.

    Attributes:
        connection_string (str): Connection string for Azure Blob Storage.
        overwrite (bool): Flag to overwrite existing blobs. Defaults to True.
        parquet_engine (str): Engine for reading/writing Parquet files. Defaults to 'pyarrow'.
    """

    def __init__(self, connection_string: str, parquet_engine: str = "pyarrow"):
        """
        Initializes the AzureStorageAdapter with the given settings.
        """
        self.connection_string = connection_string
        self.blob_service_client = BlobServiceClient.from_connection_string(self.connection_string)
        self.parquet_engine = parquet_engine
        logging.basicConfig(level=logging.INFO)

    async def read_data(self, container: str, data_object: str) -> pd.DataFrame:
        """
        Asynchronously reads data from Azure Blob Storage and returns it as a Pandas DataFrame.

        Args:
            storage_container (str): The name of the Azure storage container.
            data_object (str): The name of the blob to read.

        Returns:
            pd.DataFrame: The data read from the blob, returned as a DataFrame.
        """
        container_client = self.blob_service_client.get_container_client(container)
        blob_client = container_client.get_blob_client(data_object)
        stream = await blob_client.download_blob()
        data = await stream.readall()
        return pd.read_parquet(BytesIO(data), engine=self.parquet_engine)

    async def write_data(self, container: str, data_object: str, data: pd.DataFrame, overwrite: bool = False) -> None:
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
        try:
            container_client = self.blob_service_client.get_container_client(container)
            if not await container_client.exists():
                await container_client.create_container()

            blob_client = container_client.get_blob_client(data_object)
            if await blob_client.exists() and not overwrite:
                logging.info(
                    f"Blob '{data_object}' in container '{container}' already exists and overwrite is not allowed."
                )
                return

            buffer = BytesIO()
            data.to_parquet(buffer, engine=self.parquet_engine)
            buffer.seek(0)
            await blob_client.upload_blob(buffer, overwrite=overwrite)
            logging.info(f"Written dataframe to Blob '{data_object}' in container '{container}'")
        except Exception as e:
            logging.error(f"Failed to write data to Blob: {e}")
            raise e

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
