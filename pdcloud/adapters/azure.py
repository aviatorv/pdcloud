import logging
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

    def list_objects(self, container: str) -> List[str]:
        container = ContainerClient.from_connection_string(conn_str=self.connection_string, container_name=container)
        return [blob["name"] for blob in container.list_blobs()]
