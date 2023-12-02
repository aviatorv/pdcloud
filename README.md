# pdcloud

`pdcloud` is a Python package designed to simplify and accelerate the onboarding of data stored in cloud environments. It's built to be cloud-agnostic, allowing seamless access to dataframes stored across various cloud platforms.

## Simplifying Cloud Data Access with `pdcloud`

`pdcloud` offers a unified interface to interact with multiple cloud storage providers, abstracting away the complexities of dealing with different cloud-specific APIs. Key advantages include:

- **Cloud-Agnostic Interface**: One interface to access data across Azure, AWS, GCP, and more, removing the need to understand each cloud provider's specifics.
- **Streamlined Data Operations**: Whether reading or writing data, `pdcloud` provides a consistent, intuitive API, simplifying cloud data operations.
- **Optimized Data Handling**: Leveraging PyArrow and Parquet, `pdcloud` ensures efficient, fast, and cost-effective data processing.

Through `pdcloud`, users gain a straightforward, efficient path to access and manipulate cloud-stored data, irrespective of the underlying cloud platform.

## Benefits of Using PyArrow and Parquet

`pdcloud` leverages the power of PyArrow and Parquet for data storage and processing, offering several key advantages:

- **Efficient Data Storage**: Parquet stores data in a columnar format, which is more space-efficient compared to row-based storage, especially for analytical queries.

- **Optimized for Performance**: PyArrow's columnar memory format enables fast data access and efficient in-memory computing, which is crucial for analytics.

- **Cross-platform Support**: Parquet is supported across multiple programming languages and platforms, ensuring compatibility and flexibility.

- **Scalability**: Ideal for handling large datasets, Parquet efficiently scales to accommodate massive volumes of data.

- **Data Compression**: Parquet supports various compression techniques, significantly reducing storage costs and improving I/O performance.

- **Schema Evolution**: Parquet supports schema evolution, allowing modification of the schema over time without the need to rewrite the dataset.

By using PyArrow and Parquet, `pdcloud` ensures that data is stored and accessed in the most efficient, performant, and cost-effective manner.

## Design Choices in `pdcloud`

`pdcloud` is crafted with the vision of simplifying data access across various cloud platforms. Key design choices include:

- **Unified API**: A single, intuitive interface for all cloud storage operations, regardless of the cloud provider.
- **Abstraction Layer**: Abstracts the complexities of each cloud provider's API, providing a seamless experience.
- **Cloud-Agnostic Approach**: Designed to be adaptable to different cloud environments, ensuring flexibility and broad applicability.
- **Optimized Data Processing**: Integration with PyArrow and Parquet for efficient data handling, suitable for both small and large-scale datasets.
- **Focus on Performance and Scalability**: Ensures efficient data operations, catering to the needs of both individual users and large enterprises.

These design choices reflect our commitment to providing a versatile, efficient, and user-friendly tool for cloud-based data management.

## Key Features

- **Cloud Agnostic**: Compatible with major cloud providers, enabling access to data regardless of its cloud location.
- **Efficient Data Onboarding**: Reduces the steps involved in data transfer and processing, moving away from traditional methods like SFTP/FTP.
- **Direct Data Access**: Facilitates direct access to data through simple cloud configurations and connection strings.
- **Standardized Data Format**: Utilizes Parquet format for data storage and retrieval, ensuring efficiency and uniformity.

## Motivation

The goal of `pdcloud` is to revolutionize how data providers share and users access data. By eliminating the cumbersome process of data transfer and storage, `pdcloud` enables users to onboard data swiftly and efficiently. Upon signing necessary data agreements, users can instantly access data provided by vendors through unique cloud configurations, significantly cutting down the time and resources typically spent on data integration.

## Features

- Cloud agnostic: Works with Azure Blob Storage, with planned support for AWS S3 and Google Cloud Storage.
- Asynchronous and synchronous read/write operations.
- Utilizes Apache Arrow for efficient data handling.

## Installation

```bash
pip install pdcloud
```

## Usage

### Azure Storage Adapter

```Python
import pandas as pd

from pdcloud.adapters.azure import AzureStorageAdapter
from pdcloud.core.model import Lib

# Initialize the Azure Storage Adapter
connection_string = ""
azure_storage = AzureStorageAdapter(connection_string)

# Define the container name
container_name = "library"

# Create an instance of the Lib class
lib = Lib(container=container_name, storage=azure_storage)


# Read and process all data objects from the container
all_data: pd.DataFrame = lib.read_all()
print("All Data:", all_data)

# Read and process a specific data object from the container
data_object_name = "mydata"
specific_data: pd.DataFrame = lib.read(data_object_name)
print("Specific Data Object:", specific_data)

# Write a DataFrame to the same Container
lib.write("mydata", data=df, overwrite=True)

# Write a DataFrame to a different container
lib.write("mydata", container="library", data=df, overwrite=True)
```

## Contributing

Contributions to pdcloud are welcome! Please read our contributing guidelines for details on how to contribute to the project.

## License

This project is licensed under the MIT License.
