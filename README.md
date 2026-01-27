# Google Cloud Utils

A professional Python package providing a unified and simplified interface for interacting with various Google Cloud Platform (GCP) services. This library is designed to streamline common operations, handle authentication seamlessly, and ensure best practices in error handling and resource management.

## Features

- **BigQuery**: simplified query execution, schema generation, and data loading (JSON, Pandas DataFrame).
- **Cloud Storage**: Easy file upload/download (bytes, JSON, Excel), bucket management, and signed URL generation.
- **Cloud Tasks**: Task creation and queue management.
- **Cloud Scheduler**: Job creation, management, and triggering.
- **Secret Manager**: Secure retrieval of secrets.
- **Pub/Sub**: Topic and subscription management, message publishing and pulling.
- **Datastore & Firestore**: Unified CRUD operations for NoSQL data.

## Installation

You can install the package directly or via pip if hosted:

```bash
pip install .
# OR
pip install -e .  # for editable/development mode
```

## Configuration

The package relies on environment variables for credentials and configuration. Create a `.env` file in your project root or set these variables in your environment:

```env
# Service Account JSON strings (recommended for production)
BIGQUERY_SERVICE_ACCOUNT_JSON='{...}'
CLOUD_STORAGE_SERVICE_ACCOUNT_JSON='{...}'
SECRET_MANAGER_SERVICE_ACCOUNT_JSON='{...}'
CLOUD_TASKS_SERVICE_ACCOUNT_JSON='{...}'
FIRESTORE_SERVICE_ACCOUNT_JSON='{...}'
PUBSUB_SERVICE_ACCOUNT_JSON='{...}'

# Or paths to service account files (useful for local dev)
# Ensure files exist at these paths relative to execution
# var/bigquery_service_account.json
# var/storage_service_account.json
# ...
```

## Usage

### Initializing the Client

The `GoogleCloudHandler` acts as the main entry point, aggregating all service handlers. It ensures singleton access and thread-safe initialization.

```python
from google_cloud_utils.client import GoogleCloudHandler

# Initialize with default credentials path or env vars
gcp = GoogleCloudHandler()

# Access specific handlers
bq = gcp.bigQueryHandler
storage = gcp.cloudStorageHandler
secrets = gcp.secretManagerHandler
```

### BigQuery Example

```python
# Run a query
query = "SELECT * FROM `my-project.dataset.table` LIMIT 10"
df, job_id = gcp.bigQueryHandler.runQuery(query)
print(df.head())

# Load data from a Pandas DataFrame
import pandas as pd
df_new = pd.DataFrame({'id': [1, 2], 'value': ['a', 'b']})
gcp.bigQueryHandler.Loader.loadDataframeToBigQuery(
    dataframe=df_new,
    tableId='my_table',
    datasetId='my_dataset'
)
```

### Cloud Storage Example

```python
# Upload a file
with open('data.json', 'rb') as f:
    gcp.cloudStorageHandler.upload_file_as_bytes(
        bucket_name='my-bucket',
        file_name='data/data.json',
        file=f
    )

# Download a file
file_bytes = gcp.cloudStorageHandler.get_file_as_bytes(
    bucket_name='my-bucket',
    file_name='data/data.json'
)
```

## Project Structure

```
google_cloud_utils/
├── client.py             # Main entry point class
├── handlers/             # Service-specific implementation
│   ├── bigquery/
│   ├── cloud_storage/
│   ├── cloud_tasks/
│   ├── cloud_scheduler/
│   ├── datastore/
│   ├── firestore/
│   ├── pubsub/
│   └── secret_manager/
```

## Contributing

1.  Fork the repository.
2.  Create a feature branch.
3.  Commit your changes.
4.  Push to the branch.
5.  Create a new Pull Request.

## License

MIT
