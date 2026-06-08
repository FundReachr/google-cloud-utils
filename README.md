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

The `GoogleCloudHandler` acts as the main entry point, aggregating all service handlers with thread-safe, lazy initialization.

The package ships two variants:

- **`GoogleCloudHandler`** (default) — a **singleton**. Every construction returns the same process-wide instance, and the service handlers it builds are the shared process-wide handler singletons, so each handler is initialized at most once.
- **`GoogleCloudHandlerNonSingleton`** — returns a **fresh, independent instance** on each construction, and builds its own **isolated** service handlers (in non-singleton mode) rather than the shared singletons. Use it when you need multiple clients side by side with different credentials, or want isolation between callers.

Both expose the identical set of handler properties.

```python
from google_cloud_utils import GoogleCloudHandler, GoogleCloudHandlerNonSingleton
# (also importable from google_cloud_utils.client)

# Singleton (default): shared across the whole process
gcp = GoogleCloudHandler()

# Access specific handlers
bq = gcp.bigQueryHandler
storage = gcp.cloudStorageHandler
secrets = gcp.secretManagerHandler

# Non-singleton: a brand new, isolated instance each time —
# each owns its own handlers with its own credentials.
gcp_a = GoogleCloudHandlerNonSingleton(bigQueryServiceAccountJson=creds_a)
gcp_b = GoogleCloudHandlerNonSingleton(bigQueryServiceAccountJson=creds_b)
assert gcp_a is not gcp_b
assert gcp_a.bigQueryHandler is not gcp_b.bigQueryHandler  # isolated handlers
```

> **Note:** With the singleton, kwargs passed on a later construction are merged into the stored config only for handlers that have **not** been initialized yet; already-live handlers keep their existing instance. Call `GoogleCloudHandler.reset_instance()` to drop the cached singleton (useful in tests).
>
> Each service handler (e.g. `BigQueryHandler`) is itself a singleton by default and accepts a `singleton=False` keyword to construct a fresh, isolated instance directly. The non-singleton client passes this through automatically.

> **Lazy imports:** Handler modules are imported only on first access, so `import google_cloud_utils` (and importing `GoogleCloudHandler`) works without every Google Cloud client library being installed — you only pay the import cost (and need the dependency) for the handlers you actually touch. For example, a Datastore-only consumer never imports `google-cloud-run`, `gspread` or `google-genai`.

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
