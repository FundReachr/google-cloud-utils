from __future__ import annotations

import json
import logging
import os
import threading
import traceback
from datetime import datetime
from io import BytesIO, StringIO
from typing import Any, Dict, List, Optional

import google.cloud.exceptions
import pandas as pd
from dotenv import load_dotenv
from google.auth import compute_engine
from google.cloud import storage
from google.oauth2 import service_account
from tenacity import retry, stop_after_attempt, wait_exponential

load_dotenv()

logger = logging.getLogger(__name__)


class CloudStorageHandler:
    _instance = None
    _lock = threading.Lock()

    def __new__(cls, *args, **kwargs):
        if cls._instance is not None and getattr(cls._instance, "storage_client", None):
            return cls._instance
        with cls._lock:
            if cls._instance is None:
                cls._instance = super(CloudStorageHandler, cls).__new__(cls)
            if not getattr(cls._instance, "storage_client", None):
                cls._instance._initialize(*args, **kwargs)
        return cls._instance

    def _initialize(
        self,
        serviceAccountJson: Optional[Dict[str, Any]] = None,
        client: Optional[storage.Client] = None,
    ) -> None:
        logger.info("Initializing CloudStorageHandler")
        if hasattr(self, "storage_client"):
            logger.debug("CloudStorageHandler already initialized, skipping")
            return
        try:
            if client:
                logger.info("Initializing GCS client via provided client")
                self.storage_client = client
            elif serviceAccountJson:
                logger.info("Initializing GCS client via provided serviceAccountJson")
                self.storage_credentials = service_account.Credentials.from_service_account_info(serviceAccountJson)
                self.storage_client = storage.Client(credentials=self.storage_credentials)
            else:
                try:
                    logger.info("Attempting GCS client initialization via file: var/cloud_storage_service_account.json")
                    with open("var/cloud_storage_service_account.json", "r") as file:
                        self.storage_credentials = service_account.Credentials.from_service_account_info(json.load(file))
                        self.storage_client = storage.Client(credentials=self.storage_credentials)
                    logger.info("Successfully initialized GCS client via file: var/cloud_storage_service_account.json")
                except FileNotFoundError:
                    logger.debug("File var/cloud_storage_service_account.json not found, attempting env var")
                    try:
                        logger.info("Attempting GCS client initialization via env var: CLOUD_STORAGE_SERVICE_ACCOUNT_JSON")
                        sa_json = os.getenv("CLOUD_STORAGE_SERVICE_ACCOUNT_JSON")
                        if not sa_json:
                            raise ValueError("CLOUD_STORAGE_SERVICE_ACCOUNT_JSON is empty or invalid.")
                        serviceAccountJson = json.loads(sa_json)
                        self.storage_credentials = service_account.Credentials.from_service_account_info(serviceAccountJson)
                        self.storage_client = storage.Client(credentials=self.storage_credentials)
                        logger.info("Successfully initialized GCS client via env var: CLOUD_STORAGE_SERVICE_ACCOUNT_JSON")
                    except (ValueError, json.JSONDecodeError) as e:
                        logger.warning(f"Failed to initialize via env var: {e}, falling back to compute engine credentials")
                        logger.info("Initializing GCS client via compute engine default credentials")
                        credentials = compute_engine.Credentials()
                        self.storage_client = storage.Client(credentials=credentials)
                        logger.info("Successfully initialized GCS client via compute engine default credentials")
                except Exception as e:
                    logger.warning(f"Failed to initialize via file: {e}, attempting env var")
                    try:
                        logger.info("Attempting GCS client initialization via env var: CLOUD_STORAGE_SERVICE_ACCOUNT_JSON")
                        sa_json = os.getenv("CLOUD_STORAGE_SERVICE_ACCOUNT_JSON")
                        if not sa_json:
                            raise ValueError("CLOUD_STORAGE_SERVICE_ACCOUNT_JSON is empty or invalid.")
                        serviceAccountJson = json.loads(sa_json)
                        self.storage_credentials = service_account.Credentials.from_service_account_info(serviceAccountJson)
                        self.storage_client = storage.Client(credentials=self.storage_credentials)
                        logger.info("Successfully initialized GCS client via env var: CLOUD_STORAGE_SERVICE_ACCOUNT_JSON")
                    except (ValueError, json.JSONDecodeError) as ex:
                        logger.warning(f"Failed to initialize via env var: {ex}, falling back to compute engine credentials")
                        logger.info("Initializing GCS client via compute engine default credentials")
                        credentials = compute_engine.Credentials()
                        self.storage_client = storage.Client(credentials=credentials)
                        logger.info("Successfully initialized GCS client via compute engine default credentials")
            self.project_id = self.storage_client.project
            logger.info(f"CloudStorageHandler initialized successfully with project_id: {self.project_id}")
        except Exception as e:
            logger.exception(f"Error initializing CloudStorageHandler: {e}")
            type(self)._instance = None
            raise Exception(f"Error initializing GCP credentials: {e}") from e

    # ------------------------------------------------------------------
    # google-cloud-utils methods (exact copy)
    # ------------------------------------------------------------------

    def retrieve_file_as_bytes(
        self, bucket_name: str, file_name: str, folder_path: Optional[str] = None
    ) -> Optional[BytesIO]:
        try:
            client = self.storage_client
            try:
                bucket = client.get_bucket(bucket_name)
            except google.cloud.exceptions.NotFound:
                print(f"Bucket {bucket_name} does not exist.")
                return None
            blob = bucket.blob(f"{folder_path}/{file_name}" if folder_path else file_name)
            if blob.exists():
                file_content = BytesIO()
                blob.download_to_file(file_content)
                file_content.seek(0)
                print(f"File {file_name} retrieved from bucket {bucket_name}.")
                return file_content
            else:
                print(f"File {file_name} does not exist in bucket {bucket_name}.")
                return None
        except Exception as e:
            print(f"Error retrieving file: {e}")
            traceback.print_exc()
            raise Exception("Failed to retrieve file from GCS.")

    def get_file_as_json(
        self, bucket_name: str, file_name: str, folder_path: Optional[str] = None
    ) -> Dict[str, Any]:
        try:
            file_content = self.retrieve_file_as_bytes(bucket_name, file_name, folder_path)
            if file_content is None:
                return {}
            return json.load(file_content)
        except Exception as e:
            print(f"Error reading JSON file: {e}")
            traceback.print_exc()
            raise Exception("Failed to read JSON file from GCS.")

    def upload_file_as_bytes(
        self,
        file_bytes: bytes,
        file_name: str,
        file_content_type: str,
        bucket_name: str,
        folder_path: Optional[str] = None,
    ) -> str:
        try:
            client = self.storage_client
            try:
                bucket = client.get_bucket(bucket_name)
            except google.cloud.exceptions.NotFound:
                print(f"Bucket {bucket_name} does not exist. Creating it...")
                bucket = client.create_bucket(bucket_name)
                print(f"Bucket {bucket_name} created.")
            blob_path = f"{folder_path}/{file_name}" if folder_path else file_name
            blob = bucket.blob(blob_path)
            blob.upload_from_string(file_bytes, content_type=file_content_type)
            print(f"File {file_name} uploaded to bucket {bucket_name} at {blob_path}.")
            url = blob.generate_signed_url(version="v4", expiration=3600, method="GET")
            return url
        except Exception as e:
            print(f"Error uploading file: {e}")
            traceback.print_exc()
            raise Exception("Failed to upload file to GCS.")

    def upload_file(
        self,
        bucket_name: str,
        file_name: str,
        file: Dict[str, Any],
        folder_path: Optional[str] = None,
    ) -> None:
        try:
            client = self.storage_client
            try:
                bucket = client.get_bucket(bucket_name)
            except google.cloud.exceptions.NotFound:
                print(f"Bucket {bucket_name} does not exist. Creating it...")
                bucket = client.create_bucket(bucket_name)
                print(f"Bucket {bucket_name} created.")
            blob = bucket.blob(f"{folder_path}/{file_name}" if folder_path else file_name)
            blob.upload_from_string(json.dumps(file), content_type="application/json")
            print(f"File {file_name} uploaded to bucket {bucket_name}.")
        except Exception as e:
            print(f"Error uploading file: {e}")
            traceback.print_exc()
            raise Exception("Failed to upload file to GCS.")

    def get_file_download_url(
        self,
        file_name: str,
        bucket_name: str,
        folder_path: Optional[str] = None,
        expiration: int = 3600,
    ) -> Optional[str]:
        try:
            client = self.storage_client
            try:
                bucket = client.get_bucket(bucket_name)
            except google.cloud.exceptions.NotFound:
                print(f"Bucket {bucket_name} does not exist.")
                return None
            blob = bucket.blob(f"{folder_path}/{file_name}" if folder_path else file_name)
            if blob.exists():
                return blob.generate_signed_url(version="v4", expiration=expiration, method="GET")
            else:
                print(f"File {file_name} does not exist in bucket {bucket_name}.")
                return None
        except Exception as e:
            print(f"Error getting file download URL: {e}")
            traceback.print_exc()
            return None

    def create_bucket(self, bucket_name: str) -> storage.Bucket:
        try:
            client = self.storage_client
            bucket = client.bucket(bucket_name)
            if not bucket.exists():
                bucket = client.create_bucket(bucket_name)
                print(f"Bucket {bucket_name} created.")
            else:
                print(f"Bucket {bucket_name} already exists.")
            return bucket
        except Exception as e:
            print(f"Error creating bucket: {e}")
            traceback.print_exc()
            raise Exception("Failed to create bucket in GCS.")

    def bucket_exists(self, bucket_name: str) -> bool:
        try:
            return self.storage_client.bucket(bucket_name).exists()
        except Exception as e:
            print(f"Error checking bucket existence: {e}")
            traceback.print_exc()
            return False

    def check_folder_exists(
        self, folder_name: str, bucket: str, parent_folder: str = ""
    ) -> bool:
        try:
            client = self.storage_client
            bkt = client.get_bucket(bucket)
            folder_path = f"{parent_folder}/{folder_name}/" if parent_folder else f"{folder_name}/"
            blobs = list(bkt.list_blobs(prefix=folder_path, max_results=1))
            exists = len(blobs) > 0
            print(f"Folder {folder_path} exists in bucket {bucket}: {exists}")
            return exists
        except Exception as e:
            print(f"Error checking folder existence: {e}")
            traceback.print_exc()
            raise Exception("Failed to check folder existence in GCS.")

    def create_folder(
        self, folder_name: str, bucket: str, parent_folder: str = ""
    ) -> bool:
        if self.check_folder_exists(folder_name, bucket, parent_folder):
            print(f"Folder {folder_name} already exists in bucket {bucket}.")
            return True
        try:
            client = self.storage_client
            bkt = client.get_bucket(bucket)
            folder_path = f"{parent_folder}/{folder_name}/" if parent_folder else f"{folder_name}/"
            blob = bkt.blob(folder_path)
            blob.upload_from_string("", content_type="application/x-www-form-urlencoded;charset=UTF-8")
            print(f"Folder {folder_path} created in bucket {bucket}.")
            return True
        except Exception as e:
            print(f"Error creating folder: {e}")
            traceback.print_exc()
            raise Exception("Failed to create folder in GCS.")

    def delete_folder(
        self, folder_name: str, bucket: str, parent_folder: str = ""
    ) -> None:
        try:
            client = self.storage_client
            bkt = client.get_bucket(bucket)
            folder_path = f"{parent_folder}/{folder_name}/" if parent_folder else f"{folder_name}/"
            blobs = bkt.list_blobs(prefix=folder_path)
            for blob in blobs:
                blob.delete()
                print(f"Deleted blob {blob.name} from folder {folder_path}.")
            print(f"Folder {folder_path} deleted from bucket {bucket}.")
        except Exception as e:
            print(f"Error deleting folder: {e}")
            traceback.print_exc()
            raise Exception("Failed to delete folder in GCS.")

    def list_files(self, bucket_name: str, prefix: str = "") -> List[str]:
        try:
            client = self.storage_client
            bucket = client.get_bucket(bucket_name)
            blobs = bucket.list_blobs(prefix=prefix)
            file_list = [blob.name for blob in blobs]
            print(f"Files in bucket {bucket_name} with prefix '{prefix}': {file_list}")
            return file_list
        except Exception as e:
            print(f"Error listing files: {e}")
            traceback.print_exc()
            raise Exception("Failed to list files in GCS.")

    def get_file_metadata(
        self, bucket_name: str, file_name: str, folder_path: Optional[str] = None
    ) -> Dict[str, Any]:
        try:
            client = self.storage_client
            bucket = client.get_bucket(bucket_name)
            blob = bucket.blob(f"{folder_path}/{file_name}" if folder_path else file_name)
            if blob.exists():
                blob.reload()
                metadata = {
                    "size": blob.size,
                    "content_type": blob.content_type,
                    "updated": blob.updated,
                    "generation": blob.generation,
                    "metageneration": blob.metageneration,
                }
                print(f"Metadata for file {file_name} in bucket {bucket_name}: {metadata}")
                return metadata
            else:
                print(f"File {file_name} does not exist in bucket {bucket_name}.")
                return {}
        except Exception as e:
            print(f"Error getting file metadata: {e}")
            traceback.print_exc()
            raise Exception("Failed to get file metadata from GCS.")

    def get_file_as_bytes(
        self, bucket_name: str, file_name: str, folder_path: Optional[str] = None
    ) -> Optional[bytes]:
        try:
            client = self.storage_client
            try:
                bucket = client.get_bucket(bucket_name)
            except google.cloud.exceptions.NotFound:
                print(f"Bucket {bucket_name} does not exist.")
                return None
            blob = bucket.blob(f"{folder_path}/{file_name}" if folder_path else file_name)
            if blob.exists():
                file_content = blob.download_as_bytes()
                print(f"File {file_name} retrieved from bucket {bucket_name}.")
                return file_content
            else:
                print(f"File {file_name} does not exist in bucket {bucket_name}.")
                return None
        except Exception as e:
            print(f"Error retrieving file: {e}")
            traceback.print_exc()
            raise Exception("Failed to retrieve file from GCS.")

    def move_file(
        self,
        source_bucket_name: str,
        source_file_name: str,
        destination_bucket_name: str,
        destination_file_name: str,
    ) -> None:
        try:
            client = self.storage_client
            source_bucket = client.get_bucket(source_bucket_name)
            destination_bucket = client.get_bucket(destination_bucket_name)
            blob = source_bucket.blob(source_file_name)
            source_bucket.copy_blob(blob, destination_bucket, destination_file_name)
            blob.delete()
            print(f"File {source_file_name} moved from bucket {source_bucket_name} to bucket {destination_bucket_name}.")
        except Exception as e:
            print(f"Error moving file: {e}")
            traceback.print_exc()
            raise Exception("Failed to move file in GCS.")

    def get_bucket(self, bucket_name: str) -> Optional[storage.Bucket]:
        try:
            bucket = self.storage_client.get_bucket(bucket_name)
            print(f"Bucket {bucket_name} retrieved.")
            return bucket
        except google.cloud.exceptions.NotFound:
            print(f"Bucket {bucket_name} does not exist.")
            return None
        except Exception as e:
            print(f"Error getting bucket: {e}")
            traceback.print_exc()
            raise Exception("Failed to get bucket from GCS.")

    # ------------------------------------------------------------------
    # Bulq-specific additional methods
    # ------------------------------------------------------------------

    def load_csv(self, bucket: str, blob_name: str) -> pd.DataFrame:
        blob = self.storage_client.bucket(bucket).blob(blob_name)
        if not blob.exists():
            raise FileNotFoundError(f"gs://{bucket}/{blob_name}")
        content = blob.download_as_text()
        try:
            return pd.read_csv(StringIO(content))
        except Exception:
            return pd.read_csv(StringIO(content), sep=";")

    def load_txt(self, bucket: str, blob_name: str) -> str:
        return self.storage_client.bucket(bucket).blob(blob_name).download_as_text()

    def load_bytes(self, bucket: str, blob_name: str) -> bytes:
        blob = self.storage_client.bucket(bucket).blob(blob_name)
        if not blob.exists():
            raise FileNotFoundError(f"gs://{bucket}/{blob_name}")
        return blob.download_as_bytes()

    def load_bytes_io(
        self,
        bucket: str,
        blob_name: str,
        folder_path: Optional[str] = None,
    ) -> Optional[BytesIO]:
        full_path = f"{folder_path}/{blob_name}" if folder_path else blob_name
        blob = self.storage_client.bucket(bucket).blob(full_path)
        if not blob.exists():
            return None
        buf = BytesIO()
        blob.download_to_file(buf)
        buf.seek(0)
        return buf

    def load_excel(
        self,
        bucket: str,
        blob_name: str,
        sheet_name: int | str = 0,
    ) -> pd.DataFrame:
        blob = self.storage_client.bucket(bucket).blob(blob_name)
        if not blob.exists():
            raise FileNotFoundError(f"gs://{bucket}/{blob_name}")
        data = blob.download_as_bytes()
        return pd.read_excel(BytesIO(data), sheet_name=sheet_name)

    def load_json(
        self,
        bucket: str,
        blob_name: str,
        folder_path: Optional[str] = None,
    ) -> Dict[str, Any]:
        full_path = f"{folder_path}/{blob_name}" if folder_path else blob_name
        blob = self.storage_client.bucket(bucket).blob(full_path)
        if not blob.exists():
            return {}
        return json.loads(blob.download_as_text())

    def list_blobs(self, bucket: str, prefix: str = "") -> List[str]:
        return [b.name for b in self.storage_client.bucket(bucket).list_blobs(prefix=prefix)]

    def delete_blobs_with_prefix(self, bucket: str, prefix: str) -> None:
        blobs = list(self.storage_client.bucket(bucket).list_blobs(prefix=prefix))
        for blob in blobs:
            blob.delete()
        logger.info(f"Deleted {len(blobs)} blobs under gs://{bucket}/{prefix}")

    def move_blob(
        self,
        src_bucket: str,
        src_blob: str,
        dst_bucket: str,
        dst_blob: str,
    ) -> None:
        source = self.storage_client.bucket(src_bucket).blob(src_blob)
        self.storage_client.bucket(src_bucket).copy_blob(
            source, self.storage_client.bucket(dst_bucket), dst_blob
        )
        source.delete()
        logger.info(f"Moved gs://{src_bucket}/{src_blob} → gs://{dst_bucket}/{dst_blob}")

    @retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=1, min=2, max=60))
    def upload_from_filename(
        self,
        bucket: str,
        src_path: str,
        dest_blob: str,
        *,
        check_exists: bool = False,
        logs: bool = True,
    ) -> None:
        blob = self.storage_client.bucket(bucket).blob(dest_blob)
        if check_exists and blob.exists():
            if logs:
                logger.info(f"Already exists, skipping: gs://{bucket}/{dest_blob}")
            return
        blob.upload_from_filename(src_path)
        if logs:
            logger.info(f"Uploaded {src_path} → gs://{bucket}/{dest_blob}")

    @retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=1, min=2, max=60))
    def upload_string(self, bucket: str, content: str, dest_blob: str) -> None:
        self.storage_client.bucket(bucket).blob(dest_blob).upload_from_string(
            content, content_type="text/plain"
        )
        logger.info(f"Uploaded string → gs://{bucket}/{dest_blob}")

    @retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=1, min=2, max=60))
    def upload_raw_string(self, bucket: str, content: str, dest_blob: str) -> None:
        self.storage_client.bucket(bucket).blob(dest_blob).upload_from_string(content)
        logger.info(f"Uploaded raw string → gs://{bucket}/{dest_blob}")

    @retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=1, min=2, max=60))
    def upload_dataframe(self, bucket: str, df: pd.DataFrame, dest_blob: str) -> None:
        self.storage_client.bucket(bucket).blob(dest_blob).upload_from_string(
            df.to_csv(index=False), content_type="text/csv"
        )
        logger.info(f"Uploaded DataFrame ({len(df)} rows) → gs://{bucket}/{dest_blob}")

    @retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=1, min=2, max=60))
    def upload_json(
        self,
        bucket: str,
        data: Dict[str, Any],
        dest_blob: str,
        folder_path: Optional[str] = None,
    ) -> None:
        full_path = f"{folder_path}/{dest_blob}" if folder_path else dest_blob
        self.storage_client.bucket(bucket).blob(full_path).upload_from_string(
            json.dumps(data), content_type="application/json"
        )
        logger.info(f"Uploaded JSON → gs://{bucket}/{full_path}")

    @retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=1, min=2, max=60))
    def upload_bytes(
        self,
        bucket: str,
        data: bytes,
        dest_blob: str,
        content_type: str = "application/octet-stream",
        folder_path: Optional[str] = None,
    ) -> None:
        full_path = f"{folder_path}/{dest_blob}" if folder_path else dest_blob
        self.storage_client.bucket(bucket).blob(full_path).upload_from_string(
            data, content_type=content_type
        )
        logger.info(f"Uploaded {len(data)} bytes → gs://{bucket}/{full_path}")


# ---------------------------------------------------------------------------
# GCSLogHandler
# ---------------------------------------------------------------------------


class GCSLogHandler(logging.Handler):
    """Buffered log handler that periodically flushes chunks to GCS.

    Blob path: {prefix}/{session_ts}/chunk_{n:04d}.log
    """

    def __init__(
        self,
        bucket: storage.Bucket,
        prefix: str = "debug_logs",
        max_lines: int = 5000,
        flush_interval: float = 600.0,
    ) -> None:
        super().__init__(level=logging.DEBUG)
        self.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
        self._bucket = bucket
        self._prefix = prefix.rstrip("/")
        self._max_lines = max_lines
        self._flush_interval = flush_interval
        self._buffer: list[str] = []
        self._chunk = 0
        self._lock = threading.Lock()
        self._session_ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        self._timer: threading.Timer | None = None
        self._schedule()

    def _schedule(self) -> None:
        self._timer = threading.Timer(self._flush_interval, self._on_timer)
        self._timer.daemon = True
        self._timer.start()

    def _on_timer(self) -> None:
        self.flush()
        self._schedule()

    def emit(self, record: logging.LogRecord) -> None:
        try:
            line = self.format(record)
        except Exception:
            self.handleError(record)
            return
        with self._lock:
            self._buffer.append(line)
            if len(self._buffer) >= self._max_lines:
                self._flush_locked()

    def flush(self) -> None:
        with self._lock:
            self._flush_locked()

    def _flush_locked(self) -> None:
        if not self._buffer:
            return
        content = "\n".join(self._buffer) + "\n"
        blob_path = f"{self._prefix}/{self._session_ts}/chunk_{self._chunk:04d}.log"
        try:
            self._bucket.blob(blob_path).upload_from_string(content, content_type="text/plain")
        except Exception:
            pass  # never let logging errors crash the app
        self._buffer.clear()
        self._chunk += 1

    def close(self) -> None:
        if self._timer:
            self._timer.cancel()
        self.flush()
        super().close()
