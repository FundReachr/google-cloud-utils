from google.cloud import storage
from google.oauth2 import service_account
from google.auth import compute_engine
import traceback
import google.cloud.exceptions
import json
import threading
import os
from io import BytesIO
from dotenv import load_dotenv
from typing import Dict, Any, List, Optional, Union

load_dotenv()

class CloudStorageHandler:
    _instance = None
    _lock = threading.Lock()

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            with cls._lock:  # Prevent race conditions
                if cls._instance is None:
                    cls._instance = super(CloudStorageHandler, cls).__new__(cls)
                    cls._instance._initialize(*args, **kwargs)  
        return cls._instance
        
    def _initialize(self, serviceAccountJson: Optional[Dict[str, Any]] = None, client: Optional[storage.Client] = None) -> None:
        """
        Thread-safe initialization of the GCP storage client.
        
        Args:
            serviceAccountJson: Optional dictionary of service account credentials.
            client: Optional pre-initialized storage.Client.
        """
        """Initialize only once in a thread-safe way."""
        print("Initializing Cloud Storage Handler...")
        if hasattr(self, "client"):  # Prevent multiple initializations
            return
        print("Initializing GCP Cloud Storage client...")
        try:
            if client:
                print("Using provided client")
                self.storage_client = client
            elif serviceAccountJson:
                print("Using provided service account JSON")
                self.storage_credentials = service_account.Credentials.from_service_account_info(serviceAccountJson)
                self.storage_client = storage.Client(credentials=self.storage_credentials)
            
            else:
                try:
                    with open('var/cloud_storage_service_account.json', 'r') as file:
                        print("Using service account JSON from file")
                        self.storage_credentials = service_account.Credentials.from_service_account_info(json.load(file))
                        self.storage_client = storage.Client(credentials=self.storage_credentials)
                except Exception:
                    try:
                        print("Using environment variable for service account JSON")
                        print(os.getenv("CLOUD_STORAGE_SERVICE_ACCOUNT_JSON"))
                        serviceAccountJson = json.loads(os.getenv("CLOUD_STORAGE_SERVICE_ACCOUNT_JSON"))
                        if not serviceAccountJson:
                            raise ValueError("CLOUD_STORAGE_SERVICE_ACCOUNT_JSON is empty or invalid.")
                        self.storage_credentials = service_account.Credentials.from_service_account_info(serviceAccountJson)
                        self.storage_client = storage.Client(credentials=self.storage_credentials)
                    except Exception:
                        print("Using default credentials")
                        credentials = compute_engine.Credentials()
                        self.storage_client = storage.Client(credentials=credentials)
            self.project_id = self.storage_client.project
        except Exception as e:
            traceback.print_exc()
            raise Exception(f"Error initializing GCP credentials: {e}")
        
    def retrieve_file_as_bytes(self, bucket_name: str, file_name: str, folder_path: Optional[str] = None) -> Optional[BytesIO]:
        """
        Download a file from GCS and return it as a BytesIO stream.

        Args:
            bucket_name: Name of the GCS bucket.
            file_name: Name of the file.
            folder_path: Optional folder path within the bucket.

        Returns:
            Optional[BytesIO]: The file content as a stream, or None if not found.
        """
        try:
            client = self.storage_client
            
            try :
                bucket = client.get_bucket(bucket_name)
            except google.cloud.exceptions.NotFound as e:
                print(f"Bucket {bucket_name} does not exist.")
                return None
            
            blob = bucket.blob(f"{folder_path}/{file_name}" if folder_path else file_name)
            if blob.exists():
                file_content = BytesIO()
                blob.download_to_file(file_content)
                file_content.seek(0)  # Reset the stream position to the beginning
                print(f"File {file_name} retrieved from bucket {bucket_name}.")
                return file_content
            else:
                print(f"File {file_name} does not exist in bucket {bucket_name}.")
                return None
        except Exception as e:
            print(f"Error retrieving file: {e}")
            traceback.print_exc()
            raise Exception("Failed to retrieve file from GCS.")
        
    def get_file_as_json(self, bucket_name: str, file_name: str, folder_path: Optional[str] = None) -> Dict[str, Any]:
        """
        Download a JSON file from GCS and parse it into a dictionary.

        Args:
            bucket_name: Name of the GCS bucket.
            file_name: Name of the file.
            folder_path: Optional folder path.

        Returns:
            Dict[str, Any]: The parsed JSON data, or an empty dict if not found.
        """
        try:
            file_content = self.retrieve_file_as_bytes(bucket_name, file_name, folder_path)
            if file_content is None:
                return {}
            return json.load(file_content)
        except Exception as e:
            print(f"Error reading JSON file: {e}")
            traceback.print_exc()
            raise Exception("Failed to read JSON file from GCS.")

    def upload_file_as_bytes(self, file_bytes: bytes, file_name: str, file_content_type: str, bucket_name: str, folder_path: Optional[str] = None) -> str:
        """
        Upload raw bytes to GCS and return a signed URL.

        Args:
            file_bytes: The bytes to upload.
            file_name: Target filename.
            file_content_type: MIME type of the file.
            bucket_name: Target GCS bucket.
            folder_path: Optional destination folder.

        Returns:
            str: A signed URL for the uploaded file (valid for 1 hour).
        """
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

            print(f"✅ File {file_name} uploaded to bucket {bucket_name} at {blob_path}.")
            url = blob.generate_signed_url(
                version="v4",
                expiration=3600,  # URL valid for 1 hour
                method="GET"
            )

            return url

        except Exception as e:
            print(f"❌ Error uploading file: {e}")
            traceback.print_exc()
            raise Exception("Failed to upload file to GCS.")
            
    def upload_file(self, bucket_name: str, file_name: str, file: Dict[str, Any], folder_path: Optional[str] = None) -> None:
        """
        Upload a dictionary as a JSON file to GCS.

        Args:
            bucket_name: Target GCS bucket.
            file_name: Target filename.
            file: The dictionary to be serialized to JSON.
            folder_path: Optional destination folder.
        """
        try:
            client = self.storage_client
            
            try :
                bucket = client.get_bucket(bucket_name)
            except google.cloud.exceptions.NotFound as e:
                print(f"Bucket {bucket_name} does not exist. Creating it...")
                bucket = client.create_bucket(bucket_name)
                print(f"Bucket {bucket_name} created.")

            blob = bucket.blob(f"{folder_path}/{file_name}" if folder_path else file_name)
            blob.upload_from_string(json.dumps(file), content_type='application/json')
            print(f"File {file_name} uploaded to bucket {bucket_name}.")
        except Exception as e:
            print(f"Error uploading file: {e}")
            traceback.print_exc()
            raise Exception("Failed to upload file to GCS.")
           

    def get_file_download_url(self, file_name: str, bucket_name: str, folder_path: Optional[str] = None, expiration: int = 3600) -> Optional[str]:
        """
        Generate a signed download URL for an existing file in GCS.

        Args:
            file_name: Name of the file.
            bucket_name: GCS bucket name.
            folder_path: Optional folder path.
            expiration: URL expiration time in seconds (default 1 hour).

        Returns:
            Optional[str]: The signed URL, or None if the file doesn't exist.
        """
        try:
            client = self.storage_client
            try :
                bucket = client.get_bucket(bucket_name)
            except google.cloud.exceptions.NotFound as e:
                print(f"Bucket {bucket_name} does not exist.")
                return None
            blob = bucket.blob(f"{folder_path}/{file_name}" if folder_path else file_name)
            if blob.exists():
                return blob.generate_signed_url(
                    version="v4",
                    expiration=expiration,  
                    method="GET"
                )
            else:
                print(f"File {file_name} does not exist in bucket {bucket_name}.")
                return None
        except Exception as e:
            print(f"Error getting file download URL: {e}")
            traceback.print_exc()
            return None
        
    def create_bucket(self, bucket_name: str) -> storage.Bucket:
        """
        Ensure a GCS bucket exists, creating it if necessary.

        Args:
            bucket_name: The name of the bucket to create or retrieve.

        Returns:
            google.cloud.storage.Bucket: The bucket object.
        """
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

    def check_folder_exists(self, folder_name: str, bucket: str, parent_folder: str = "") -> bool:
        """
        Check if a 'folder' (prefix) exists in a GCS bucket.

        Args:
            folder_name: The name of the folder.
            bucket: The GCS bucket name.
            parent_folder: Optional parent folder path.

        Returns:
            bool: True if the folder exists, False otherwise.
        """
        try:
            client = self.storage_client
            bucket = client.get_bucket(bucket)
            folder_path = f"{parent_folder}/{folder_name}/" if parent_folder else f"{folder_name}/"
            blobs = list(bucket.list_blobs(prefix=folder_path, max_results=1))
            exists = len(blobs) > 0
            print(f"Folder {folder_path} exists in bucket {bucket}: {exists}")
            return exists
        except Exception as e:
            print(f"Error checking folder existence: {e}")
            traceback.print_exc()
            raise Exception("Failed to check folder existence in GCS.")

    def create_folder(self, folder_name: str, bucket: str, parent_folder: str = "") -> bool:
        """
        Create a new 'folder' (placeholder blob) in a GCS bucket.

        Args:
            folder_name: Name of the folder to create.
            bucket: GCS bucket name.
            parent_folder: Optional parent folder path.

        Returns:
            bool: True if successful.
        """
        if self.check_folder_exists(folder_name, bucket, parent_folder):
            print(f"Folder {folder_name} already exists in bucket {bucket}.")
            return True

        try:
            client = self.storage_client
            bucket = client.get_bucket(bucket)
            folder_path = f"{parent_folder}/{folder_name}/" if parent_folder else f"{folder_name}/"
            blob = bucket.blob(folder_path)
            blob.upload_from_string('', content_type='application/x-www-form-urlencoded;charset=UTF-8')
            print(f"Folder {folder_path} created in bucket {bucket}.")
            return True
        except Exception as e:
            print(f"Error creating folder: {e}")
            traceback.print_exc()
            raise Exception("Failed to create folder in GCS.")
        
    def delete_folder(self, folder_name: str, bucket: str, parent_folder: str = "") -> None:
        """
        Delete a 'folder' and all its contents from a GCS bucket.

        Args:
            folder_name: Name of the folder to delete.
            bucket: GCS bucket name.
            parent_folder: Optional parent folder path.
        """
        try:
            client = self.storage_client
            bucket = client.get_bucket(bucket)
            folder_path = f"{parent_folder}/{folder_name}/" if parent_folder else f"{folder_name}/"
            blobs = bucket.list_blobs(prefix=folder_path)
            for blob in blobs:
                blob.delete()
                print(f"Deleted blob {blob.name} from folder {folder_path}.")
            print(f"Folder {folder_path} deleted from bucket {bucket}.")
        except Exception as e:
            print(f"Error deleting folder: {e}")
            traceback.print_exc()
            raise Exception("Failed to delete folder in GCS.")
        
    def list_files(self, bucket_name: str, prefix: str = "") -> List[str]:
        """
        List all files (blobs) in a bucket matching a given prefix.

        Args:
            bucket_name: GCS bucket name.
            prefix: Optional path prefix to filter files.

        Returns:
            List[str]: A list of blob names.
        """
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
        
    def get_file_metadata(self, bucket_name: str, file_name: str, folder_path: Optional[str] = None) -> Dict[str, Any]:
        """
        Retrieve metadata for a specific file in GCS.

        Args:
            bucket_name: GCS bucket name.
            file_name: Filename.
            folder_path: Optional folder path.

        Returns:
            Dict[str, Any]: Dictionary of metadata (size, content_type, etc.).
        """
        try:
            client = self.storage_client
            bucket = client.get_bucket(bucket_name)
            blob = bucket.blob(f"{folder_path}/{file_name}" if folder_path else file_name)
            if blob.exists():
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
        
    def get_file_as_bytes(self, bucket_name: str, file_name: str, folder_path: Optional[str] = None) -> Optional[bytes]:
        """
        Download a file from GCS and return its content as raw bytes.

        Args:
            bucket_name: GCS bucket name.
            file_name: Filename.
            folder_path: Optional folder path.

        Returns:
            Optional[bytes]: The file bytes, or None if not found.
        """
        try:
            client = self.storage_client
            
            try :
                bucket = client.get_bucket(bucket_name)
            except google.cloud.exceptions.NotFound as e:
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
        
    def move_file(self,source_bucket_name: str, source_file_name: str, destination_bucket_name: str, destination_file_name: str) -> None:
        """
        Move a file from one bucket to another.
        """
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