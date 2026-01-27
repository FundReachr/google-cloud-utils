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

load_dotenv()

class CloudStorageHandler:
    """
    Handler for interacting with Google Cloud Storage.
    
    Provides methods for uploading, downloading, and managing files and buckets.
    Supports bytes, JSON, and Excel formats.
    """
    _instance = None
    _lock = threading.Lock()

    def __new__(cls, *args, **kwargs):
        """Standard singleton implementation."""
        if cls._instance is None:
            with cls._lock:  # Prevent race conditions
                if cls._instance is None:
                    cls._instance = super(CloudStorageHandler, cls).__new__(cls)
                    cls._instance._initialize(*args, **kwargs)  
        return cls._instance
        
    def _initialize(self, serviceAccountJson : dict = None, client: storage.Client = None):
        """
        Initialize the CloudStorageHandler.

        Args:
            serviceAccountJson (dict, optional): Service account credentials info.
            client (storage.Client, optional): Existing Cloud Storage client.

        Raises:
            Exception: If credentials cannot be loaded.
        """
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
                    with open('var/storage_service_account.json', 'r') as file:
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

    def get_file_as_bytes(self, bucket_name : str, file_name : str) -> BytesIO:
        try:
            client = self.storage_client
            
            try :
                bucket = client.get_bucket(bucket_name)
            except google.cloud.exceptions.NotFound as e:
                print(f"Bucket {bucket_name} does not exist.")
                return None
            
            blob = bucket.blob(file_name)
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
        
    def get_file_as_json(self, bucket_name : str, file_name : str) -> dict:
        try:
            file_content = self.retrieve_file_as_bytes(bucket_name, file_name)
            if file_content is None:
                return {}
            return json.load(file_content)
        except Exception as e:
            print(f"Error reading JSON file: {e}")
            traceback.print_exc()
            raise Exception("Failed to read JSON file from GCS.")

    def upload_file_as_bytes(self, bucket_name : str, file_name : str, file: BytesIO):
        try:
            client = self.storage_client
            
            try :
                bucket = client.get_bucket(bucket_name)
            except google.cloud.exceptions.NotFound as e:
                print(f"Bucket {bucket_name} does not exist. Creating it...")
                bucket = client.create_bucket(bucket_name)
                print(f"Bucket {bucket_name} created.")

            blob = bucket.blob(file_name)
            with blob.open("wb",ignore_flush=True) as f:
                if isinstance(file, BytesIO):
                    f.write(file.getvalue())
                else:
                    f.write(file)
            print(f"File {file_name} uploaded to bucket {bucket_name}.")
        except Exception as e:
            print(f"Error uploading file: {e}")
            traceback.print_exc()
            raise Exception("Failed to upload file to GCS.")
        
    def upload_file(self, bucket_name : str, file_name : str, file: dict):
        try:
            client = self.storage_client
            
            try :
                bucket = client.get_bucket(bucket_name)
            except google.cloud.exceptions.NotFound as e:
                print(f"Bucket {bucket_name} does not exist. Creating it...")
                bucket = client.create_bucket(bucket_name)
                print(f"Bucket {bucket_name} created.")

            blob = bucket.blob(file_name)
            blob.upload_from_string(json.dumps(file), content_type='application/json')
            print(f"File {file_name} uploaded to bucket {bucket_name}.")
        except Exception as e:
            print(f"Error uploading file: {e}")
            traceback.print_exc()
            raise Exception("Failed to upload file to GCS.")

    def get_file_download_url(self, bucket_name, file_name):
        try:
            client = self.storage_client
            try :
                bucket = client.get_bucket(bucket_name)
            except google.cloud.exceptions.NotFound as e:
                print(f"Bucket {bucket_name} does not exist.")
                return None
            blob = bucket.blob(file_name)
            if blob.exists():
                return blob.generate_signed_url(
                    version="v4",
                    expiration=3600,  # URL valid for 1 hour
                    method="GET"
                )
            else:
                print(f"File {file_name} does not exist in bucket {bucket_name}.")
                return None
        except Exception as e:
            print(f"Error getting file download URL: {e}")
            traceback.print_exc()
            return None
        
    def create_bucket(self, bucket_name: str):
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
        
    def move_file(self, source_bucket_name: str, source_file_name: str, destination_bucket_name: str, destination_file_name: str):
        try:
            client = self.storage_client
            source_bucket = client.bucket(source_bucket_name)
            destination_bucket = client.bucket(destination_bucket_name)
            blob = source_bucket.blob(source_file_name)
            if not blob.exists():
                print(f"File {source_file_name} does not exist in bucket {source_bucket_name}.")
                return
            new_blob = source_bucket.copy_blob(blob, destination_bucket, destination_file_name)
            new_blob.make_public()
            blob.delete()
            print(f"File {source_file_name} moved from bucket {source_bucket_name} to {destination_file_name} in bucket {destination_bucket_name}.")
        except Exception as e:
            print(f"Error moving file: {e}")
            traceback.print_exc()
            raise Exception("Failed to move file in GCS.")
        
    def get_bucket_files(self, bucket_name: str):
        try:
            client = self.storage_client
            bucket = client.bucket(bucket_name)
            blobs = bucket.list_blobs()
            file_names = [blob.name for blob in blobs]
            return file_names
        except Exception as e:
            print(f"Error listing files in bucket: {e}")
            traceback.print_exc()
            raise Exception("Failed to list files in GCS bucket.")