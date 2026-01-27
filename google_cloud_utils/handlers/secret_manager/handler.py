from google.oauth2 import service_account
from google.auth import compute_engine
from google.cloud import secretmanager
import json
import os
from dotenv import load_dotenv
import threading
import traceback

load_dotenv()

class SecretManagerHandler:
    _instance = None
    _lock = threading.Lock()

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            with cls._lock:  # Prevent
                if cls._instance is None:
                    cls._instance = super(SecretManagerHandler, cls).__new__(cls)
                    cls._instance._initialize(*args, **kwargs)  
        return cls._instance

    def _initialize(self, serviceAccountJson: dict = None):
        """Initialize only once in a thread-safe way."""
        print("Initializing Secret Manager Handler...")
        if hasattr(self, "client"):
            return # Already initialized

        try:
            if serviceAccountJson:
                print("Using provided service account JSON")
                self.project_id = serviceAccountJson.get('project_id', None)
                self.client = secretmanager.SecretManagerServiceClient.from_service_account_info(serviceAccountJson)
            else:
                traceback.print_exc()
                try:
                    load_dotenv()
                    print("Using service account JSON from environment variable")
                    credentials = json.loads(os.getenv("SECRET_MANAGER_SERVICE_ACCOUNT_JSON"))
                    self.project_id = credentials.get('project_id', None)
                    self.client = secretmanager.SecretManagerServiceClient.from_service_account_info(credentials)
                except Exception:
                    try:
                        if os.path.exists('var/secret_manager_service_account.json'):
                            print("Using service account JSON from file")
                            self.project_id = json.loads(open('var/secret_manager_service_account.json').read()).get('project_id', None)
                            self.client = secretmanager.SecretManagerServiceClient.from_service_account_json('var/secret_manager_service_account.json')
                    except Exception:
                        print("Using default credentials")
                        traceback.print_exc()
                        credentials = compute_engine.Credentials()
                        self.project_id = compute_engine.Credentials()._quota_project_id
                        self.client = secretmanager.SecretManagerServiceClient(credentials=credentials)
        except Exception as e:
            traceback.print_exc()
            raise Exception(f"Error initializing GCP credentials: {e}")

    def get_secret(self, secret_name: str):
        """Retrieve a secret from Google Cloud Secret Manager."""
        try:
            name = f"projects/{self.project_id}/secrets/{secret_name}/versions/latest"
            response = self.client.access_secret_version(name=name)
            secret_data = response.payload.data.decode('UTF-8')
            return json.loads(secret_data)
        except Exception as e:
            traceback.print_exc()
            return None