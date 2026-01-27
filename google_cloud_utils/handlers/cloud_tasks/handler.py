import json
import os
import threading
import traceback
from google.cloud import tasks_v2
from google.oauth2 import service_account
from google.auth import compute_engine
from google.protobuf import timestamp_pb2
import datetime


class CloudTasksHandler:
    """
    Handler for interaction with Google Cloud Tasks.
    
    Manages queue creation, task scheduling, and task listing.
    """
    _instance = None
    _lock = threading.Lock()

    def __new__(cls, *args, **kwargs):
        """Singleton pattern implementation."""
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super(CloudTasksHandler, cls).__new__(cls)
                    cls._instance._initialize(*args, **kwargs)
        return cls._instance

    def _initialize(self, serviceAccountJson: dict = None, client: tasks_v2.CloudTasksClient = None):
        """
        Initialize the CloudTasksHandler.

        Args:
            serviceAccountJson (dict, optional): Service account credentials info.
            client (tasks_v2.CloudTasksClient, optional): Existing Cloud Tasks client.

        Raises:
            Exception: If initialization error occurs.
        """
        print("Initializing Cloud Tasks Handler...")

        if hasattr(self, "client"):
            return

        try:
            # --------------------
            # Credentials
            # --------------------
            if serviceAccountJson:
                print("Using provided Cloud Tasks service account JSON")
                credentials = service_account.Credentials.from_service_account_info(
                    serviceAccountJson,
                    scopes=["https://www.googleapis.com/auth/cloud-platform"],
                )
                self.project_id = serviceAccountJson["project_id"]

            else:
                try:
                    print("Using Cloud Tasks service account JSON from env")
                    serviceAccountJson = json.loads(os.getenv("CLOUD_TASKS_SERVICE_ACCOUNT_JSON"))
                    credentials = service_account.Credentials.from_service_account_info(
                        serviceAccountJson,
                        scopes=["https://www.googleapis.com/auth/cloud-platform"],
                    )
                    self.project_id = serviceAccountJson["project_id"]

                except Exception:
                    print("Using default credentials for Cloud Tasks")
                    credentials = compute_engine.Credentials()
                    self.project_id = os.getenv("GCP_PROJECT")

            # --------------------
            # Client
            # --------------------
            self.client = client or tasks_v2.CloudTasksClient(credentials=credentials)

        except Exception as e:
            traceback.print_exc()
            raise Exception(f"Error initializing CloudTasksHandler: {e}")

    def queue_path(self, queue_name: str, location: str):
        return self.client.queue_path(self.project_id, location, queue_name)

    def create_queue(self, queue_name: str, location: str):
        parent = f"projects/{self.project_id}/locations/{location}"
        queue = {"name": self.queue_path(queue_name, location)}
        try:
            self.client.create_queue(parent=parent, queue=queue)
            return True
        except Exception as e:
            print(f"Error creating queue: {e}")
            traceback.print_exc()
            return False

    def create_task(
        self,
        queue_name: str,
        location: str,
        url: str,
        payload: dict = None,
        delay_seconds: int = None,
        service_account_email: str = None,
        http_method=tasks_v2.HttpMethod.POST
    ):
        parent = self.queue_path(queue_name, location)

        task = {
            "http_request": {
                "http_method": http_method,
                "url": url,
                "headers": {"Content-Type": "application/json"},
            }
        }

        if payload:
            task["http_request"]["body"] = json.dumps(payload).encode()

        if delay_seconds:
            d = datetime.datetime.utcnow() + datetime.timedelta(seconds=delay_seconds)
            timestamp = timestamp_pb2.Timestamp()
            timestamp.FromDatetime(d)
            task["schedule_time"] = timestamp

        if service_account_email:
            task["http_request"]["oidc_token"] = {
                "service_account_email": service_account_email
            }

        try:
            response = self.client.create_task(parent=parent, task=task)
            return response
        except Exception as e:
            print(f"Error creating task: {e}")
            traceback.print_exc()
            raise

    def list_tasks(self, queue_name: str, location: str):
        parent = self.queue_path(queue_name, location)
        try:
            tasks = self.client.list_tasks(parent=parent)
            return list(tasks)
        except Exception as e:
            print(f"Error listing tasks: {e}")
            traceback.print_exc()
            raise
