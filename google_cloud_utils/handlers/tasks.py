from __future__ import annotations

import datetime
import json
import os
import threading
import traceback

from dotenv import load_dotenv
from google.auth import compute_engine
from google.cloud import tasks_v2
from google.oauth2 import service_account
from google.protobuf import timestamp_pb2

load_dotenv()


class CloudTasksHandler:
    """Handler for interaction with Google Cloud Tasks."""

    _instance = None
    _lock = threading.Lock()

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super(CloudTasksHandler, cls).__new__(cls)
                    cls._instance._initialize(*args, **kwargs)
        return cls._instance

    def _initialize(
        self,
        serviceAccountJson: dict | None = None,
        client: tasks_v2.CloudTasksClient | None = None,
    ):
        print("Initializing Cloud Tasks Handler...")

        if hasattr(self, "client"):
            return

        try:
            if serviceAccountJson:
                print("Using provided Cloud Tasks service account JSON")
                credentials = service_account.Credentials.from_service_account_info(
                    serviceAccountJson,
                    scopes=["https://www.googleapis.com/auth/cloud-platform"],
                )
                self.project_id = serviceAccountJson["project_id"]
                self.credentials = credentials
            else:
                try:
                    print("Using Cloud Tasks service account JSON from env")
                    serviceAccountJson = json.loads(os.getenv("CLOUD_TASKS_SERVICE_ACCOUNT_JSON"))  # type: ignore
                    credentials = service_account.Credentials.from_service_account_info(
                        serviceAccountJson,
                        scopes=["https://www.googleapis.com/auth/cloud-platform"],
                    )
                    self.project_id = serviceAccountJson["project_id"]
                    self.credentials = credentials
                except Exception:
                    print("Using default credentials for Cloud Tasks")
                    credentials = compute_engine.Credentials()
                    self.project_id = os.getenv("GCP_PROJECT")
                    self.credentials = credentials

            self.client = client or tasks_v2.CloudTasksClient(credentials=credentials)

        except Exception as e:
            traceback.print_exc()
            raise Exception(f"Error initializing CloudTasksHandler: {e}")

    def queue_path(self, queue_name: str, location: str) -> str:
        return self.client.queue_path(self.project_id, location, queue_name)  # type: ignore

    def create_queue(self, queue_name: str, location: str) -> bool:
        parent = f"projects/{self.project_id}/locations/{location}"
        queue = {"name": self.queue_path(queue_name, location)}
        try:
            self.client.create_queue(parent=parent, queue=queue)  # type: ignore
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
        payload: dict | None = None,
        delay_seconds: int | None = None,
        service_account_email: str | None = None,
        http_method=tasks_v2.HttpMethod.POST,
        http_headers: dict = {"Content-Type": "application/json"},
    ):
        parent = self.queue_path(queue_name, location)

        task = {
            "http_request": {
                "http_method": http_method,
                "url": url,
                "headers": http_headers,
            }
        }

        if payload:
            task["http_request"]["body"] = json.dumps(payload).encode()

        if delay_seconds:
            d = datetime.datetime.utcnow() + datetime.timedelta(seconds=delay_seconds)  # type: ignore
            timestamp = timestamp_pb2.Timestamp()  # type: ignore
            timestamp.FromDatetime(d)
            task["schedule_time"] = timestamp

        if service_account_email:
            task["http_request"]["oidc_token"] = {
                "service_account_email": service_account_email
            }
        else:
            task["http_request"]["oidc_token"] = {
                "service_account_email": self.credentials.service_account_email
            }

        try:
            return self.client.create_task(parent=parent, task=task)  # type: ignore
        except Exception as e:
            print(f"Error creating task: {e}")
            traceback.print_exc()
            raise

    def list_tasks(self, queue_name: str, location: str):
        parent = self.queue_path(queue_name, location)
        try:
            return list(self.client.list_tasks(parent=parent))
        except Exception as e:
            print(f"Error listing tasks: {e}")
            traceback.print_exc()
            raise

    def create_task_unique(
        self,
        queue_name: str,
        location: str,
        url: str,
        payload: dict | None = None,
        service_account_email: str | None = None,
        delay_seconds: int | None = None,
        http_method=tasks_v2.HttpMethod.POST,
        http_headers: dict = {"Content-Type": "application/json"},
    ):
        existing_tasks = self.list_tasks(queue_name, location)
        for task in existing_tasks:
            encoded_payload = task.http_request.body.decode()
            if encoded_payload and json.loads(encoded_payload) == payload:
                print(f"Duplicate task found for payload={payload}. Skipping creation.")
                return
        self.create_task(
            queue_name=queue_name,
            location=location,
            url=url,
            payload=payload,
            service_account_email=service_account_email or self.credentials.service_account_email,
            delay_seconds=delay_seconds,
            http_method=http_method,
            http_headers=http_headers,
        )
