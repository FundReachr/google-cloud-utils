from __future__ import annotations

import json
import logging
import os
import threading
from pathlib import Path

import google.auth
from dotenv import load_dotenv
from google.cloud import secretmanager

load_dotenv()

logger = logging.getLogger(__name__)

# src/bulq_commons/cloud/secrets.py → parents[3] = repo root
REPO_ROOT = Path(__file__).resolve().parents[3]
_SECRET_MANAGER_JSON = REPO_ROOT / "var" / "secret_manager_service_account.json"


class SecretManagerHandler:
    _instance = None
    _lock = threading.Lock()

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super(SecretManagerHandler, cls).__new__(cls)
                    cls._instance._initialize(*args, **kwargs)
        return cls._instance

    def _initialize(self, serviceAccountJson: dict | None = None) -> None:
        if hasattr(self, "client"):
            logger.debug("SecretManagerHandler already initialized, skipping")
            return

        logger.info("Initializing SecretManagerHandler")

        if serviceAccountJson:
            logger.info("Initializing Secret Manager client via provided serviceAccountJson")
            self._init_from_service_account_info(serviceAccountJson)
            logger.info(f"Successfully initialized Secret Manager client via serviceAccountJson with project_id: {self.project_id}")
            return

        env_json = os.getenv("SECRET_MANAGER_SERVICE_ACCOUNT_JSON")
        if env_json:
            logger.info("Attempting Secret Manager client initialization via env var: SECRET_MANAGER_SERVICE_ACCOUNT_JSON")
            try:
                credentials = json.loads(env_json)
            except json.JSONDecodeError as e:
                logger.exception(f"Invalid JSON in SECRET_MANAGER_SERVICE_ACCOUNT_JSON: {e}")
                raise ValueError(
                    "SECRET_MANAGER_SERVICE_ACCOUNT_JSON is set but is not valid JSON"
                ) from e
            self._init_from_service_account_info(credentials)
            logger.info(f"Successfully initialized Secret Manager client via env var with project_id: {self.project_id}")
            return

        if _SECRET_MANAGER_JSON.is_file():
            logger.info(f"Initializing Secret Manager client via file: {_SECRET_MANAGER_JSON}")
            with _SECRET_MANAGER_JSON.open(encoding="utf-8") as f:
                credentials = json.load(f)
            self.project_id = credentials.get("project_id")
            self.client = secretmanager.SecretManagerServiceClient.from_service_account_file(
                str(_SECRET_MANAGER_JSON)
            )
            logger.info(f"Successfully initialized Secret Manager client via file with project_id: {self.project_id}")
            return

        logger.info("Initializing Secret Manager client via compute engine default credentials")
        credentials, project_id = google.auth.default(
            scopes=["https://www.googleapis.com/auth/cloud-platform"],
        )
        self.project_id = project_id
        self.client = secretmanager.SecretManagerServiceClient(credentials=credentials)
        logger.info(f"Successfully initialized Secret Manager client via compute engine default credentials with project_id: {self.project_id}")

    def _init_from_service_account_info(self, service_account: dict) -> None:
        self.project_id = service_account.get("project_id")
        self.client = secretmanager.SecretManagerServiceClient.from_service_account_info(
            service_account
        )

    def get_secret(self, secret_name: str) -> str:
        """Retrieve a secret from Google Cloud Secret Manager.

        Accepts either:
        - A short name (e.g. ``"my-secret"``) — builds the full path from ``self.project_id``
        - A fully-qualified name (e.g. ``"projects/123/secrets/my-secret/versions/latest"``)
        """
        if secret_name.startswith("projects/"):
            name = secret_name
        elif self.project_id:
            name = f"projects/{self.project_id}/secrets/{secret_name}/versions/latest"
        else:
            raise RuntimeError(
                f"Cannot resolve secret '{secret_name}': no GCP project_id on "
                "SecretManagerHandler (set via service account JSON or "
                "`gcloud config set project` when using application-default credentials)"
            )

        try:
            response = self.client.access_secret_version(name=name)
            return response.payload.data.decode("UTF-8")
        except Exception as e:
            raise RuntimeError(
                f"Failed to retrieve secret '{secret_name}' from Secret Manager "
                f"(resource: {name}): {e}"
            ) from e
