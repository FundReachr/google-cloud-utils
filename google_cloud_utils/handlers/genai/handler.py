import json
import os
import threading
import traceback
from typing import Any, Dict, Optional

import google.auth
from dotenv import load_dotenv
from google.auth import compute_engine
from google.auth.transport.requests import Request
from google.oauth2 import service_account

load_dotenv()

_VERTEX_AI_SCOPE = "https://www.googleapis.com/auth/cloud-platform"


class GenAIHandler:
    """Singleton handler for Google GenAI / Vertex AI credentials.

    Holds a dedicated service-account for Vertex AI calls, separate from the
    Cloud Storage service-account.  Exposes:
      - ``genai_credentials`` — scoped SA credentials ready for ``genai.Client``
      - ``get_client(project, location)`` — factory that builds a ``google.genai.Client``
    """

    _instance = None
    _lock = threading.Lock()

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super(GenAIHandler, cls).__new__(cls)
                    cls._instance._initialize(*args, **kwargs)
        return cls._instance

    def _initialize(self, serviceAccountJson: Optional[Dict[str, Any]] = None) -> None:
        print("Initializing GenAI Handler...")
        if hasattr(self, "genai_credentials"):
            return
        try:
            if serviceAccountJson:
                print("GenAI Handler: using provided service account JSON")
                self.genai_credentials = service_account.Credentials.from_service_account_info(
                    serviceAccountJson,
                    scopes=[_VERTEX_AI_SCOPE],
                )
            else:
                try:
                    env_json = os.getenv("GENAI_SERVICE_ACCOUNT_JSON")
                    if not env_json:
                        raise ValueError("GENAI_SERVICE_ACCOUNT_JSON is not set")
                    print("GenAI Handler: using environment variable for service account JSON")
                    sa_info = json.loads(env_json)
                    self.genai_credentials = service_account.Credentials.from_service_account_info(
                        sa_info,
                        scopes=[_VERTEX_AI_SCOPE],
                    )
                except Exception:
                    print("GenAI Handler: falling back to application default credentials (gcloud auth application-default login)")
                    self.genai_credentials, _ = google.auth.default(scopes=[_VERTEX_AI_SCOPE])
        except Exception as e:
            traceback.print_exc()
            raise Exception(f"Error initializing GenAI credentials: {e}")

    def get_client(self, project: str, location: str):
        """Return a ``google.genai.Client`` backed by the service-account credentials.

        Args:
            project: GCP project ID.
            location: Vertex AI region (e.g. ``"europe-west1"``).

        Returns:
            google.genai.Client: Authenticated Vertex AI GenAI client.
        """
        from google import genai
        return genai.Client(
            vertexai=True,
            project=project,
            location=location,
            credentials=self.genai_credentials,
        )
