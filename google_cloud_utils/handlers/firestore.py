from __future__ import annotations

import json
import os
import threading
from typing import Any
import traceback

from dotenv import load_dotenv
from google.cloud import firestore

load_dotenv()


class FirestoreHandler:
    """Singleton handler for Google Cloud Firestore."""

    _instance = None
    _lock = threading.Lock()

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super(FirestoreHandler, cls).__new__(cls)
                    cls._instance._initialize(*args, **kwargs)
        return cls._instance

    def _initialize(
        self,
        serviceAccountJson: dict | None = None,
        client: firestore.Client | None = None,
    ):
        if hasattr(self, "client"):
            return

        print("Initializing Firestore Handler...")
        try:
            if client:
                self.client = client
            elif serviceAccountJson:
                self.client = firestore.Client.from_service_account_info(serviceAccountJson)
            else:
                load_dotenv()
                env_creds = os.getenv("FIRESTORE_SERVICE_ACCOUNT_JSON")
                if env_creds:
                    credentials = json.loads(env_creds)
                    self.client = firestore.Client.from_service_account_info(credentials)
                else:
                    self.client = firestore.Client()
        except Exception as e:
            print(f"Failed to initialize Firestore: {e}")
            traceback.print_exc()
            self.client = firestore.Client()

    def get_document(self, collection: str, document_id: str):
        try:
            doc = self.client.collection(collection).document(str(document_id)).get()
            return doc.to_dict() if doc.exists else None  # type: ignore
        except Exception as e:
            print(f"Firestore Error (get): {e}")
            return None

    def set_document(
        self, collection: str, document_id: str, data: dict, merge: bool = True
    ):
        try:
            self.client.collection(collection).document(str(document_id)).set(data, merge=merge)
            return True
        except Exception as e:
            print(f"Firestore Error (set): {e}")
            return False

    def delete_document(self, collection: str, document_id: str):
        try:
            self.client.collection(collection).document(str(document_id)).delete()
            return True
        except Exception as e:
            print(f"Firestore Error (delete): {e}")
            return False

    def query_collection(
        self,
        collection: str,
        filters: list[tuple[str, str, Any]] | None = None,
        limit: int | None = None,
    ) -> list[dict[str, Any] | None]:
        try:
            query = self.client.collection(collection)
            if filters:
                for field, op, val in filters:
                    query = query.where(field, op, val)
            if limit:
                query = query.limit(limit)
            return [doc.to_dict() for doc in query.stream()]
        except Exception as e:
            print(f"Firestore Error (query): {e}")
            return []

    def upsert_document(self, collection: str, document_id: str, data: dict):
        return self.set_document(collection, document_id, data, merge=True)
