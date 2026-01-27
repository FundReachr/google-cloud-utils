import json
import traceback
import threading
import os
from google.cloud import firestore
from dotenv import load_dotenv

class FirestoreHandler:
    """
    A singleton handler for Google Cloud Firestore.
    Provides basic CRUD operations and manages the client lifecycle.
    """
    _instance = None
    _lock = threading.Lock()

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super(FirestoreHandler, cls).__new__(cls)
                    cls._instance._initialize(*args, **kwargs)
        return cls._instance

    def _initialize(self, serviceAccountJson: dict = None, client: firestore.Client = None):
        """
        Initialize the Firestore client.
        :param serviceAccountJson: Dictionary containing service account credentials.
        :param client: Optional existing firestore.Client instance.
        """
        if hasattr(self, "client"):
            return

        print("Initializing Firestore Handler...")
        try:
            if client:
                self.client = client
            elif serviceAccountJson:
                self.client = firestore.Client.from_service_account_info(serviceAccountJson)
            else:
                # Load from environment or use default credentials
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
            # Fallback to default credentials if possible
            self.client = firestore.Client()

    def get_document(self, collection: str, document_id: str):
        """Retrieve data from a specific document."""
        try:
            doc_ref = self.client.collection(collection).document(str(document_id))
            doc = doc_ref.get()
            return doc.to_dict() if doc.exists else None
        except Exception as e:
            print(f"Firestore Error (get): {e}")
            return None

    def set_document(self, collection: str, document_id: str, data: dict, merge: bool = True):
        """Write or update data to a specific document."""
        try:
            doc_ref = self.client.collection(collection).document(str(document_id))
            doc_ref.set(data, merge=merge)
            return True
        except Exception as e:
            print(f"Firestore Error (set): {e}")
            return False

    def delete_document(self, collection: str, document_id: str):
        """Delete a document."""
        try:
            self.client.collection(collection).document(str(document_id)).delete()
            return True
        except Exception as e:
            print(f"Firestore Error (delete): {e}")
            return False

    def query_collection(self, collection: str, filters: list = None, limit: int = None):
        """
        Query a collection with filters.
        :param filters: List of tuples (field, operator, value) e.g., [('status', '==', 'active')]
        """
        try:
            query = self.client.collection(collection)
            if filters:
                for field, op, val in filters:
                    query = query.where(field, op, val)
            if limit:
                query = query.limit(limit)
            
            docs = query.stream()
            return [doc.to_dict() for doc in docs]
        except Exception as e:
            print(f"Firestore Error (query): {e}")
            return []
