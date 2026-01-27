import json
import traceback
import threading
import os
from google.cloud import datastore
from dotenv import load_dotenv

class DatastoreHandler:
    """
    A singleton handler for Google Cloud Datastore (Firestore in Datastore Mode).
    Provides basic CRUD operations and manages the client lifecycle.
    """
    _instance = None
    _lock = threading.Lock()

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super(DatastoreHandler, cls).__new__(cls)
                    cls._instance._initialize(*args, **kwargs)
        return cls._instance

    def _initialize(self, serviceAccountJson: dict = None, client: datastore.Client = None):
        """
        Initialize the Datastore client.
        :param serviceAccountJson: Dictionary containing service account credentials.
        :param client: Optional existing datastore.Client instance.
        """
        if hasattr(self, "client"):
            return

        print("Initializing Datastore Handler...")
        try:
            if client:
                self.client = client
            elif serviceAccountJson:
                self.client = datastore.Client.from_service_account_info(serviceAccountJson)
            else:
                # Load from environment or use default credentials
                load_dotenv()
                env_creds = os.getenv("FIRESTORE_SERVICE_ACCOUNT_JSON")
                if env_creds:
                    credentials = json.loads(env_creds)
                    self.client = datastore.Client.from_service_account_info(credentials)
                else:
                    self.client = datastore.Client()
        except Exception as e:
            print(f"Failed to initialize Datastore: {e}")
            traceback.print_exc()
            self.client = datastore.Client()

    def get_document(self, collection: str, document_id: str):
        """Retrieve data from a specific entity (document)."""
        try:
            key = self.client.key(collection, str(document_id))
            entity = self.client.get(key)
            return dict(entity) if entity else None
        except Exception as e:
            print(f"Datastore Error (get): {e}")
            return None

    def set_document(self, collection: str, document_id: str, data: dict, merge: bool = True):
        """Write or update data to a specific entity (document)."""
        try:
            key = self.client.key(collection, str(document_id))
            
            # Start with an empty entity or fetch existing
            if merge:
                entity = self.client.get(key)
                if not entity:
                    entity = datastore.Entity(key=key)
            else:
                entity = datastore.Entity(key=key)
            
            # Update entity with new data
            entity.update(data)

            # Datastore has a 1500-byte limit for indexed string properties.
            # We must explicitly exclude large strings and complex objects from indexing.
            # We scan the ENTIRE entity to ensure legacy or merged data doesn't trigger the limit.
            exclude = list(entity.exclude_from_indexes)
            
            for k, v in entity.items():
                if isinstance(v, str) and len(v.encode('utf-8')) > 1500:
                    if k not in exclude:
                        exclude.append(k)
                elif isinstance(v, (dict, list)):
                    if k not in exclude:
                        exclude.append(k)

            entity.exclude_from_indexes = tuple(exclude)
            
            self.client.put(entity)
            return True
        except Exception as e:
            print(f"Datastore Error (set): {e}")
            return False

    def delete_document(self, collection: str, document_id: str):
        """Delete an entity."""
        try:
            key = self.client.key(collection, str(document_id))
            self.client.delete(key)
            return True
        except Exception as e:
            print(f"Datastore Error (delete): {e}")
            return False

    def query_collection(self, collection: str, filters: list = None, limit: int = None):
        """
        Query a kind (collection) with filters.
        :param filters: List of tuples (field, operator, value) e.g., [('status', '=', 'active')]
        Note: Datastore uses '=' instead of '=='
        """
        try:
            query = self.client.query(kind=collection)
            if filters:
                for field, op, val in filters:
                    # Map common operators if needed
                    datastore_op = '=' if op == '==' else op
                    query.add_filter(field, datastore_op, val)
            if limit:
                docs = list(query.fetch(limit=limit))
            else:
                docs = list(query.fetch())
            
            return [dict(doc) for doc in docs]
        except Exception as e:
            print(f"Datastore Error (query): {e}")
            return []
    def clear_collection(self, collection: str):
        """Delete all entities in a collection (kind)."""
        try:
            print(f"Clearing collection: {collection}")
            query = self.client.query(kind=collection)
            query.keys_only()
            keys = list(query.fetch())
            if keys:
                # Delete in batches of 500 (Datastore limit)
                for i in range(0, len(keys), 500):
                    batch = keys[i:i+500]
                    self.client.delete_multi(batch)
                print(f"Deleted {len(keys)} entities from {collection}.")
            else:
                print(f"Collection {collection} is already empty.")
            return True
        except Exception as e:
            print(f"Datastore Error (clear_collection): {e}")
            return False
