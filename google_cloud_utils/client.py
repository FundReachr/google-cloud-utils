from .handlers import *
import threading
import json 

class GoogleCloudHandler:
    """
    Main client for Google Cloud Utilities with Lazy Initialization.
    """
    _instance = None
    _lock = threading.Lock()

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super(GoogleCloudHandler, cls).__new__(cls)
                    cls._instance._setup_lazy_loading(**kwargs)
        return cls._instance

    def _setup_lazy_loading(self, **kwargs):
        """Store config and prepare locks for lazy initialization."""
        self._init_kwargs = kwargs
        self._handler_locks = {
            "secret": threading.Lock(),
            "storage": threading.Lock(),
            "bigquery": threading.Lock(),
            "scheduler": threading.Lock(),
            "datastore": threading.Lock(),
            "pubsub": threading.Lock(),
            "tasks": threading.Lock(),
            "functions": threading.Lock(),
            "genai": threading.Lock(),
        }
        
        # Internal private storage for instances
        self._secret_manager_handler = None
        self._cloud_storage_handler = None
        self._big_query_handler = None
        self._cloud_scheduler_handler = None
        self._datastore_handler = None
        self._pub_sub_handler = None
        self._cloud_tasks_handler = None
        self._cloud_function_handler = None
        self._gen_ai_handler = None

    # --- Lazy Properties ---

    @property
    def secretManagerHandler(self):
        if self._secret_manager_handler is None:
            with self._handler_locks["secret"]:
                if self._secret_manager_handler is None:
                    print("Lazy Initializing SecretManagerHandler...")
                    self._secret_manager_handler = SecretManagerHandler(
                        serviceAccountJson=self._init_kwargs.get("secretManagerServiceAccount")
                    )
        return self._secret_manager_handler

    @property
    def cloudStorageHandler(self):
        if self._cloud_storage_handler is None:
            with self._handler_locks["storage"]:
                if self._cloud_storage_handler is None:
                    print("Lazy Initializing CloudStorageHandler...")
                    creds = self._init_kwargs.get("cloudStorageServiceAccountJson") or \
                            self.secretManagerHandler.get_secret("cloud-storage-admin-service-account")
                    creds = json.loads(creds) if isinstance(creds, str) else creds
                    self._cloud_storage_handler = CloudStorageHandler(serviceAccountJson=creds)
        return self._cloud_storage_handler

    @property
    def bigQueryHandler(self):
        if self._big_query_handler is None:
            with self._handler_locks["bigquery"]:
                if self._big_query_handler is None:
                    print("Lazy Initializing BigQueryHandler...")
                    creds = self._init_kwargs.get("bigQueryServiceAccountJson") or \
                            self.secretManagerHandler.get_secret("bigquery-admin-service-account")
                    creds = json.loads(creds) if isinstance(creds, str) else creds
                    self._big_query_handler = BigQueryHandler(serviceAccountJson=creds)
        return self._big_query_handler

    @property
    def cloudSchedulerHandler(self):
        if self._cloud_scheduler_handler is None:
            with self._handler_locks["scheduler"]:
                if self._cloud_scheduler_handler is None:
                    print("Lazy Initializing CloudSchedulerHandler...")
                    creds = self._init_kwargs.get("cloudSchedulerServiceAccountJson") or \
                            self.secretManagerHandler.get_secret("cloud-scheduler-admin-service-account")
                    creds = json.loads(creds) if isinstance(creds, str) else creds
                    self._cloud_scheduler_handler = CloudSchedulerHandler(serviceAccountJson=creds)
        return self._cloud_scheduler_handler

    @property
    def datastoreHandler(self):
        if self._datastore_handler is None:
            with self._handler_locks["datastore"]:
                if self._datastore_handler is None:
                    print("Lazy Initializing DatastoreHandler...")
                    creds = self._init_kwargs.get("datastoreServiceAccountJson") or \
                            self.secretManagerHandler.get_secret("firestore-admin-service-account")
                    creds = json.loads(creds) if isinstance(creds, str) else creds
                    self._datastore_handler = DatastoreHandler(serviceAccountJson=creds)
        return self._datastore_handler

    @property
    def pubSubHandler(self):
        if self._pub_sub_handler is None:
            with self._handler_locks["pubsub"]:
                if self._pub_sub_handler is None:
                    print("Lazy Initializing PubSubHandler...")
                    creds = self._init_kwargs.get("pubSubServiceAccountJson") or \
                            self.secretManagerHandler.get_secret("pubsub-admin-service-account")
                    creds = json.loads(creds) if isinstance(creds, str) else creds
                    self._pub_sub_handler = PubSubHandler(serviceAccountJson=creds)
        return self._pub_sub_handler

    @property
    def cloudTasksHandler(self):
        if self._cloud_tasks_handler is None:
            with self._handler_locks["tasks"]:
                if self._cloud_tasks_handler is None:
                    print("Lazy Initializing CloudTasksHandler...")
                    creds = self._init_kwargs.get("cloudTasksServiceAccountJson") or \
                            self.secretManagerHandler.get_secret("cloud-tasks-admin-service-account")
                    creds = json.loads(creds) if isinstance(creds, str) else creds
                    self._cloud_tasks_handler = CloudTasksHandler(serviceAccountJson=creds)
        return self._cloud_tasks_handler

    @property
    def cloudFunctionHandler(self):
        if self._cloud_function_handler is None:
            with self._handler_locks["functions"]:
                if self._cloud_function_handler is None:
                    print("Lazy Initializing CloudFunctionHandler...")
                    creds = self._init_kwargs.get("cloudFunctionsServiceAccountJson") or \
                            self.secretManagerHandler.get_secret("cloud-function-admin-service-account")
                    creds = json.loads(creds) if isinstance(creds, str) else creds
                    self._cloud_function_handler = CloudFunctionHandler(serviceAccountJson=creds)
        return self._cloud_function_handler

    @property
    def genAIHandler(self):
        if self._gen_ai_handler is None:
            with self._handler_locks["genai"]:
                if self._gen_ai_handler is None:
                    print("Lazy Initializing GenAIHandler...")
                    creds = self._init_kwargs.get("genaiServiceAccountJson") or \
                            self.secretManagerHandler.get_secret("genai-user-service-account")
                    creds = json.loads(creds) if isinstance(creds, str) else creds
                    self._gen_ai_handler = GenAIHandler(serviceAccountJson=creds)
        return self._gen_ai_handler