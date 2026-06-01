from .gcs import CloudStorageHandler
from .secrets import SecretManagerHandler
from .bigquery import BigQueryHandler
from .pubsub import PubSubHandler
from .tasks import CloudTasksHandler
from .datastore import DatastoreHandler
from .firestore import FirestoreHandler
from .cloud_scheduler import CloudSchedulerHandler
from .cloud_function import CloudFunctionHandler
from .genai import GenAIHandler
from .sheets import SheetsHandler
from .cloud_run import CloudRunJobHandler
import threading
import json
import logging

logger = logging.getLogger(__name__)

class _GoogleCloudHandlerBase:
    """
    Shared lazy-loading implementation for the Google Cloud client.

    This base class holds all of the lazy-initialized service handlers. It is
    not a singleton on its own — construction always produces a fresh instance.
    The public variants are :class:`GoogleCloudHandler` (singleton, the default)
    and :class:`GoogleCloudHandlerNonSingleton` (a new instance per call).
    """

    # Whether the lazily-built service handlers should themselves be
    # process-wide singletons. The non-singleton client overrides this to
    # False so each client owns fresh, independently-credentialed handlers
    # instead of sharing the global handler singletons.
    _handler_singleton = True

    def __init__(self, **kwargs):
        self._setup_lazy_loading(**kwargs)

    def _setup_lazy_loading(self, **kwargs):
        """Store config and prepare locks for lazy initialization."""
        self._init_kwargs = kwargs
        self._handler_locks = {
            "secret": threading.Lock(),
            "storage": threading.Lock(),
            "bigquery": threading.Lock(),
            "scheduler": threading.Lock(),
            "datastore": threading.Lock(),
            "firestore": threading.Lock(),
            "pubsub": threading.Lock(),
            "tasks": threading.Lock(),
            "functions": threading.Lock(),
            "genai": threading.Lock(),
            "sheets": threading.Lock(),
            "cloud_run": threading.Lock(),
        }

        # Internal private storage for instances
        self._secret_manager_handler = None
        self._cloud_storage_handler = None
        self._big_query_handler = None
        self._cloud_scheduler_handler = None
        self._datastore_handler = None
        self._firestore_handler = None
        self._pub_sub_handler = None
        self._cloud_tasks_handler = None
        self._cloud_function_handler = None
        self._gen_ai_handler = None
        self._sheets_handler = None
        self._cloud_run_job_handler = None

    # --- Lazy Properties ---

    @property
    def secretManagerHandler(self):
        if self._secret_manager_handler is None:
            with self._handler_locks["secret"]:
                if self._secret_manager_handler is None:
                    logger.info("Lazy loading SecretManagerHandler")
                    try:
                        self._secret_manager_handler = SecretManagerHandler(
                            serviceAccountJson=self._init_kwargs.get("secretManagerServiceAccount"),
                            singleton=self._handler_singleton,
                        )
                        logger.info("SecretManagerHandler lazy loaded successfully")
                    except Exception as e:
                        logger.exception(f"Failed to lazy load SecretManagerHandler: {e}")
                        raise
        return self._secret_manager_handler

    @property
    def cloudStorageHandler(self):
        if self._cloud_storage_handler is None:
            with self._handler_locks["storage"]:
                if self._cloud_storage_handler is None:
                    logger.info("Lazy loading CloudStorageHandler")
                    try:
                        if self._init_kwargs.get("cloudStorageServiceAccountJson"):
                            logger.info("CloudStorageHandler: using credential from init kwargs")
                            creds = self._init_kwargs.get("cloudStorageServiceAccountJson")
                        else:
                            logger.info("CloudStorageHandler: fetching credential from Secret Manager (cloud-storage-admin-service-account)")
                            creds = self.secretManagerHandler.get_secret("cloud-storage-admin-service-account")
                        creds = json.loads(creds) if isinstance(creds, str) else creds
                        self._cloud_storage_handler = CloudStorageHandler(serviceAccountJson=creds, singleton=self._handler_singleton)
                        logger.info("CloudStorageHandler lazy loaded successfully")
                    except Exception as e:
                        logger.exception(f"Failed to lazy load CloudStorageHandler: {e}")
                        raise
        return self._cloud_storage_handler

    @property
    def bigQueryHandler(self):
        if self._big_query_handler is None:
            with self._handler_locks["bigquery"]:
                if self._big_query_handler is None:
                    logger.info("Lazy loading BigQueryHandler")
                    try:
                        if self._init_kwargs.get("bigQueryServiceAccountJson"):
                            logger.info("BigQueryHandler: using credential from init kwargs")
                            creds = self._init_kwargs.get("bigQueryServiceAccountJson")
                        else:
                            logger.info("BigQueryHandler: fetching credential from Secret Manager (bigquery-admin-service-account)")
                            creds = self.secretManagerHandler.get_secret("bigquery-admin-service-account")
                        creds = json.loads(creds) if isinstance(creds, str) else creds
                        self._big_query_handler = BigQueryHandler(serviceAccountJson=creds, singleton=self._handler_singleton)
                        logger.info("BigQueryHandler lazy loaded successfully")
                    except Exception as e:
                        logger.exception(f"Failed to lazy load BigQueryHandler: {e}")
                        raise
        return self._big_query_handler

    @property
    def cloudSchedulerHandler(self):
        if self._cloud_scheduler_handler is None:
            with self._handler_locks["scheduler"]:
                if self._cloud_scheduler_handler is None:
                    logger.info("Lazy loading CloudSchedulerHandler")
                    try:
                        if self._init_kwargs.get("cloudSchedulerServiceAccountJson"):
                            logger.info("CloudSchedulerHandler: using credential from init kwargs")
                            creds = self._init_kwargs.get("cloudSchedulerServiceAccountJson")
                        else:
                            logger.info("CloudSchedulerHandler: fetching credential from Secret Manager (cloud-scheduler-admin-service-account)")
                            creds = self.secretManagerHandler.get_secret("cloud-scheduler-admin-service-account")
                        creds = json.loads(creds) if isinstance(creds, str) else creds
                        self._cloud_scheduler_handler = CloudSchedulerHandler(serviceAccountJson=creds)  # type: ignore
                        logger.info("CloudSchedulerHandler lazy loaded successfully")
                    except Exception as e:
                        logger.exception(f"Failed to lazy load CloudSchedulerHandler: {e}")
                        raise
        return self._cloud_scheduler_handler

    @property
    def datastoreHandler(self):
        if self._datastore_handler is None:
            with self._handler_locks["datastore"]:
                if self._datastore_handler is None:
                    logger.info("Lazy loading DatastoreHandler")
                    try:
                        if self._init_kwargs.get("datastoreServiceAccountJson"):
                            logger.info("DatastoreHandler: using credential from init kwargs")
                            creds = self._init_kwargs.get("datastoreServiceAccountJson")
                        else:
                            logger.info("DatastoreHandler: fetching credential from Secret Manager (firestore-admin-service-account)")
                            creds = self.secretManagerHandler.get_secret("firestore-admin-service-account")
                        creds = json.loads(creds) if isinstance(creds, str) else creds
                        self._datastore_handler = DatastoreHandler(serviceAccountJson=creds, singleton=self._handler_singleton)
                        logger.info("DatastoreHandler lazy loaded successfully")
                    except Exception as e:
                        logger.exception(f"Failed to lazy load DatastoreHandler: {e}")
                        raise
        return self._datastore_handler

    @property
    def pubSubHandler(self):
        if self._pub_sub_handler is None:
            with self._handler_locks["pubsub"]:
                if self._pub_sub_handler is None:
                    logger.info("Lazy loading PubSubHandler")
                    try:
                        if self._init_kwargs.get("pubSubServiceAccountJson"):
                            logger.info("PubSubHandler: using credential from init kwargs")
                            creds = self._init_kwargs.get("pubSubServiceAccountJson")
                        else:
                            logger.info("PubSubHandler: fetching credential from Secret Manager (pubsub-admin-service-account)")
                            creds = self.secretManagerHandler.get_secret("pubsub-admin-service-account")
                        creds = json.loads(creds) if isinstance(creds, str) else creds
                        self._pub_sub_handler = PubSubHandler(serviceAccountJson=creds, singleton=self._handler_singleton)
                        logger.info("PubSubHandler lazy loaded successfully")
                    except Exception as e:
                        logger.exception(f"Failed to lazy load PubSubHandler: {e}")
                        raise
        return self._pub_sub_handler

    @property
    def cloudTasksHandler(self):
        if self._cloud_tasks_handler is None:
            with self._handler_locks["tasks"]:
                if self._cloud_tasks_handler is None:
                    logger.info("Lazy loading CloudTasksHandler")
                    try:
                        if self._init_kwargs.get("cloudTasksServiceAccountJson"):
                            logger.info("CloudTasksHandler: using credential from init kwargs")
                            creds = self._init_kwargs.get("cloudTasksServiceAccountJson")
                        else:
                            logger.info("CloudTasksHandler: fetching credential from Secret Manager (cloud-tasks-admin-service-account)")
                            creds = self.secretManagerHandler.get_secret("cloud-tasks-admin-service-account")
                        creds = json.loads(creds) if isinstance(creds, str) else creds
                        self._cloud_tasks_handler = CloudTasksHandler(serviceAccountJson=creds, singleton=self._handler_singleton)
                        logger.info("CloudTasksHandler lazy loaded successfully")
                    except Exception as e:
                        logger.exception(f"Failed to lazy load CloudTasksHandler: {e}")
                        raise
        return self._cloud_tasks_handler

    @property
    def cloudFunctionHandler(self):
        if self._cloud_function_handler is None:
            with self._handler_locks["functions"]:
                if self._cloud_function_handler is None:
                    print("Lazy Initializing CloudFunctionHandler...")
                    creds = self._init_kwargs.get("cloudFunctionServiceAccountJson") or \
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
                    creds = self._init_kwargs.get("genAIServiceAccountJson") or \
                            self.secretManagerHandler.get_secret("genai-user-service-account")
                    creds = json.loads(creds) if isinstance(creds, str) else creds
                    self._gen_ai_handler = GenAIHandler(serviceAccountJson=creds, singleton=self._handler_singleton)
        return self._gen_ai_handler

    @property
    def cloudRunJobHandler(self):
        if self._cloud_run_job_handler is None:
            with self._handler_locks["cloud_run"]:
                if self._cloud_run_job_handler is None:
                    print("Lazy Initializing CloudRunJobHandler...")
                    creds =	self._init_kwargs.get("cloudRunServiceAccountJson") or \
                            self.secretManagerHandler.get_secret("app-builder-service-account")
                    if creds is not None:
                        creds = json.loads(creds) if isinstance(creds, str) else creds
                    self._cloud_run_job_handler = CloudRunJobHandler(serviceAccountJson=creds)
        return self._cloud_run_job_handler

    @property
    def sheetsHandler(self):
        if self._sheets_handler is None:
            with self._handler_locks["sheets"]:
                if self._sheets_handler is None:
                    print("Lazy Initializing SheetsHandler...")
                    creds = self._init_kwargs.get("sheetsServiceAccountJson") or \
                            self.secretManagerHandler.get_secret("sheets-admin-service-account")
                    creds = json.loads(creds) if isinstance(creds, str) else creds
                    self._sheets_handler = SheetsHandler(
                        serviceAccountJson=creds,
                        parent_folder_id=self._init_kwargs.get("sheetsParentFolderId"),
                    )
        return self._sheets_handler

    @property
    def firestoreHandler(self):
        if self._firestore_handler is None:
            with self._handler_locks["firestore"]:
                if self._firestore_handler is None:
                    logger.info("Lazy loading FirestoreHandler")
                    try:
                        if self._init_kwargs.get("firestoreServiceAccountJson"):
                            logger.info("FirestoreHandler: using credential from init kwargs")
                            creds = self._init_kwargs.get("firestoreServiceAccountJson")
                        else:
                            logger.info("FirestoreHandler: fetching credential from Secret Manager (firestore-admin-service-account)")
                            creds = self.secretManagerHandler.get_secret("firestore-admin-service-account")
                        creds = json.loads(creds) if isinstance(creds, str) else creds
                        self._firestore_handler = FirestoreHandler(serviceAccountJson=creds, singleton=self._handler_singleton)
                        logger.info("FirestoreHandler lazy loaded successfully")
                    except Exception as e:
                        logger.exception(f"Failed to lazy load FirestoreHandler: {e}")
                        raise
        return self._firestore_handler


class GoogleCloudHandlerNonSingleton(_GoogleCloudHandlerBase):
    """
    Non-singleton variant of the Google Cloud client.

    Every construction returns a fresh, independent instance with its own
    lazily-loaded handlers and configuration. Use this when you need multiple
    clients side by side (e.g. different credentials per instance) or want
    isolation between callers. For the shared, process-wide default, use
    :class:`GoogleCloudHandler`.

    The underlying service handlers are also built in non-singleton mode, so a
    fresh instance owns its own handlers (with their own credentials) rather
    than sharing the process-wide handler singletons.
    """
    _handler_singleton = False


class GoogleCloudHandler(_GoogleCloudHandlerBase):
    """
    Singleton variant of the Google Cloud client (the package default).

    The first construction sets up lazy loading; every later construction
    returns the same instance. Kwargs passed on a later construction are merged
    into the stored config for handlers that have not been initialized yet —
    already-live handlers keep their existing instance.

    Need a fresh, independent client instead? Use
    :class:`GoogleCloudHandlerNonSingleton`.
    """
    _instance = None
    _lock = threading.Lock()

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super(GoogleCloudHandler, cls).__new__(cls)
        return cls._instance

    def __init__(self, **kwargs):
        with self._lock:
            if getattr(self, "_initialized", False):
                # Merge new kwargs into existing instance for handlers not yet
                # initialized. Already-live handlers are unaffected — their
                # instance is already cached.
                if kwargs:
                    self._init_kwargs.update(kwargs)
                return
            super().__init__(**kwargs)
            self._initialized = True

    @classmethod
    def reset_instance(cls):
        """Drop the cached singleton so the next construction starts fresh.

        Mainly useful for tests or when re-configuring the process-wide client.
        """
        with cls._lock:
            cls._instance = None
