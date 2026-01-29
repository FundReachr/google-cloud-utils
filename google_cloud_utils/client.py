from .handlers.bigquery.handler import BigQueryHandler
from .handlers.cloud_storage.handler import CloudStorageHandler
from .handlers.cloud_tasks.handler import CloudTasksHandler
from .handlers.secret_manager.handler import SecretManagerHandler
from .handlers.cloud_scheduler.handler import CloudSchedulerHandler
from .handlers.datastore.handler import DatastoreHandler
from .handlers.pubsub.handler import PubSubHandler
from .handlers import CloudFunctionHandler
import threading

class GoogleCloudHandler:
    """
    Main client for Google Cloud Utilities.
    
    This singleton class aggregates various service handlers into a single access point.
    It ensures that handlers are initialized only once and in a thread-safe manner.

    Attributes:
        _instance (GoogleCloudHandler): The singleton instance.
        _lock (threading.Lock): Lock to ensure thread-safe singleton creation.
    """
    _instance = None
    _lock = threading.Lock()

    def __new__(cls, *args, **kwargs):
        """
        Create or return the singleton instance of GoogleCloudHandler.
        
        Using the Double-Checked Locking pattern for thread safety.
        """
        if cls._instance is None:
            with cls._lock:  # Prevent race conditions
                if cls._instance is None:
                    cls._instance = super(GoogleCloudHandler, cls).__new__(cls)
                    cls._instance._initialize(*args, **kwargs)  
        return cls._instance
    
    def _initialize(self, 
                    secretManagerServiceAccount: dict = None, 
                    bigQueryServiceAccountJson: dict = None, 
                    cloudStorageServiceAccountJson: dict = None, 
                    cloudSchedulerServiceAccountJson: dict = None,
                    datastoreServiceAccountJson: dict = None,
                    pubSubServiceAccountJson: dict = None,
                    cloudTasksServiceAccountJson: dict = None,
                    cloudFunctionsServiceAccountJson: dict = None):
        """
        Initialize the handlers.
        
        If secretManagerServiceAccount is provided, it attempts to fetch other credentials
        from Secret Manager if they are not explicitly provided.

        Args:
            secretManagerServiceAccount (dict, optional): Service account info for Secret Manager.
            bigQueryServiceAccountJson (dict, optional): Service account info for BigQuery.
            cloudStorageServiceAccountJson (dict, optional): Service account info for Cloud Storage.
            cloudSchedulerServiceAccountJson (dict, optional): Service account info for Cloud Scheduler.
            datastoreServiceAccountJson (dict, optional): Service account info for Datastore.
            pubSubServiceAccountJson (dict, optional): Service account info for Pub/Sub.
            cloudTasksServiceAccountJson (dict, optional): Service account info for Cloud Tasks.
            cloudFunctionsServiceAccountJson (dict, optional): Service account info for Cloud Functions.
        
        Raises:
            Exception: If initialization of any handler fails.
        """
        print("Initializing Google Cloud Handler...")
        if hasattr(self, "secretManagerHandler"):
            return

        try:
            self.secretManagerHandler = SecretManagerHandler(serviceAccountJson=secretManagerServiceAccount)
            
            # Fetch credentials from Secret Manager if not provided
            cloudStorageServiceAccountJson = cloudStorageServiceAccountJson or self.secretManagerHandler.get_secret("cloud-storage-admin-service-account")
            self.cloudStorageHandler = CloudStorageHandler(serviceAccountJson=cloudStorageServiceAccountJson)
            
            bigQueryServiceAccountJson = bigQueryServiceAccountJson or self.secretManagerHandler.get_secret("bigquery-admin-service-account")
            self.bigQueryHandler = BigQueryHandler(serviceAccountJson=bigQueryServiceAccountJson)
            
            cloudSchedulerServiceAccountJson = cloudSchedulerServiceAccountJson or self.secretManagerHandler.get_secret("cloud-scheduler-admin-service-account")
            self.cloudSchedulerHandler = CloudSchedulerHandler(serviceAccountJson=cloudSchedulerServiceAccountJson)
            
            datastoreServiceAccountJson = datastoreServiceAccountJson or self.secretManagerHandler.get_secret("firestore-admin-service-account")
            self.datastoreHandler = DatastoreHandler(serviceAccountJson=datastoreServiceAccountJson) 
            
            pubSubServiceAccountJson = pubSubServiceAccountJson or self.secretManagerHandler.get_secret("pubsub-admin-service-account")
            self.pubSubHandler = PubSubHandler(serviceAccountJson=pubSubServiceAccountJson)
            
            cloudTasksServiceAccountJson = cloudTasksServiceAccountJson or self.secretManagerHandler.get_secret("cloud-tasks-admin-service-account")
            self.cloudTasksHandler = CloudTasksHandler(serviceAccountJson=cloudTasksServiceAccountJson)

            cloudFunctionsServiceAccountJson = cloudFunctionsServiceAccountJson or self.secretManagerHandler.get_secret("cloud-function-admin-service-account")
            self.cloudFunctionHandler = CloudFunctionHandler(serviceAccountJson=cloudFunctionsServiceAccountJson)
        except Exception as e:
            print(f"Error initializing Google Cloud Handler: {e}")
            raise e
