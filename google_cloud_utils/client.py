from .handlers import *
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
    
    def _initialize(self, **kwargs):
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
            self.secretManagerHandler = SecretManagerHandler(serviceAccountJson=kwargs.get("secretManagerServiceAccount"))
            
            # Fetch credentials from Secret Manager if not provided
            self.cloudStorageServiceAccountJson = kwargs.get("cloudStorageServiceAccountJson") or self.secretManagerHandler.get_secret("cloud-storage-admin-service-account")
            self.cloudStorageHandler = CloudStorageHandler(serviceAccountJson=self.cloudStorageServiceAccountJson)
            
            self.bigQueryServiceAccountJson = kwargs.get("bigQueryServiceAccountJson") or self.secretManagerHandler.get_secret("bigquery-admin-service-account")
            self.bigQueryHandler = BigQueryHandler(serviceAccountJson=self.bigQueryServiceAccountJson)
            
            self.cloudSchedulerServiceAccountJson = kwargs.get("cloudSchedulerServiceAccountJson") or self.secretManagerHandler.get_secret("cloud-scheduler-admin-service-account")
            self.cloudSchedulerHandler = CloudSchedulerHandler(serviceAccountJson=self.cloudSchedulerServiceAccountJson)
            
            self.datastoreServiceAccountJson = kwargs.get("datastoreServiceAccountJson") or self.secretManagerHandler.get_secret("firestore-admin-service-account")
            self.datastoreHandler = DatastoreHandler(serviceAccountJson=self.datastoreServiceAccountJson) 
            
            self.pubSubServiceAccountJson = kwargs.get("pubSubServiceAccountJson") or self.secretManagerHandler.get_secret("pubsub-admin-service-account")
            self.pubSubHandler = PubSubHandler(serviceAccountJson=self.pubSubServiceAccountJson)
            
            self.cloudTasksServiceAccountJson = kwargs.get("cloudTasksServiceAccountJson") or self.secretManagerHandler.get_secret("cloud-tasks-admin-service-account")
            self.cloudTasksHandler = CloudTasksHandler(serviceAccountJson=self.cloudTasksServiceAccountJson)

            self.cloudFunctionsServiceAccountJson = kwargs.get("cloudFunctionsServiceAccountJson") or self.secretManagerHandler.get_secret("cloud-function-admin-service-account")
            self.cloudFunctionHandler = CloudFunctionHandler(serviceAccountJson=self.cloudFunctionsServiceAccountJson)
        
            self.genaiServiceAccountJson = kwargs.get("genaiServiceAccountJson") or self.secretManagerHandler.get_secret("genai-user-service-account")
            self.genAIHandler = GenAIHandler(serviceAccountJson=self.genaiServiceAccountJson)
        except Exception as e:
            print(f"Error initializing Google Cloud Handler: {e}")
            raise e
