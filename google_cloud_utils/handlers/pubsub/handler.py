import json
import os
import threading
import traceback
from google.cloud import pubsub_v1
from google.oauth2 import service_account
from google.auth import compute_engine


class PubSubHandler:
    """
    Handler for Google Cloud Pub/Sub interactions.

    Facilitates topic and subscription creation, message publishing, and pulling.
    """
    _instance = None
    _lock = threading.Lock()

    def __new__(cls, *args, **kwargs):
        """Singleton pattern implementation."""
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super(PubSubHandler, cls).__new__(cls)
                    cls._instance._initialize(*args, **kwargs)
        return cls._instance

    def _initialize(self, serviceAccountJson: dict = None,
                    publisher: pubsub_v1.PublisherClient = None,
                    subscriber: pubsub_v1.SubscriberClient = None):
        """
        Initialize the PubSubHandler.

        Args:
            serviceAccountJson (dict, optional): Service account credentials.
            publisher (pubsub_v1.PublisherClient, optional): Existing publisher client.
            subscriber (pubsub_v1.SubscriberClient, optional): Existing subscriber client.

        Raises:
            Exception: If initialization fails.
        """
        print("Initializing PubSub Handler...")

        if hasattr(self, "publisher"):
            return

        try:
            # --------------------
            # Credentials
            # --------------------
            if serviceAccountJson:
                print("Using provided Pub/Sub service account JSON")
                credentials = service_account.Credentials.from_service_account_info(
                    serviceAccountJson,
                    scopes=["https://www.googleapis.com/auth/cloud-platform"],
                )
                self.project_id = serviceAccountJson["project_id"]

            else:
                try:
                    print("Using Pub/Sub service account JSON from env")
                    serviceAccountJson = json.loads(
                        os.getenv("PUBSUB_SERVICE_ACCOUNT_JSON")
                    )
                    credentials = service_account.Credentials.from_service_account_info(
                        serviceAccountJson,
                        scopes=["https://www.googleapis.com/auth/cloud-platform"],
                    )
                    self.project_id = serviceAccountJson["project_id"]

                except Exception:
                    print("Using default credentials for Pub/Sub")
                    credentials = compute_engine.Credentials()
                    self.project_id = os.getenv("GCP_PROJECT")

            # --------------------
            # Clients
            # --------------------
            self.publisher = publisher or pubsub_v1.PublisherClient(
                credentials=credentials
            )
            self.subscriber = subscriber or pubsub_v1.SubscriberClient(
                credentials=credentials
            )

        except Exception as e:
            traceback.print_exc()
            raise Exception(f"Error initializing PubSubHandler: {e}")

    # =====================================================
    # Helpers
    # =====================================================
    def topic_path(self, topic_name: str):
        return f"projects/{self.project_id}/topics/{topic_name}"

    def subscription_path(self, subscription_name: str):
        return f"projects/{self.project_id}/subscriptions/{subscription_name}"

    # =====================================================
    # Topics / Subscriptions
    # =====================================================
    def create_topic(self, topic_name: str):
        try:
            self.publisher.create_topic(name=self.topic_path(topic_name))
            return True
        except Exception:
            traceback.print_exc()
            return False

    def create_push_subscription(
        self,
        topic_name: str,
        subscription_name: str,
        push_endpoint: str,
        ack_deadline_seconds: int = 30,
    ):
        try:
            self.subscriber.create_subscription(
                name=self.subscription_path(subscription_name),
                topic=self.topic_path(topic_name),
                push_config={"push_endpoint": push_endpoint},
                ack_deadline_seconds=ack_deadline_seconds,
            )
            return True
        except Exception:
            traceback.print_exc()
            return False

    # =====================================================
    # Publish
    # =====================================================
    def publish(self, topic_name: str, payload: dict, attributes: dict = None):
        try:
            data = json.dumps(payload).encode("utf-8")
            future = self.publisher.publish(
                self.topic_path(topic_name),
                data=data,
                **(attributes or {})
            )
            return future.result()
        except Exception:
            traceback.print_exc()
            return None

    # =====================================================
    # Pull (optional)
    # =====================================================
    def pull(self, subscription_name: str, max_messages: int = 1):
        try:
            response = self.subscriber.pull(
                subscription=self.subscription_path(subscription_name),
                max_messages=max_messages,
            )
            return response.received_messages
        except Exception:
            traceback.print_exc()
            return []

    def ack(self, subscription_name: str, ack_ids: list):
        try:
            self.subscriber.acknowledge(
                subscription=self.subscription_path(subscription_name),
                ack_ids=ack_ids,
            )
        except Exception:
            traceback.print_exc()
