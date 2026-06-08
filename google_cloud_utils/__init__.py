"""google_cloud_utils — a unified, lazily-loaded interface to Google Cloud services.

Handler classes are imported on first access (PEP 562 module ``__getattr__``), so
``import google_cloud_utils`` stays cheap and does not require every Google Cloud
client library to be installed — you only need the dependencies for the handlers
you actually use. ``GoogleCloudHandler`` is the singleton default;
``GoogleCloudHandlerNonSingleton`` returns a fresh, isolated instance per call.
"""
from __future__ import annotations

import importlib
from typing import TYPE_CHECKING

# Public name -> fully-qualified submodule (relative to this package) that defines it.
_LAZY_IMPORTS = {
    "CloudStorageHandler": ".handlers.gcs",
    "GCSLogHandler": ".handlers.gcs",
    "SecretManagerHandler": ".handlers.secrets",
    "BigQueryHandler": ".handlers.bigquery",
    "PubSubHandler": ".handlers.pubsub",
    "CloudTasksHandler": ".handlers.tasks",
    "FirestoreHandler": ".handlers.firestore",
    "DatastoreHandler": ".handlers.datastore",
    "CloudSchedulerHandler": ".handlers.cloud_scheduler",
    "CloudFunctionHandler": ".handlers.cloud_function",
    "GenAIHandler": ".handlers.genai",
    "SheetsHandler": ".handlers.sheets",
    "CloudRunJobHandler": ".handlers.cloud_run",
    "GoogleCloudHandler": ".handlers.client",
    "GoogleCloudHandlerNonSingleton": ".handlers.client",
}

__all__ = list(_LAZY_IMPORTS)


def __getattr__(name: str):
    try:
        module_path = _LAZY_IMPORTS[name]
    except KeyError:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}") from None
    module = importlib.import_module(module_path, __name__)
    value = getattr(module, name)
    globals()[name] = value  # cache so __getattr__ isn't called again for this name
    return value


def __dir__():
    return sorted(set(globals()) | set(_LAZY_IMPORTS))


if TYPE_CHECKING:  # static analysers / IDEs — no runtime import cost
    from .handlers.gcs import CloudStorageHandler, GCSLogHandler
    from .handlers.secrets import SecretManagerHandler
    from .handlers.bigquery import BigQueryHandler
    from .handlers.pubsub import PubSubHandler
    from .handlers.tasks import CloudTasksHandler
    from .handlers.firestore import FirestoreHandler
    from .handlers.datastore import DatastoreHandler
    from .handlers.cloud_scheduler import CloudSchedulerHandler
    from .handlers.cloud_function import CloudFunctionHandler
    from .handlers.genai import GenAIHandler
    from .handlers.sheets import SheetsHandler
    from .handlers.cloud_run import CloudRunJobHandler
    from .handlers.client import GoogleCloudHandler, GoogleCloudHandlerNonSingleton
