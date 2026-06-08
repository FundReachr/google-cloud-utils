"""Lazy access to the Google Cloud service handlers.

Each handler lives in its own submodule and is imported on first attribute
access (PEP 562 module ``__getattr__``) rather than at package-import time. This
means importing :mod:`google_cloud_utils.handlers` — or :mod:`google_cloud_utils`
— does **not** require every Google Cloud client library to be installed. A
consumer that only uses, say, Datastore will not pull in ``google-cloud-run``,
``gspread`` or ``google-genai`` unless it actually touches those handlers.
"""
from __future__ import annotations

import importlib
from typing import TYPE_CHECKING

# Public name -> submodule (within this package) that defines it.
_LAZY_IMPORTS = {
    "CloudStorageHandler": "gcs",
    "GCSLogHandler": "gcs",
    "SecretManagerHandler": "secrets",
    "BigQueryHandler": "bigquery",
    "PubSubHandler": "pubsub",
    "CloudTasksHandler": "tasks",
    "FirestoreHandler": "firestore",
    "DatastoreHandler": "datastore",
    "CloudSchedulerHandler": "cloud_scheduler",
    "CloudFunctionHandler": "cloud_function",
    "GenAIHandler": "genai",
    "SheetsHandler": "sheets",
    "CloudRunJobHandler": "cloud_run",
    "GoogleCloudHandler": "client",
    "GoogleCloudHandlerNonSingleton": "client",
}

__all__ = list(_LAZY_IMPORTS)


def __getattr__(name: str):
    try:
        module_name = _LAZY_IMPORTS[name]
    except KeyError:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}") from None
    module = importlib.import_module(f".{module_name}", __name__)
    value = getattr(module, name)
    globals()[name] = value  # cache so __getattr__ isn't called again for this name
    return value


def __dir__():
    return sorted(set(globals()) | set(_LAZY_IMPORTS))


if TYPE_CHECKING:  # static analysers / IDEs — no runtime import cost
    from .gcs import CloudStorageHandler, GCSLogHandler
    from .secrets import SecretManagerHandler
    from .bigquery import BigQueryHandler
    from .pubsub import PubSubHandler
    from .tasks import CloudTasksHandler
    from .firestore import FirestoreHandler
    from .datastore import DatastoreHandler
    from .cloud_scheduler import CloudSchedulerHandler
    from .cloud_function import CloudFunctionHandler
    from .genai import GenAIHandler
    from .sheets import SheetsHandler
    from .cloud_run import CloudRunJobHandler
    from .client import GoogleCloudHandler, GoogleCloudHandlerNonSingleton
