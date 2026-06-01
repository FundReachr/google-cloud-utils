"""Backwards-compatible entry point for the Google Cloud client.

The canonical implementation lives in :mod:`google_cloud_utils.handlers.client`.
This module simply re-exports it so that ``from google_cloud_utils.client import
GoogleCloudHandler`` keeps working. ``GoogleCloudHandler`` is the singleton
default; ``GoogleCloudHandlerNonSingleton`` returns a fresh instance per call.
"""

from .handlers.client import GoogleCloudHandler, GoogleCloudHandlerNonSingleton

__all__ = [
    "GoogleCloudHandler",
    "GoogleCloudHandlerNonSingleton",
]
