from __future__ import annotations

import traceback
from typing import Any, Dict, Optional

import google.auth
import google.auth.transport.requests
import requests
from google.oauth2 import service_account


class CloudFunctionHandler:
    """Handler for interaction with Google Cloud Functions."""

    def __init__(self, serviceAccountJson: Dict[str, Any]) -> None:
        self.serviceAccountJson = serviceAccountJson
        self.project_id = serviceAccountJson["project_id"]
        self.credentials = self.getCredentials()
        self.ACCESS_TOKEN = self.credentials.token

    def getCredentials(self):
        credentials = service_account.Credentials.from_service_account_info(
            self.serviceAccountJson,
            scopes=["https://www.googleapis.com/auth/cloud-platform"],
        )
        request = google.auth.transport.requests.Request()
        credentials.refresh(request)
        return credentials

    def getCloudLocations(self):
        url = f"https://cloudfunctions.googleapis.com/v1/projects/{self.project_id}/locations"
        headers = {"Authorization": f"Bearer {self.ACCESS_TOKEN}"}
        return requests.get(url, headers=headers).json()

    def getCloudOperations(self):
        url = "https://cloudfunctions.googleapis.com/v1/operations"
        headers = {"Authorization": f"Bearer {self.ACCESS_TOKEN}"}
        return requests.get(url, headers=headers).json()

    def getCloudOperation(self, operation_name: str):
        url = f"https://cloudfunctions.googleapis.com/v1/projects/operations/{operation_name}"
        headers = {"Authorization": f"Bearer {self.ACCESS_TOKEN}"}
        return requests.get(url, headers=headers).json()

    def callCloudFunction(self, location: str, function_name: str, data=None):
        if data is None:
            data = str({})
        url = f"https://cloudfunctions.googleapis.com/v1/projects/{self.project_id}/locations/{location}/functions/{function_name}:call"
        headers = {"Authorization": f"Bearer {self.ACCESS_TOKEN}"}
        return requests.post(url, headers=headers, json={"data": data}).json()

    def getCloudFunction(self, location: str, function_name: str):
        url = f"https://cloudfunctions.googleapis.com/v1/projects/{self.project_id}/locations/{location}/functions/{function_name}"
        headers = {"Authorization": f"Bearer {self.ACCESS_TOKEN}"}
        return requests.get(url, headers=headers).json()

    def listCloudFunctions(self, location: str):
        url = f"https://cloudfunctions.googleapis.com/v1/projects/{self.project_id}/locations/{location}/functions"
        headers = {"Authorization": f"Bearer {self.ACCESS_TOKEN}"}
        try:
            response = requests.get(url, headers=headers)
            if response.status_code == 200:
                return response.json(), response.status_code
            return "", response.status_code
        except Exception:
            return "", 500

    def createCloudFunction(self, location: str, data: dict):
        url = f"https://cloudfunctions.googleapis.com/v1/projects/{self.project_id}/locations/{location}/functions"
        headers = {"Authorization": f"Bearer {self.ACCESS_TOKEN}"}
        data["name"] = f"projects/{self.project_id}/locations/{location}/functions/{data['name']}"
        return requests.post(url, headers=headers, json=data).json()

    def updateCloudFunction(self, location: str, function_name: str, data: dict):
        url = f"https://cloudfunctions.googleapis.com/v1/projects/{self.project_id}/locations/{location}/functions/{function_name}"
        headers = {"Authorization": f"Bearer {self.ACCESS_TOKEN}"}
        return requests.patch(url, headers=headers, json=data).json()

    def deleteCloudFunction(self, location: str, function_name: str):
        url = f"https://cloudfunctions.googleapis.com/v1/projects/{self.project_id}/locations/{location}/functions/{function_name}"
        headers = {"Authorization": f"Bearer {self.ACCESS_TOKEN}"}
        return requests.delete(url, headers=headers).json()

    def generateUploadUrl(self, location: str, kmsKeyName: str) -> Optional[str]:
        url = f"https://cloudfunctions.googleapis.com/v1/projects/{self.project_id}/locations/{location}/functions:generateUploadUrl"
        headers = {
            "Authorization": f"Bearer {self.ACCESS_TOKEN}",
            "Content-Type": "application/zip",
        }
        params = {"kmsKeyName": kmsKeyName}
        response = requests.post(url, headers=headers, params=params)
        try:
            return response.json()["uploadUrl"]
        except Exception as e:
            print(e)
            print(response.json())
            traceback.print_exc()
            return None

    def generateDownloadUrl(self, location: str):
        url = f"https://cloudfunctions.googleapis.com/v1/projects/{self.project_id}/locations/{location}/functions:generateDownloadUrl"
        headers = {"Authorization": f"Bearer {self.ACCESS_TOKEN}"}
        return requests.get(url, headers=headers).json()

    def uploadFunctionToStorage(self, uploadUrl: str, zippedFunctionPath: str):
        try:
            with open(zippedFunctionPath, "rb") as f:
                header = {
                    "Content-Type": "application/zip",
                    "x-goog-content-length-range": "0,104857600",
                }
                return requests.put(uploadUrl, headers=header, data=f).content
        except Exception as e:
            print(e)
            traceback.print_exc()
            return None
