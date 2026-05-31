from __future__ import annotations


import google.auth.transport.requests
import requests
from google.oauth2 import service_account


class CloudSchedulerHandler:
    """Handler for interaction with Google Cloud Scheduler."""

    def __init__(self, serviceAccountJson: dict):
        self.serviceAccountJson = serviceAccountJson
        self.project_id = serviceAccountJson["project_id"] if serviceAccountJson and "project_id" in serviceAccountJson else None
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
        url = f"https://cloudscheduler.googleapis.com/v1/projects/{self.project_id}/locations"
        headers = {"Authorization": f"Bearer {self.ACCESS_TOKEN}"}
        response = requests.get(url, headers=headers)
        return response.json()

    def getCloudJobs(self, location: str):
        next_page_token = ""
        url = f"https://cloudscheduler.googleapis.com/v1/projects/{self.project_id}/locations/{location}/jobs"
        headers = {"Authorization": f"Bearer {self.ACCESS_TOKEN}"}
        jobs = []
        while next_page_token is not None:
            params = {"pageToken": next_page_token}
            response = requests.get(url, headers=headers, params=params).json()
            next_page_token = response.get("nextPageToken", None)
            jobs.extend(response.get("jobs", []))
        return jobs

    def getCloudJob(self, location: str, job_name: str):
        url = f"https://cloudscheduler.googleapis.com/v1/projects/{self.project_id}/locations/{location}/jobs/{job_name}"
        headers = {"Authorization": f"Bearer {self.ACCESS_TOKEN}"}
        return requests.get(url, headers=headers).json()

    def createCloudJob(self, location: str, job: dict):
        url = f"https://cloudscheduler.googleapis.com/v1/projects/{self.project_id}/locations/{location}/jobs"
        headers = {"Authorization": f"Bearer {self.ACCESS_TOKEN}"}
        return requests.post(url, headers=headers, json=job).json()

    def createPubSubJob(
        self,
        location: str,
        name: str,
        description: str,
        schedule: str,
        timeZone: str,
        pubSubTarget: dict,
    ) -> dict:
        return {
            "name": f"projects/{self.project_id}/locations/{location}/jobs/{name}",
            "description": description,
            "schedule": schedule,
            "timeZone": timeZone,
            "pubsubTarget": pubSubTarget,
        }

    def createHttpJob(
        self,
        location: str,
        name: str,
        description: str,
        schedule: str,
        timeZone: str,
        httpTarget: dict,
    ) -> dict:
        return {
            "name": f"projects/{self.project_id}/locations/{location}/jobs/{name}",
            "description": description,
            "schedule": schedule,
            "timeZone": timeZone,
            "httpTarget": httpTarget,
        }

    def createPubSubTarget(
        self, topicName: str, data: str, attributes: dict = {}
    ) -> dict:
        return {
            "topicName": f"projects/{self.project_id}/topics/{topicName}",
            "data": data,
            "attributes": attributes,
        }

    def deleteCloudJob(self, location: str, job_name: str):
        url = f"https://cloudscheduler.googleapis.com/v1/projects/{self.project_id}/locations/{location}/jobs/{job_name}"
        headers = {"Authorization": f"Bearer {self.ACCESS_TOKEN}"}
        return requests.delete(url, headers=headers).json()

    def pauseCloudJob(self, location: str, job_name: str):
        url = f"https://cloudscheduler.googleapis.com/v1/projects/{self.project_id}/locations/{location}/jobs/{job_name}:pause"
        headers = {"Authorization": f"Bearer {self.ACCESS_TOKEN}"}
        return requests.post(url, headers=headers).json()

    def resumeCloudJob(self, location: str, job_name: str):
        url = f"https://cloudscheduler.googleapis.com/v1/projects/{self.project_id}/locations/{location}/jobs/{job_name}:resume"
        headers = {"Authorization": f"Bearer {self.ACCESS_TOKEN}"}
        return requests.post(url, headers=headers).json()

    def runCloudJob(self, location: str, job_name: str):
        url = f"https://cloudscheduler.googleapis.com/v1/projects/{self.project_id}/locations/{location}/jobs/{job_name}:run"
        headers = {"Authorization": f"Bearer {self.ACCESS_TOKEN}"}
        return requests.post(url, headers=headers).json()

    def updateCloudJob(self, location: str, job_name: str, job: dict):
        url = f"https://cloudscheduler.googleapis.com/v1/projects/{self.project_id}/locations/{location}/jobs/{job_name}"
        headers = {"Authorization": f"Bearer {self.ACCESS_TOKEN}"}
        return requests.patch(url, headers=headers, json=job).json()
