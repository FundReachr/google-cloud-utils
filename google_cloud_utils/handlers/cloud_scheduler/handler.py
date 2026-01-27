import google.auth.transport.requests
from google.oauth2 import service_account
import requests
import json 

class CloudSchedulerHandler:
    """
    Handler for interaction with Google Cloud Scheduler.

    Allows for creating, updating, pausing, resuming, and deleting Cloud Scheduler jobs.
    """
    def __init__(self,serviceAccountJson):
        """
        Initialize the CloudSchedulerHandler.

        Args:
            serviceAccountJson (dict): Service account credentials containing 'project_id'.
        """
        self.serviceAccountJson = serviceAccountJson
        self.project_id = serviceAccountJson['project_id'] if serviceAccountJson and 'project_id' in serviceAccountJson else None
        self.credentials = self.getCredentials()
        self.ACCESS_TOKEN = self.credentials.token

    def getCredentials(self):
        """Refresh and retrieve credentials."""
        credentials = service_account.Credentials.from_service_account_info(self.serviceAccountJson,
                                                                            scopes=['https://www.googleapis.com/auth/cloud-platform'])
        request = google.auth.transport.requests.Request()
        credentials.refresh(request)
        return credentials
    
    def getCloudLocations(self):
        url = f"https://cloudscheduler.googleapis.com/v1/projects/{self.project_id}/locations"
        headers = {'Authorization' : f'Bearer {self.ACCESS_TOKEN}'}
        response = requests.get(url, headers=headers)
        return response.json()
    
    def getCloudJobs(self,location):
        next_page_token = ""
        url = f"https://cloudscheduler.googleapis.com/v1/projects/{self.project_id}/locations/{location}/jobs"
        headers = {'Authorization' : f'Bearer {self.ACCESS_TOKEN}'}
        jobs = []
        while next_page_token != None:
            params = {'pageToken': next_page_token}
            response = requests.get(url, headers=headers, params=params) 
            response = response.json()
            next_page_token = response.get('nextPageToken', None)
            new_jobs = response.get('jobs', [])
            jobs.extend(new_jobs)
        return jobs
    
    def getCloudJob(self,location,job_name):
        url = f"https://cloudscheduler.googleapis.com/v1/projects/{self.project_id}/locations/{location}/jobs/{job_name}"
        headers = {'Authorization' : f'Bearer {self.ACCESS_TOKEN}'}
        response = requests.get(url, headers=headers)
        return response.json()

    def createCloudJob(self,location,job):
        url = f"https://cloudscheduler.googleapis.com/v1/projects/{self.project_id}/locations/{location}/jobs"
        headers = {'Authorization' : f'Bearer {self.ACCESS_TOKEN}'}
        data = json.dumps(job)
        response = requests.post(url, headers=headers, json=job)
        return response.json()
    
    def createPusSubJob(self,location,name,description,schedule,timeZone,pubSubTarget):
        job = {
            "name": "projects/"+self.project_id+"/locations/"+location+"/jobs/"+name,
            "description": description,
            "schedule": schedule,
            "timeZone": timeZone,
            "pubsubTarget": pubSubTarget
        }
        return job
    
    def createHttpJob(self,location,name,description,schedule,timeZone,httpTarget):
        job = {
            "name": "projects/"+self.project_id+"/locations/"+location+"/jobs/"+name,
            "description": description,
            "schedule": schedule,
            "timeZone": timeZone,
            "httpTarget": httpTarget
        }
        return job
    
    def createPubSubTarget(self,topicName,data,attributes={}):
        pubSubTarget = {
            "topicName": "projects/"+self.project_id+"/topics/"+topicName,
            "data": data,
            "attributes": attributes
        }
        return pubSubTarget
    
    def deleteCloudJob(self,location,job_name):
        url = f"https://cloudscheduler.googleapis.com/v1/projects/{self.project_id}/locations/{location}/jobs/{job_name}"
        headers = {'Authorization' : f'Bearer {self.ACCESS_TOKEN}'}
        response = requests.delete(url, headers=headers)
        return response.json()
    
    def pauseCloudJob(self,location,job_name):
        url = f"https://cloudscheduler.googleapis.com/v1/projects/{self.project_id}/locations/{location}/jobs/{job_name}:pause"
        headers = {'Authorization' : f'Bearer {self.ACCESS_TOKEN}'}
        response = requests.post(url, headers=headers)
        return response.json()
    
    def resumeCloudJob(self,location,job_name):
        url = f"https://cloudscheduler.googleapis.com/v1/projects/{self.project_id}/locations/{location}/jobs/{job_name}:resume"
        headers = {'Authorization' : f'Bearer {self.ACCESS_TOKEN}'}
        response = requests.post(url, headers=headers)
        return response.json()
    
    def runCloudJob(self,location,job_name):
        url = f"https://cloudscheduler.googleapis.com/v1/projects/{self.project_id}/locations/{location}/jobs/{job_name}:run"
        headers = {'Authorization' : f'Bearer {self.ACCESS_TOKEN}'}
        response = requests.post(url, headers=headers)
        return response.json()
    
    def updateCloudJob(self,location,job_name,job):
        url = f"https://cloudscheduler.googleapis.com/v1/projects/{self.project_id}/locations/{location}/jobs/{job_name}"
        headers = {'Authorization' : f'Bearer {self.ACCESS_TOKEN}'}
        response = requests.patch(url, headers=headers, json=job)
        return response.json()