import google.auth
from google.oauth2 import service_account
from google.cloud import run_v2

_DEFAULT_LOCATION = "europe-west9"
_JOB_PATH = "projects/{project_id}/locations/{location}/jobs/{job_name}"


class CloudRunJobHandler:
    """Handler for triggering Cloud Run Job executions using explicit SA credentials."""

    def __init__(self, serviceAccountJson: dict | None = None):
        if serviceAccountJson:
            credentials = service_account.Credentials.from_service_account_info(
                serviceAccountJson,
                scopes=["https://www.googleapis.com/auth/cloud-platform"],
            )
            self.project_id = serviceAccountJson.get("project_id") or credentials.project_id
            self.client = run_v2.JobsClient(credentials=credentials)
        else:
            credentials, project_id = google.auth.default(
                scopes=["https://www.googleapis.com/auth/cloud-platform"],
            )
            self.project_id = project_id
            self.client = run_v2.JobsClient(credentials=credentials)

    def trigger_job(self, job_name: str, env: dict, location: str = _DEFAULT_LOCATION) -> str:
        """Trigger a Cloud Run Job execution. Returns the execution name (non-blocking).

        The job's project is taken from the service account's ``project_id`` (or
        the ambient credentials' project when no SA was supplied).
        """
        operation = self.client.run_job(
            request=run_v2.RunJobRequest(
                name=_JOB_PATH.format(
                    project_id=self.project_id,
                    location=location,
                    job_name=job_name,
                ),
                overrides=run_v2.RunJobRequest.Overrides(
                    container_overrides=[
                        run_v2.RunJobRequest.Overrides.ContainerOverride(
                            env=[run_v2.EnvVar(name=k, value=v) for k, v in env.items()]
                        )
                    ]
                ),
            )
        )
        return operation.metadata.name
