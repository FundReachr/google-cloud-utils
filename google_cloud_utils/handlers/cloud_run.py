from google.oauth2 import service_account
from google.cloud import run_v2

_JOB_PATH = "projects/skilled-sunrise-447019-e2/locations/europe-west9/jobs/{job_name}"


class CloudRunJobHandler:
    """Handler for triggering Cloud Run Job executions using explicit SA credentials."""

    def __init__(self, serviceAccountJson: dict | None = None):
        if serviceAccountJson:
            credentials = service_account.Credentials.from_service_account_info(
                serviceAccountJson,
                scopes=["https://www.googleapis.com/auth/cloud-platform"],
            )
            self.client = run_v2.JobsClient(credentials=credentials)
        else:
            self.client = run_v2.JobsClient()

    def trigger_job(self, job_name: str, env: dict) -> str:
        """Trigger a Cloud Run Job execution. Returns the execution name (non-blocking)."""
        operation = self.client.run_job(
            request=run_v2.RunJobRequest(
                name=_JOB_PATH.format(job_name=job_name),
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
