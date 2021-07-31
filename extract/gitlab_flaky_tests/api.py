import logging
from typing import Dict, Any, List, Optional

import requests


class GitLabAPI:
    GITLAB_COM_API_BASE_URL = "https://gitlab.com/api/v4"

    def __init__(self, api_token):
        self.api_token = api_token

    def get_pipeline_schedule(
        self, project_id: str, pipeline_schedule_id: int
    ) -> Optional[Dict[Any, Any]]:
        url = f"{self.GITLAB_COM_API_BASE_URL}/projects/{project_id}/pipeline_schedules/{pipeline_schedule_id}"
        response = requests.get(url, headers={"Private-Token": self.api_token})

        if response.status_code == 200:
            return response.json()
        else:
            logging.warn(
                f"Request for pipeline schedule {pipeline_schedule_id} from project id {project_id} resulted in a code {response.status_code}."
            )
        return None

    def get_job_json_artifact(
        self, project_id: str, job_id: int, artifact_path: str
    ) -> Optional[Dict[Any, Any]]:
        url = f"{self.GITLAB_COM_API_BASE_URL}/projects/{project_id}/jobs/{job_id}/artifacts/{artifact_path}"
        response = requests.get(url, headers={"Private-Token": self.api_token})

        if response.status_code == 200:
            return response.json()
        else:
            logging.warn(
                f"Request for job artifact '{artifact_path}' of job {job_id} from project id {project_id} resulted in a code {response.status_code}."
            )
        return None

    def get_pipeline_job_paged(
        self, project_id: str, pipeline_id: int, page: int, scope: str = None
    ) -> List[Dict[Any, Any]]:
        url = f"{self.GITLAB_COM_API_BASE_URL}/projects/{project_id}/pipelines/{pipeline_id}/jobs?per_page=100&page={page}"

        if scope != None:
            url += f"&scope={scope}"

        response = requests.get(url, headers={"Private-Token": self.api_token})

        if response.status_code == 200:
            return response.json()
        else:
            logging.warn(
                f"Request for jobs of pipeline {pipeline_id} from project id {project_id} resulted in a code {response.status_code}."
            )
        return []

    def get_pipeline_job(
        self, project_id: str, pipeline_id: int, job_name: str
    ) -> Optional[Dict[Any, Any]]:
        current_page_number = 1
        while True:
            current_result = self.get_pipeline_job_paged(
                project_id, pipeline_id, current_page_number, "success"
            )

            if not current_result:
                return None

            for job in current_result:
                if job["name"] == job_name:
                    return job

            current_page_number = current_page_number + 1
