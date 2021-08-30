import json
import re
import sys
import os
import logging

from logging import error, info, basicConfig, getLogger, warning
from os.path import join, getsize, dirname
from os import environ as env

from api import GitLabAPI
from gitlabdata.orchestration_utils import (
    snowflake_engine_factory,
    snowflake_stage_load_copy_remove,
)

if __name__ == "__main__":
    GITLAB_PROJECT_ID = 278964
    NIGHTLY_PIPELINE_SCHEDULE = 47406
    GITLAB_COM_API_BASE_URL = "https://gitlab.com/api/v4"
    UPDATE_TESTS_METADATA_JOB_NAME = "update-tests-metadata"
    RSPEC_FLAKY_REPORT_ARTIFACT = "rspec_flaky/report-suite.json"

    logging.basicConfig(stream=sys.stdout, level=20)

    api_token = env["GITLAB_COM_API_TOKEN"]
    api_client = GitLabAPI(api_token)

    info(
        f"1. Getting pipeline schedule {NIGHTLY_PIPELINE_SCHEDULE} for project {GITLAB_PROJECT_ID}."
    )
    pipeline_schedule = api_client.get_pipeline_schedule(
        GITLAB_PROJECT_ID, NIGHTLY_PIPELINE_SCHEDULE
    )

    if pipeline_schedule is None:
        error(
            f"Pipeline schedule {NIGHTLY_PIPELINE_SCHEDULE} couldn't be found for project {GITLAB_PROJECT_ID}."
        )
        sys.exit(0)

    last_scheduled_pipeline_id = pipeline_schedule["last_pipeline"]["id"]

    info(
        f"2. Getting job '{UPDATE_TESTS_METADATA_JOB_NAME}' for pipeline {last_scheduled_pipeline_id} of project {GITLAB_PROJECT_ID}."
    )
    update_tests_metadata_job = api_client.get_pipeline_job(
        GITLAB_PROJECT_ID, last_scheduled_pipeline_id, UPDATE_TESTS_METADATA_JOB_NAME
    )

    if update_tests_metadata_job is None:
        error(
            f"Job '{UPDATE_TESTS_METADATA_JOB_NAME}' couldn't be found for pipeline {last_scheduled_pipeline_id} of project {GITLAB_PROJECT_ID}"
        )
        sys.exit(0)

    update_tests_metadata_job_id = update_tests_metadata_job["id"]

    info(
        f"3. Getting '{RSPEC_FLAKY_REPORT_ARTIFACT}' artifact for the {update_tests_metadata_job_id} job of project {GITLAB_PROJECT_ID}."
    )
    rspec_flaky_report = api_client.get_job_json_artifact(
        GITLAB_PROJECT_ID, update_tests_metadata_job_id, RSPEC_FLAKY_REPORT_ARTIFACT
    )

    if rspec_flaky_report is None:
        info(
            f"Artifact '{RSPEC_FLAKY_REPORT_ARTIFACT}' couldn't be found for job {update_tests_metadata_job_id} of project {GITLAB_PROJECT_ID}"
        )
        sys.exit(0)

    flaky_tests = []

    for test_hash, test_data in rspec_flaky_report.items():
        test_data["hash"] = test_hash
        flaky_tests.append(test_data)

    logging.info("Writing JSON...")

    with open("flaky_tests.json", "w") as outfile:
        json.dump(flaky_tests, outfile)

    snowflake_engine = snowflake_engine_factory(env.copy(), "LOADER")
    snowflake_stage_load_copy_remove(
        "flaky_tests.json",
        "raw.gitlab_data_yaml.gitlab_data_yaml_load",
        "raw.gitlab_data_yaml.flaky_tests",
        snowflake_engine,
    )
