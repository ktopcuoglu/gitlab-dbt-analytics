import json
import sys
import logging
import requests

from typing import Dict, Any
from logging import error, info, basicConfig, getLogger, warning
from os import environ as env

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
    RSPEC_FLAKY_REPORT_URL = (
        "https://gitlab-org.gitlab.io/gitlab/rspec_flaky/report-suite.json"
    )

    logging.basicConfig(stream=sys.stdout, level=20)

    r = requests.get(
        RSPEC_FLAKY_REPORT_URL, timeout=120, headers={"Accept": "application/json"}
    )
    r.raise_for_status()

    rspec_flaky_report = r.json()

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
