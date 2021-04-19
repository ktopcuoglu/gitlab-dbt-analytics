from datetime import datetime, timedelta
import json
from os import environ as env

from typing import Any, Dict, List

from gitlabdata.orchestration_utils import (
    snowflake_engine_factory,
    snowflake_stage_load_copy_remove,
)
from qualtrics_client import QualtricsClient


def extract_survey_information(qualtrics_client, survey_id, survey_table_name):
    snowflake_engine = snowflake_engine_factory(config_dict, "LOADER")

    questions_format_list = [
        question for question in qualtrics_client.get_questions(survey_id)
    ]
    for question in questions_format_list:
        question["survey_id"] = survey_id
    if questions_format_list:
        with open("questions.json", "w") as out_file:
            json.dump(questions_format_list, out_file)

        snowflake_stage_load_copy_remove(
            "questions.json",
            "raw.qualtrics.qualtrics_nps_load",
            "raw.qualtrics.questions",
            snowflake_engine,
        )

    local_file_names = qualtrics_client.download_survey_response_file(survey_id, "json")
    for local_file_name in local_file_names:
        snowflake_stage_load_copy_remove(
            local_file_name,
            "raw.qualtrics.qualtrics_nps_load",
            f"raw.qualtrics.{survey_table_name}",
            snowflake_engine,
        )


if __name__ == "__main__":
    config_dict = env.copy()
    client = QualtricsClient(
        config_dict["QUALTRICS_API_TOKEN"], config_dict["QUALTRICS_DATA_CENTER"]
    )
    survey_id = config_dict["QUALTRICS_NPS_ID"]

    extract_survey_information(client, survey_id, "nps_survey_responses")

    survey_id = config_dict["QUALTRICS_POST_PURCHASE_ID"]

    extract_survey_information(client, survey_id, "post_purchase_survey_responses")
