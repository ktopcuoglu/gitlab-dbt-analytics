import argparse
import json
import logging
import os
import sys

from api import DiscourseClient
from yaml import load, safe_load, YAMLError

from gitlabdata.orchestration_utils import (
    snowflake_engine_factory,
    snowflake_stage_load_copy_remove,
)


def fixup_datetime_string_format(datetime_string: str) -> str:
    """
    Take datetime as formatted in iso6801, and replace the timezone with "Z"
    This function is necessary because the Thanos api doesn't accept timezones in the format airflow provides them in.
    """
    return datetime_string[:-6] + "Z"


DISCOURSE_API_TOKEN = os.environ.get("DISCOURSE_API_TOKEN")

if __name__ == "__main__":

    logging.basicConfig(stream=sys.stdout, level=20)

    parser = argparse.ArgumentParser()
    parser.add_argument("--start_date")
    parser.add_argument("--end_date")
    parser.add_argument("--months_ago")
    parser.add_argument("--reports_yml")
    args = parser.parse_args()

    config_dict = os.environ.copy()
    snowflake_engine = snowflake_engine_factory(config_dict, "LOADER")

    with open(args.reports_yml, "r") as file:
        try:
            stream = safe_load(file)
        except YAMLError as exc:
            print(exc)

        reports = {report["name"]: report["path"] for report in stream["reports"]}

    params = {
        "start_date": fixup_datetime_string_format(args.start_date),
        "end_date": fixup_datetime_string_format(args.end_date),
        "months_ago": args.months_ago,
    }

    api_client = DiscourseClient(api_token=DISCOURSE_API_TOKEN)

    for report, path in reports.items():
        logging.info(
            f"Processing {report} with start date {params['start_date']} "
            f"and end date {params['end_date']} "
        )
        file_name = f"{report}.json"
        with open(file_name, "w") as outfile:
            data = api_client.get_json(endpoint=path, params=params)
            json.dump(data, outfile)

        snowflake_stage_load_copy_remove(
            file=file_name,
            stage=f"raw.discourse.discourse_load",
            table_path=f"raw.discourse.{report}",
            engine=snowflake_engine,
        )
