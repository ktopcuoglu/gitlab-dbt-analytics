""" Extracts data from the MailGun API event stream """
import datetime
import json
import requests
import sys
from email import utils
from fire import Fire
from gitlabdata.orchestration_utils import (
    snowflake_engine_factory,
    snowflake_stage_load_copy_remove,
)
from logging import info, basicConfig, getLogger, error
from os import environ as env
from typing import Dict, List

config_dict = env.copy()

api_key = env.get("MAILGUN_API_KEY")
domains = ["mg.gitlab.com"]


def get_logs(domain: str, event: str, formatted_date: str) -> requests.Response:
    """
    Small convenience wrapper function for mailgun event requests,
    :param domain:
    :param event:
    :param formatted_date:
    :return:
    """
    return requests.get(
        f"https://api.mailgun.net/v3/{domain}/events",
        auth=("api", api_key),
        params={"begin": formatted_date, "ascending": "yes", "event": event},
    )


def extract_logs(event: str, start_date: datetime.datetime) -> List[Dict]:
    """
    Requests and retrieves the event logs for a particular event.
    :param start_date:
    :param event:
    :return:
    """
    page_token = None
    all_results: List[Dict] = []

    formatted_date = utils.format_datetime(start_date)

    for domain in domains:

        while True:
            if page_token:
                response = requests.get(page_token, auth=("api", api_key))
                try:
                    data = response.json()
                except json.decoder.JSONDecodeError:
                    error("No response received")
                    break

                items = data.get("items")

                info(f"Data retrieved length: {len(items)}")

                if len(items) == 0:
                    break

                first_timestamp = items[0].get("timestamp")
                str_stamp = datetime.datetime.fromtimestamp(first_timestamp).strftime(
                    "%d-%m-%Y %H:%M:%S.%f"
                )
                info(f"Processed data starting on {str_stamp}")

                all_results = all_results[:] + items[:]

            else:
                response = get_logs(domain, event, formatted_date)

                try:
                    data = response.json()
                except json.decoder.JSONDecodeError:
                    error("No response received")
                    break

                items = data.get("items")
                info(f"Data retrieved length: {len(items)}")

                if len(items) == 0:
                    break

                all_results = all_results[:] + items[:]

            page_token = data.get("paging").get("next")

            if not page_token:
                break

    return all_results


def load_event_logs(event: str, full_refresh: bool = False):
    """
    CLI main function, starting point for setting up engine and processing data.
    :param event:
    :param full_refresh:
    """
    snowflake_engine = snowflake_engine_factory(config_dict, "LOADER")

    file_name = f"{event}.json"

    if full_refresh:
        start_date = datetime.datetime(2021, 2, 1)
    else:
        start_date = datetime.datetime.now() - datetime.timedelta(hours=16)

    results = extract_logs(event, start_date)

    info(f"Results length: {len(results)}")

    with open(file_name, "w") as outfile:
        json.dump(results, outfile)

    snowflake_stage_load_copy_remove(
        file_name,
        f"mailgun.mailgun_load_{event}",
        "mailgun.mailgun_events",
        snowflake_engine,
    )


if __name__ == "__main__":
    basicConfig(stream=sys.stdout, level=20)
    getLogger("snowflake.connector.cursor").disabled = True
    Fire({"load_event_logs": load_event_logs})
    info("Complete.")
