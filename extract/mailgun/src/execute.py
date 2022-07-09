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
from logging import info, basicConfig, getLogger
from os import environ as env
from typing import Dict, List

config_dict = env.copy()

api_key = env.get("MAILGUN_API_KEY")
domains = ["mg.gitlab.com"]


def reformat_data(items: List[Dict]) -> List[Dict]:
    """
    Extracts the fields we want from the nested json response.
    :param items:
    :return:
    """
    formatted_data = []
    if items and len(items) > 0:
        for i in items:
            new_dict = {
                "id": i.get("id", ""),
                "message-id": i.get("message", {})
                .get("headers", {})
                .get("message-id", ""),
                "timestamp": i.get("timestamp", ""),
                "tags": i.get("tags", ""),
                "event": i.get("event", ""),
                "delivery-status-code": i.get("delivery-status", {}).get("code", ""),
                "delivery-status-message": i.get("delivery-status", {}).get(
                    "description", ""
                ),
                "log-level": i.get("log-level", ""),
                "url": i.get("url", ""),
                "recipient": i.get("recipient", ""),
                "sender": i.get("envelope", {}).get("sender", ""),
                "targets": i.get("envelope", {}).get("targets", ""),
                "subject": i.get("message", {}).get("headers").get("subject", ""),
                "city": i.get("geolocation", {}).get("city", ""),
                "region": i.get("geolocation", {}).get("region", ""),
                "country": i.get("geolocation", {}).get("country", ""),
                "is-routed": i.get("flags", {}).get("is-routed", ""),
                "is-authenticated": i.get("flags", {}).get("is-authenticated", ""),
                "is-system-test": i.get("flags", {}).get("is-system-test", ""),
                "is-test-mode": i.get("flags", {}).get("is-test-mode", ""),
            }
            formatted_data.append(new_dict)

    return formatted_data


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
                data = requests.get(page_token, auth=("api", api_key)).json()
                items = data.get("items")
                formatted_data = reformat_data(items)
                info(f"Data retrieved length: {len(formatted_data)}")
                if len(formatted_data) == 0:
                    break
                all_results = all_results[:] + formatted_data[:]

            else:
                data = get_logs(domain, event, formatted_date).json()
                items = data.get("items")
                formatted_data = reformat_data(items)
                info(f"Data retrieved length: {len(formatted_data)}")
                if len(formatted_data) == 0:
                    break
                all_results = all_results[:] + formatted_data[:]

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
        start_date = datetime.datetime.now() - datetime.timedelta(hours=30)

    results = extract_logs(event, start_date)

    info(f"Results length: {len(results)}")

    with open(file_name, "w") as outfile:
        json.dump(results, outfile)

    snowflake_stage_load_copy_remove(
        file_name, "mailgun.mailgun_load", "mailgun.mailgun_events", snowflake_engine
    )


if __name__ == "__main__":
    basicConfig(stream=sys.stdout, level=20)
    getLogger("snowflake.connector.cursor").disabled = True
    Fire({"load_event_logs": load_event_logs})
    info("Complete.")
