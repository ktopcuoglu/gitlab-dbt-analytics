from os import environ as env
import requests
import json
import datetime
from pandas import DataFrame
from typing import Dict, Tuple, List
from api import get_logs
from email import utils

from gitlabdata.orchestration_utils import (
    snowflake_engine_factory,
    snowflake_stage_load_copy_remove,
    dataframe_uploader,
)

config_dict = env.copy()

api_key = env.get("MAILGUN_API_KEY")

domains = ['mg.gitlab.com']
events = [
    'rejected',
    'delivered',
    'failed',
    'opened',
    'clicked',
    'unsubscribed',
    'complained'
]


def reformat_data(items: List[Dict]):
    formatted_data = []
    if items and len(items) > 0:
        for i in items:
            new_dict = {
                "id": i.get('id'),
                "message-id": i.get('message').get('headers').get('message-id'),
                "timestamp": i.get('timestamp'),
                "tags": i.get('tags'),
                "event": i.get('event'),
                "delivery-status-code": i.get('delivery-status').get('code'),
                "delivery-status-message": i.get('delivery-status').get('message'),
                "log-level": i.get('log-level'),
                "url": i.get('url'),
                "recipient": i.get('recipient'),
                "sender": i.get('envelope').get('sender'),
                "targets": i.get('envelope').get('targets'),
                "subject": i.get('message').get('headers').get('subject'),
                "city": i.get('geolocation').get('city'),
                "region": i.get('geolocation').get('region'),
                "country": i.get('geolocation').get('country'),
                "is-routed": i.get('flags').get('is-routed'),
                "is-authenticated": i.get('flags').get('is-authenticated'),
                "is-system-test": i.get('flags').get('is-system-test'),
                "is-test-mode": i.get('flags').get('is-test-mode'),
            }
            formatted_data.append(new_dict)

    return formatted_data


def extract_logs(event):
    page_token = None
    all_results: List[Dict] = []

    start_date = datetime.datetime.strptime('2021-02-01', '%Y-%m-%d')
    formatted_date = utils.format_datetime(start_date)

    for domain in domains:

        while True:
            if page_token:
                data = requests.get(
                        page_token,
                        auth=("api", api_key)).json()

                items = data.get('items')
                formatted_data = reformat_data(items)
                if len(formatted_data) == 0:
                    break
                all_results = all_results[:] + formatted_data[:]

            else:
                data = get_logs(api_key, domain, event, formatted_date).json()
                items = data.get('items')
                formatted_data = reformat_data(items)
                if len(formatted_data) == 0:
                    break
                all_results = all_results[:] + formatted_data[:]

            page_token = data.get("paging").get('next')

            if not page_token:
                break

    return all_results


if __name__ == "__main__":
    snowflake_engine = snowflake_engine_factory(config_dict, "LOADER")
    for event in events:
        file_name = f'{event}.json'
        results = extract_logs(event)

        with open(file_name, 'w') as outfile:
            json.dump(results, outfile)

        snowflake_stage_load_copy_remove(
                file_name, "mailgun.mailgun_load", "mailgun.events", snowflake_engine
        )

