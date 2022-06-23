from os import environ as env
import requests
from pandas import DataFrame

from gitlabdata.orchestration_utils import (
    snowflake_engine_factory,
    snowflake_stage_load_copy_remove,
    dataframe_uploader,
)

config_dict = env.copy()

base_url = env.get('MAILGUN_BASE_URL')
api_key = env.get("MAILGUN_API_KEY")

def get_stats():
    return requests.get(
        f"{base_url}/stats/total",
        auth=("api", api_key),
        params={"event": ["accepted", "delivered", "failed"],"duration": "1m"})


if __name__ == "__main__":
    get_stats()

