import pandas as pd
import requests
from datetime import datetime
from dateutil import parser as date_parser
from gitlabdata.orchestration_utils import (
    snowflake_engine_factory,
    snowflake_stage_load_copy_remove,
    dataframe_uploader,
)
from os import environ as env
from typing import Any

config_dict = env.copy()

# Year date range for mapping dates passed on JSON.
years_back = 15
valid_years = range(datetime.now().year - 15, datetime.now().year + 1)

year = datetime.now().year


def recursive_parse_dict(dict_to_parse: Any, path: str = ""):
    """
    Parses dicts of dicts, specifically for the keyhole extract.
    Written as a recursive function so new data endpoints can be added without any changes required.
    """

    for key in dict_to_parse.keys():
        field = dict_to_parse.get(key)

        dtype = type(field)

        if dtype == dict:

            if len(path) > 1:
                path = path + "_" + key
            else:
                path = path + key

            if str.isdigit(key) and int(key) in valid_years:
                print(key)

                new_fields = {
                    date_parser.parse(f"{k} {key} 01"): value
                    for k, value in field.items()
                }
                field = new_fields

                path = path[: path.find(key) - 1]

            yield from recursive_parse_dict(field, path)
        else:

            return_data = {"path": path, "field": key, "value": field}

            yield (return_data)


def get_twitter_impressions_data(endpoint: str) -> pd.DataFrame:
    """
    Retrieves twitter data from internally setup keyhole endpoint
    """
    raw_data = requests.get(endpoint).json()
    data = [d for d in recursive_parse_dict(raw_data) if d is not None]
    output_df = pd.DataFrame(data)
    return output_df


def write_csv_data(file_name: str, data: pd.DataFrame):
    """
    Just here to return filenames, probably not needed but consistent with our other extracts
    """
    if data.to_csv(f"{file_name}.csv", index=False):
        return file_name
    else:
        return False


if __name__ == "__main__":
    snowflake_engine = snowflake_engine_factory(config_dict, "LOADER")

    api_endpoint = (
        "https://gitlab-com.gitlab.io/marketing/corporate_marketing/developer-evangelism/code/de-dashboard"
        "/metrics/data.json"
    )
    output_df = get_twitter_impressions_data(api_endpoint)

    # Groups by date so we can create a file for each day
    df_by_path = output_df.groupby(by="path")

    [write_csv_data(file_name, data) for file_name, data in df_by_path]
