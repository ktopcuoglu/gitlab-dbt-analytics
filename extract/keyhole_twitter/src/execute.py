from os import environ as env
import requests
import pandas as pd
from datetime import datetime
from dateutil import parser as date_parser

from gitlabdata.orchestration_utils import (
    snowflake_engine_factory,
    snowflake_stage_load_copy_remove,
    dataframe_uploader,
)

config_dict = env.copy()

years_back = 15
valid_years = range(datetime.now().year - 15, datetime.now().year + 1)

year = datetime.now().year

def recursive_parse_dict(dict_to_parse, path=""):
    for key in dict_to_parse.keys():
        field = dict_to_parse.get(key)

        dtype = type(field)

        if dtype == dict:

            if len(path) > 1:
                path = path + "_" + key
            else:
                path = path + key

            if str.isdigit(key) \
                    and int(key) in valid_years:
                print(key)

                new_fields = {date_parser.parse(f"{k} {key} 01"): value for k, value in field.items()}
                field = new_fields

                path = path[:path.find(key) - 1]

            yield from recursive_parse_dict(field, path)
        else:

            return_data = {
                "path" : path,
                "field": key,
                "value": field
            }

            yield (return_data)

def get_twitter_impressions_data(endpoint) -> pd.DataFrame:
    raw_data = requests.get(endpoint).json()
    data = [d for d in recursive_parse_dict(raw_data)]
    output_df = pd.DataFrame(data)
    return output_df

def write_csv_data(file_name, data):
    if data.to_csv(f"{file_name}.csv"):
        return file_name
    else:
        return False


if __name__ == "__main__":
    snowflake_engine = snowflake_engine_factory(config_dict, "LOADER")

    endpoint = "https://gitlab-com.gitlab.io/marketing/corporate_marketing/developer-evangelism/code/de-dashboard" \
            "/metrics/data.json"
    output_data = get_twitter_impressions_data(endpoint)

    # Groups by date so we can create a file for each day
    df_by_path = output_df.groupby(by="path")

    [write_csv_data(file_name, data) for file_name, data in df_by_path]


