import datetime
import json
import re
import os
from os import environ as env

import pandas as pd
import sqlparse
import sql_metadata

from flatten_dict import flatten
from pprint import pprint
from sqlparse.sql import (
    Identifier,
    IdentifierList,
    remove_quotes,
    Token,
    TokenList,
    Where,
)
from sqlparse.tokens import Keyword, Name, Punctuation, String, Whitespace
from sqlparse.utils import imt

from gitlabdata.orchestration_utils import (
    dataframe_uploader,
    dataframe_enricher,
    snowflake_engine_factory,
)

import pandas as pd
from sqlalchemy.engine.base import Engine
from sqlalchemy.exc import SQLAlchemyError

## Files imported

## counter queries (sql+redis)
json_file_path = "/usr/local/analytics/transform/general/usage_ping_sql.json"

# saas_usage_ping_queries = os.path.join(os.path.dirname(__file__),"saas_usage_pings/usage_ping_metrics_sql.json")
saas_usage_ping_queries = os.path.join(
    os.path.dirname(__file__), "saas_usage_pings/all_sql_queries.json"
)

## foreign_key csv generated
# foreign_key_df = pd.read_csv(os.path.join(os.path.dirname(__file__),"saas_usage_pings/foreign_keys.csv")

## To join an event to a ultimate_namespace_id, we have 3 potential standard tables to join
# table_to_join = ['projects', 'namespaces', 'groups']

with open(saas_usage_ping_queries) as f:
    saas_queries = json.load(f)

config_dict = env.copy()
snowflake_engine_sysadmin = snowflake_engine_factory(config_dict, "SYSADMIN")
connection = snowflake_engine_sysadmin.connect()

results_all = {}

for key, query in saas_queries.items():
    try:
        results = pd.read_sql(sql=query, con=connection)
        counter_value = results["counter_value"].values[0]
        data_to_write = str(counter_value)
    except SQLAlchemyError as e:
        error = str(e.__dict__["orig"])
        data_to_write = error

    results_all[key] = data_to_write

    print(key, data_to_write)

connection.close()
snowflake_engine_sysadmin.dispose()

ping_to_upload = pd.DataFrame(columns=["query_map", "run_results", "ping_date"])

current_date = datetime.datetime.now()

ping_to_upload.loc[0] = [saas_queries, json.dumps(results_all), current_date]

snowflake_engine_loader = snowflake_engine_factory(config_dict, "LOADER")
dataframe_uploader(
    ping_to_upload, snowflake_engine_loader, "gitlab_dotcom", "saas_usage_ping"
)
snowflake_engine_loader.dispose()
