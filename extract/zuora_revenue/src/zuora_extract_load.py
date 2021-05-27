import sys
from os import environ as env
import time
import logging
from fire import Fire
from typing import Dict, Tuple, List
from yaml import load, safe_load, YAMLError

import pandas as pd

from google.cloud import storage
from google.oauth2 import service_account
from sqlalchemy.engine.base import Engine

from gitlabdata.orchestration_utils import (
    snowflake_engine_factory,
    query_executor,
)


def zuora_revenue_extract(table_name: str) -> None:
    logging.basicConfig(stream=sys.stdout, level=20)
    logging.info("Prepare the authentication URL")
    headers = {
        "role": "APIRole",
        "clientname": "Default",
        "Authorization": env["ZUORA_REVENUE_AUTH_CODE"],
    }
    authenticate_url_zuora_revpro = (
        "https://" + env["ZUORA_REVENUE_API_URL"] + "/api/integration/v1/authenticate"
    )
    zuora_fetch_data_url = (
        "https://" + env["ZUORA_REVENUE_API_URL"] + "/api/integration/v2/biviews/"
    )


def zuora_revenue_load(
    bucket: str,
    schema: str,
    table_name: str,
    gapi_keyfile: str = None,
    conn_dict: Dict[str, str] = None,
    compression: str = None,
) -> None:
    # Set some vars
    engine = snowflake_engine_factory(conn_dict or env, "LOADER", schema)

    upload_query = f"""
        copy into {table_name}
        from @zuora_revenue_staging/RAW_DB/staging/{table_name} 
        pattern= '.*{table_name}_.*[.]csv'
    """

    results = query_executor(engine, upload_query)
    if results[1] == "LOADED":
        total_rows = results[2]

    log_result = f"Loaded {total_rows} rows for table {table_name}"
    logging.info(log_result)


if __name__ == "__main__":
    logging.basicConfig(stream=sys.stdout, level=20)
    logging.getLogger("snowflake.connector.cursor").disabled = True
    # Copy all environment variables to dict.
    config_dict = env.copy()
    Fire(
        {
            "zuora_extract": zuora_revenue_extract,
            "zuora_load": zuora_revenue_load,
        }
    )
