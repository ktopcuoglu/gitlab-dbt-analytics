import json
import logging
import re
import sys
import yaml
import os
from os.path import join, getsize, dirname
from os import environ as env
from subprocess import run

from pathlib import Path
from time import time
from typing import Any, Dict, List, Tuple

import pandas as pd
from snowflake.sqlalchemy import URL as snowflake_URL
from sqlalchemy import create_engine
from sqlalchemy.engine.base import Engine


def snowflake_engine_factory(
    args: Dict[str, str], role: str, schema: str = ""
) -> Engine:
    """
    Create a database engine from a dictionary of database info.
    """

    # Figure out which vars to grab
    role_dict = {
        "SYSADMIN": {
            "USER": "SNOWFLAKE_USER",
            "PASSWORD": "SNOWFLAKE_PASSWORD",
            "ACCOUNT": "SNOWFLAKE_ACCOUNT",
            "DATABASE": "SNOWFLAKE_LOAD_DATABASE",
            "WAREHOUSE": "SNOWFLAKE_LOAD_WAREHOUSE",
            "ROLE": "SYSADMIN",
        },
        "ANALYTICS_LOADER": {
            "USER": "SNOWFLAKE_LOAD_USER",
            "PASSWORD": "SNOWFLAKE_LOAD_PASSWORD",
            "ACCOUNT": "SNOWFLAKE_ACCOUNT",
            "DATABASE": "SNOWFLAKE_TRANSFORM_DATABASE",
            "WAREHOUSE": "SNOWFLAKE_LOAD_WAREHOUSE",
            "ROLE": "LOADER",
        },
        "LOADER": {
            "USER": "SNOWFLAKE_LOAD_USER",
            "PASSWORD": "SNOWFLAKE_LOAD_PASSWORD",
            "ACCOUNT": "SNOWFLAKE_ACCOUNT",
            "DATABASE": "SNOWFLAKE_LOAD_DATABASE",
            "WAREHOUSE": "SNOWFLAKE_LOAD_WAREHOUSE",
            "ROLE": "LOADER",
        },
        "CI_USER": {
            "USER": "SNOWFLAKE_USER",  ## this is the CI User
            "PASSWORD": "SNOWFLAKE_PASSWORD",
            "ACCOUNT": "SNOWFLAKE_ACCOUNT",
            "DATABASE": "SNOWFLAKE_TRANSFORM_DATABASE",
            "WAREHOUSE": "SNOWFLAKE_TRANSFORM_WAREHOUSE",
            "ROLE": "TRANSFORMER",
        },
    }

    vars_dict = role_dict[role]

    conn_string = snowflake_URL(
        user=args[vars_dict["USER"]],
        password=args[vars_dict["PASSWORD"]],
        account=args[vars_dict["ACCOUNT"]],
        database=args[vars_dict["DATABASE"]],
        warehouse=args[vars_dict["WAREHOUSE"]],
        role=vars_dict["ROLE"],  # Don't need to do a lookup on this one
        schema=schema,
    )

    return create_engine(conn_string, connect_args={"sslcompression": 0})


def snowflake_stage_load_copy_remove(
    file: str, stage: str, table_path: str, engine: Engine, type: str = "json"
) -> None:
    """
    Upload file to stage, copy to table, remove file from stage on Snowflake
    """
    file_date = file.split(".")[0]
    print(file_date)
    put_query = f"put file://{file} @{stage} auto_compress=true;"

    copy_query = f"""copy into {table_path}(jsontext, uploaded_at) 
                    from (select $1, '{file_date} 01:01:01.000'::TIMESTAMP_NTZ as uploaded_at from @{stage})
                    file_format=(type='{type}'),
                    on_error='skip_file';"""
    print(copy_query)
    remove_query = f"remove @{stage} pattern='.*.{type}.gz'"

    logging.basicConfig(stream=sys.stdout, level=20)

    try:
        connection = engine.connect()

        logging.info(f"Clearing {type} files from stage.")
        remove = connection.execute(remove_query)
        logging.info(remove)

        logging.info("Writing to Snowflake.")
        results = connection.execute(put_query)
        logging.info(results)
    finally:
        connection.close()
        engine.dispose()

    try:
        connection = engine.connect()

        logging.info(f"Copying to Table {table_path}.")
        copy_results = connection.execute(copy_query)
        logging.info(copy_results)

        logging.info(f"Removing {file} from stage.")
        remove = connection.execute(remove_query)
        logging.info(remove)
    finally:
        connection.close()
        engine.dispose()


if __name__ == "__main__":

    logging.basicConfig(stream=sys.stdout, level=20)

    config_dict = env.copy()
    snowflake_engine = snowflake_engine_factory(config_dict, "LOADER")

    for root, dirs, files in os.walk(
        "/usr/local/analytics/extract/gitlab_feature_flags_yaml"
    ):
        for file in files:
            if re.search("json", file):
                snowflake_stage_load_copy_remove(
                    file,
                    "raw.gitlab_data_yaml.gitlab_data_yaml_load",
                    "raw.gitlab_data_yaml.feature_flags",
                    snowflake_engine,
                )
