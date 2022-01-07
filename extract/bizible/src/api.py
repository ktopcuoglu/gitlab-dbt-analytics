import json
import logging
import os
import sys

from pathlib import Path
from time import time
from typing import Any, Dict, List, Tuple

import pandas as pd
import yaml
from snowflake.sqlalchemy import URL as snowflake_URL
from sqlalchemy import create_engine
from sqlalchemy.engine.base import Engine
from yaml.loader import SafeLoader

from gitlabdata.orchestration_utils import (
    data_science_engine_factory,
    query_dataframe,
    query_executor,
    snowflake_stage_load_copy_remove,
    snowflake_engine_factory,
)
from typing import Dict

class BizibleSnowFlakeExtractor:
    def __init__(self, config_dict: Dict):
        """

        :param config_dict: To be passed from the execute.py, should be ENV.
        :type config_dict:
        """
        self.bizible_engine = snowflake_engine_factory(config_dict, "BIZIBLE_USER")
        self.snowflake_engine = snowflake_engine_factory(config_dict, "LOADER")


    def get_latest_bizible_tables(self) -> object:
        """

        :param bizible_engine:
        :type bizible_engine:
        :return:
        :rtype:
        """
        bizible_tables_query = f"""
            SELECT TABLE_CATALOG || '.' || TABLE_SCHEMA || '.' || TABLE_NAME as FULL_TABLE_NAME
            FROM BIZIBLE_ROI_V3.INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA = 'GITLAB'
        """

        df = query_dataframe(self.bizible_engine, bizible_tables_query)
        tables_to_download = df['full_table_name'].to_list()
        return tables_to_download

    def get_bizible_queries(self, bizible_tables: object) -> object:
        """

        :param bizible_tables:
        :type bizible_tables:
        :return:
        :rtype:
        """
        bizible_dict = {}
        for t in bizible_tables:
            table_name = t.split('.')[-1]

            snowflake_query_max_date = f"""
                SELECT 
                    max(to_timestamp(d.value['_modified_date']::INT / 1000)) as last_modified_date
                FROM "BIZIBLE_WAREHOUSE_IMPORT_RAW"."BIZIBLE".{table_name}, 
                LATERAL FLATTEN(INPUT => PARSE_JSON(jsontext), outer => true) d
            """
            df = query_dataframe(self.snowflake_engine, snowflake_query_max_date)

            last_modified_date_list = df['last_modified_date'].to_list()

            snowflake_last_modified_date = None

            if len(last_modified_date_list) > 0 and last_modified_date_list[0]:
                snowflake_last_modified_date = last_modified_date_list[0]

            if snowflake_last_modified_date:
                bizible_dict[table_name] = f"SELECT * FROM {t} WHERE _last_modified_date > '{snowflake_last_modified_date}'"
            else:
                bizible_dict[table_name] = f"SELECT * FROM {t}"
        return bizible_dict

    def extract_latest_bizible_files(self, bizible_queries: Dict):
        """

        :param bizible_queries:
        :type bizible_queries:
        """
        for table_name in bizible_queries.keys():
            file_name = f"{table_name}.json"
            logging.info(f"Running {table_name} query")
            df = query_dataframe(self.bizible_engine, bizible_queries[table_name])
            logging.info(f"Creating {file_name}")
            df.to_json(file_name, orient="records")

            logging.info("Writing to db")
            snowflake_stage_load_copy_remove(
                    file_name,
                    f"BIZIBLE_WAREHOUSE_IMPORT_RAW.BIZIBLE.BIZIBLE_LOAD",
                    f"BIZIBLE_WAREHOUSE_IMPORT_RAW.BIZIBLE.{table_name}",
                    self.snowflake_engine,
            )
