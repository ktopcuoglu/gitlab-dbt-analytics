import logging

from gitlabdata.orchestration_utils import (
    query_dataframe,
    snowflake_stage_load_copy_remove,
    snowflake_engine_factory,
    bizible_snowflake_engine_factory,
)
from typing import Dict


class BizibleSnowFlakeExtractor:
    def __init__(self, config_dict: Dict):
        """

        :param config_dict: To be passed from the execute.py, should be ENV.
        :type config_dict:
        """
        self.bizible_engine = bizible_snowflake_engine_factory(
            config_dict, "BIZIBLE_USER"
        )
        self.snowflake_engine = snowflake_engine_factory(config_dict, "LOADER")

    def get_latest_bizible_tables(self) -> list:
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
        tables_to_download = df["full_table_name"].to_list()
        return tables_to_download

    def get_bizible_queries(self, bizible_tables: list) -> Dict:
        """

        :param bizible_tables:
        :type bizible_tables:
        :return:
        :rtype:
        """
        bizible_dict = {}
        for t in bizible_tables:
            table_name = t.split(".")[-1]

            snowflake_query_max_date = f"""
                SELECT 
                    max('_modified_date') as last_modified_date
                FROM "BIZIBLE".{table_name} 
            """
            df = query_dataframe(self.snowflake_engine, snowflake_query_max_date)

            last_modified_date_list = df["last_modified_date"].to_list()

            snowflake_last_modified_date = None

            if len(last_modified_date_list) > 0 and last_modified_date_list[0]:
                snowflake_last_modified_date = last_modified_date_list[0]

            if snowflake_last_modified_date:
                bizible_dict[
                    table_name
                ] = f"SELECT * FROM {t} WHERE _last_modified_date > '{snowflake_last_modified_date}'"
            else:
                bizible_dict[table_name] = f"SELECT * FROM {t}"
        return bizible_dict

    def get_bizible_query(self, full_table_name, date_column):
        """

        :param full_table_name:
        :type full_table_name:
        """
        table_name = full_table_name.split(".")[-1]
        if len(date_column) > 0:
            snowflake_query_max_date = f"""
                            SELECT 
                                max({date_column}) as last_modified_date
                            FROM "BIZIBLE".{table_name} 
                        """
            df = query_dataframe(self.snowflake_engine, snowflake_query_max_date)

            last_modified_date_list = df["last_modified_date"].to_list()

            snowflake_last_modified_date = None

            if len(last_modified_date_list) > 0 and last_modified_date_list[0]:
                snowflake_last_modified_date = last_modified_date_list[0]

            if snowflake_last_modified_date:
                return {
                    table_name: f"SELECT * FROM GITLAB.{table_name} WHERE {date_column} > '{snowflake_last_modified_date}'"
                }
            else:
                return {table_name: f"SELECT * FROM GITLAB.{table_name}"}
        else:
            return {table_name: f"SELECT * FROM GITLAB.{table_name}"}

    def extract_latest_bizible_files(self, bizible_queries: Dict):
        """

        :param bizible_queries:
        :type bizible_queries:
        """
        for table_name in bizible_queries.keys():
            logging.info(f"Running {table_name} query")
            df = query_dataframe(self.bizible_engine, bizible_queries[table_name])

            file_name = f"{table_name}.json"
            logging.info(f"Creating {file_name}")

            df.to_csv(file_name, index=False, sep="|")

            logging.info("Writing to db")
            snowflake_stage_load_copy_remove(
                file_name,
                f"BIZIBLE.BIZIBLE_LOAD",
                f"BIZIBLE.{table_name}",
                self.snowflake_engine,
                "csv",
                file_format_options="trim_space=true field_optionally_enclosed_by = '0x22' SKIP_HEADER = 1 field_delimiter = '|' ESCAPE_UNENCLOSED_FIELD = None",
            )

    def process_bizible_query(self, query_details: Dict):
        for table_name in query_details.keys():
            file_name = f"{table_name}.json"
            logging.info(f"Running {table_name} query")
            logging.info(query_details[table_name])
            df = query_dataframe(self.bizible_engine, query_details[table_name])
            logging.info(f"Creating {file_name}")
            df.to_csv(file_name, index=False, sep="|")

            logging.info("Writing to db")
            snowflake_stage_load_copy_remove(
                file_name,
                f"BIZIBLE.BIZIBLE_LOAD",
                f"BIZIBLE.{table_name}",
                self.snowflake_engine,
                "csv",
                file_format_options="trim_space=true field_optionally_enclosed_by = '0x22' SKIP_HEADER = 1 field_delimiter = '|' ESCAPE_UNENCLOSED_FIELD = None",
            )

    def extract_latest_bizible_file(self, table_name: str, date_column: str = ''):
        """

        :param table_name:
        :type table_name:
        """

        query = self.get_bizible_query(
            full_table_name=table_name, date_column=date_column
        )
        self.process_bizible_query(query_details=query)
