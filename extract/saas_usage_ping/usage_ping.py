from os import environ as env
from typing import Dict, List
from hashlib import md5

from logging import info
import datetime
import json
import logging
import os
import sys
import requests
import pandas as pd


from transform_instance_level_queries_to_snowsql import (
    META_API_COLUMNS,
    TRANSFORMED_INSTANCE_QUERIES_FILE,
    USAGE_PING_NAMESPACE_QUERIES_FILE,
    META_DATA_INSTANCE_QUERIES_FILE,
    METRICS_EXCEPTION,
)
from fire import Fire
from gitlabdata.orchestration_utils import (
    dataframe_uploader,
    snowflake_engine_factory,
)
from sqlalchemy.exc import SQLAlchemyError


class UsagePing(object):
    """
    Usage ping class represent as an umbrella
    to sort out service ping data import
    """

    def __init__(self, ping_date=None):
        self.config_vars = env.copy()
        self.loader_engine = snowflake_engine_factory(self.config_vars, "LOADER")

        if ping_date is not None:
            self.end_date = datetime.datetime.strptime(ping_date, "%Y-%m-%d").date()
        else:
            self.end_date = datetime.datetime.now().date()

        self.start_date_28 = self.end_date - datetime.timedelta(28)
        self.dataframe_api_columns = META_API_COLUMNS

    def _get_instance_sql_queries(self) -> Dict:
        """
        can be updated to query an end point or query other functions
        to generate the {ping_name: sql_query} dictionary
        """

        instance_sql_queries = self._get_json_data_from_file(
            TRANSFORMED_INSTANCE_QUERIES_FILE
        )

        # exclude metrics we do not want to track
        instance_sql_queries = {
            metrics_name: metrics_sql
            for metrics_name, metrics_sql in instance_sql_queries.items()
            if not metrics_name.lower() in METRICS_EXCEPTION
        }

        return instance_sql_queries

    def _get_dataframe_api_values(self, input_json: dict) -> list:
        """
        pick up values from .json file defined in dataframe_api_columns
        and return them as a list

        param input_json: dict
        return: list
        """
        dataframe_api_value_list = [
            input_json.get(dataframe_api_column, "")
            for dataframe_api_column in self.dataframe_api_columns
        ]

        return dataframe_api_value_list

    def _get_md5(
        self, input_timestamp: float = datetime.datetime.utcnow().timestamp()
    ) -> str:
        """
        Convert input datetime into md5 hash.
        Result is returned as a string.
        Example:

            Input (datetime): datetime.utcnow().timestamp()
            Output (str): md5 hash

            -----------------------------------------------------------
            current timestamp: 1629986268.131019
            md5 timestamp: 54da37683078de0c1360a8e76d942227
        """
        encoding = "utf-8"
        timestamp_encoded = str(input_timestamp).encode(encoding=encoding)

        return md5(timestamp_encoded).hexdigest()

    def _upload_dataframe(
        self,
        usage_ping_dataframe: pd.DataFrame,
        table_name: str,
        schema_name="saas_usage_ping",
    ) -> None:
        """
        Routine to upload DataFrame to Snowflake
        """

        dataframe_uploader(
            dataframe=usage_ping_dataframe,
            engine=self.loader_engine,
            schema=schema_name,
            table_name=table_name,
        )

    def _prepare_dataframe_to_upload(self, columns: list, content: list):
        """
        Make a proper structure for dataframe to upload into snowflake
        """
        prepared_dataframe = pd.DataFrame(columns)

        prepared_dataframe.loc[0] = content

        return prepared_dataframe

    def _get_json_data_from_file(self, file_name: str) -> dict:
        """
        Load meta data from .json file from the file system
        param file_name: str
        return: dict
        """
        with open(os.path.join(os.path.dirname(__file__), file_name)) as file:
            meta_data = json.load(file)

        return meta_data

    def saas_instance_ping(self):
        """
        Take a dictionary of {ping_name: sql_query} and run each
        query to then upload to a table in raw.
        """
        saas_queries = self._get_instance_sql_queries()

        connection = self.loader_engine.connect()

        results_all = {}
        errors_data_all = {}

        for key, query in saas_queries.items():
            logging.info(f"Running ping: {key}...")
            try:
                data_to_write = error_data_to_write = None
                results = pd.read_sql(sql=query, con=connection)
                info(results)
                counter_value = results.loc[0, "counter_value"]
                data_to_write = str(counter_value)
            except KeyError as k:
                data_to_write = "0"
            except SQLAlchemyError as e:
                error_data_to_write = str(e.__dict__["orig"])

            if data_to_write:
                results_all[key] = data_to_write

            if error_data_to_write:
                errors_data_all[key] = error_data_to_write

        info("Processed queries")
        connection.close()
        self.loader_engine.dispose()

        columns = [
            "query_map",
            "run_results",
            "ping_date",
            "run_id",
        ] + self.dataframe_api_columns

        content = [
            saas_queries,
            json.dumps(results_all),
            self.end_date,
            self._get_md5(datetime.datetime.utcnow().timestamp()),
        ] + self._get_dataframe_api_values(
            self._get_json_data_from_file(META_DATA_INSTANCE_QUERIES_FILE)
        )

        instance_sql_metrics_to_upload = self._prepare_dataframe_to_upload(
            columns=columns, content=content
        )

        self._upload_dataframe(
            usage_ping_dataframe=instance_sql_metrics_to_upload,
            table_name="instance_sql_metrics",
        )

        """
        Handling error data part to load data into table: raw.saas_usage_ping.instance_sql_errors
        """
        if errors_data_all:
            error_columns = ["run_id", "sql_errors", "ping_date"]
            error_content = [
                self._get_md5(datetime.datetime.utcnow().timestamp()),
                json.dumps(errors_data_all),
                self.end_date,
            ]
            error_data_to_upload = self._prepare_dataframe_to_upload(
                columns=error_columns, content=error_content
            )

            self._upload_dataframe(
                usage_ping_dataframe=error_data_to_upload,
                table_name="instance_sql_errors",
            )

        self.loader_engine.dispose()

    def saas_instance_redis_metrics(self):

        """
        Call the Non SQL Metrics API and store the results in Snowflake RAW database
        """
        config_dict = env.copy()
        headers = {
            "PRIVATE-TOKEN": config_dict["GITLAB_ANALYTICS_PRIVATE_TOKEN"],
        }

        response = requests.get(
            "https://gitlab.com/api/v4/usage_data/non_sql_metrics", headers=headers
        )
        json_data = json.loads(response.text)

        columns = ["jsontext", "ping_date", "run_id"] + self.dataframe_api_columns
        content = [
            json.dumps(json_data),
            self.end_date,
            self._get_md5(datetime.datetime.utcnow().timestamp()),
        ] + self._get_dataframe_api_values(json_data)

        redis_data_to_upload = self._prepare_dataframe_to_upload(
            columns=columns, content=content
        )

        self._upload_dataframe(
            usage_ping_dataframe=redis_data_to_upload,
            table_name="instance_redis_metrics",
        )

    def _get_namespace_queries(self) -> List[Dict]:
        """
        can be updated to query an end point or query other functions
        to generate:
        {
            { counter_name: ping_name,
              counter_query: sql_query,
              time_window_query: true,
              level: namespace,
            }
        }
        """

        namespace_queries = self._get_json_data_from_file(
            USAGE_PING_NAMESPACE_QUERIES_FILE
        )

        return namespace_queries

    def process_namespace_ping(self, query_dict, connection):
        base_query = query_dict.get("counter_query")
        ping_name = query_dict.get("counter_name", "Missing Name")
        logging.info(f"Running ping {ping_name}...")

        if query_dict.get("time_window_query", False):
            base_query = base_query.replace(
                "between_end_date", f"'{str(self.end_date)}'"
            )
            base_query = base_query.replace(
                "between_start_date", f"'{str(self.start_date_28)}'"
            )

        if "namespace_ultimate_parent_id" not in base_query:
            logging.info(f"Skipping ping {ping_name} due to no namespace information.")
            return

        try:
            # Expecting [id, namespace_ultimate_parent_id, counter_value]
            results = pd.read_sql(sql=base_query, con=connection)
            error = "Success"
        except SQLAlchemyError as e:
            error = str(e.__dict__["orig"])
            results = pd.DataFrame(
                columns=["id", "namespace_ultimate_parent_id", "counter_value"]
            )
            results.loc[0] = [None, None, None]

        results["ping_name"] = ping_name
        results["level"] = query_dict.get("level", None)
        results["query_ran"] = base_query
        results["error"] = error
        results["ping_date"] = self.end_date

        self._upload_dataframe(
            usage_ping_dataframe=results, table_name="gitlab_dotcom_namespace"
        )

    def saas_namespace_ping(self, filter=lambda _: True):
        """
        Take a dictionary of the following type and run each
        query to then upload to a table in raw.
        {
            ping_name:
            {
              query_base: sql_query,
              level: namespace,
              between: true
            }
        }
        """
        saas_queries = self._get_namespace_queries()

        connection = self.loader_engine.connect()

        for query_dict in saas_queries:
            if filter(query_dict):
                self.process_namespace_ping(query_dict, connection)

        connection.close()
        self.loader_engine.dispose()

    def backfill(self):
        """
        Routine to backfilling data for namespace ping
        """
        filter = lambda query_dict: query_dict.get("time_window_query", False)
        self.saas_namespace_ping(filter)


if __name__ == "__main__":
    logging.basicConfig(stream=sys.stdout, level=20)
    Fire(UsagePing)
    logging.info("Done with pings.")
