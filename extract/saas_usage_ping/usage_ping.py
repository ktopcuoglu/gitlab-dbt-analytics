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

    def _get_instance_queries(self) -> Dict:
        """
        can be updated to query an end point or query other functions
        to generate the {ping_name: sql_query} dictionary
        """
        with open(
            os.path.join(os.path.dirname(__file__), "transformed_instance_queries.json")
        ) as f:
            saas_queries = json.load(f)

        return saas_queries

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

    def saas_instance_ping(self):
        """
        Take a dictionary of {ping_name: sql_query} and run each
        query to then upload to a table in raw.
        """
        saas_queries = self._get_instance_queries()

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

        ping_to_upload = pd.DataFrame(
            columns=["query_map", "run_results", "ping_date", "run_id"]
        )

        ping_to_upload.loc[0] = [
            saas_queries,
            json.dumps(results_all),
            self.end_date,
            self._get_md5(datetime.datetime.utcnow().timestamp()),
        ]

        dataframe_uploader(
            ping_to_upload,
            self.loader_engine,
            "instance_sql_metrics",
            "saas_usage_ping",
        )

        """
        Handling error data part to load data into table: raw.saas_usage_ping.instance_sql_errors
        """
        if errors_data_all:
            error_data_to_upload = pd.DataFrame(
                columns=["run_id", "sql_errors", "ping_date"]
            )

            error_data_to_upload.loc[0] = [
                self._get_md5(datetime.datetime.utcnow().timestamp()),
                json.dumps(errors_data_all),
                self.end_date,
            ]

            dataframe_uploader(
                error_data_to_upload,
                self.loader_engine,
                "instance_sql_errors",
                "saas_usage_ping",
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

        redis_data_to_upload = pd.DataFrame(columns=["jsontext", "ping_date", "run_id"])

        redis_data_to_upload.loc[0] = [
            json.dumps(json_data),
            self.end_date,
            self._get_md5(datetime.datetime.utcnow().timestamp()),
        ]

        dataframe_uploader(
            redis_data_to_upload,
            self.loader_engine,
            "instance_redis_metrics",
            "saas_usage_ping",
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
        with open(
            os.path.join(os.path.dirname(__file__), "usage_ping_namespace_queries.json")
        ) as namespace_file:
            saas_queries = json.load(namespace_file)

        return saas_queries

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

        dataframe_uploader(
            results,
            self.loader_engine,
            "gitlab_dotcom_namespace",
            "saas_usage_ping",
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
