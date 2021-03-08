import datetime
import json
import logging
import re
import os
import sys
from os import environ as env
from typing import Dict, List

import pandas as pd
import sqlparse
import sql_metadata

from fire import Fire
from flatten_dict import flatten
from gitlabdata.orchestration_utils import (
    dataframe_uploader,
    dataframe_enricher,
    snowflake_engine_factory,
)
from pprint import pprint
from sqlalchemy.engine.base import Engine
from sqlalchemy.exc import SQLAlchemyError
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


class UsagePing(object):
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
            os.path.join(
                os.path.dirname(__file__), "all_sql_queries.json"
            )
        ) as f:
            saas_queries = json.load(f)

        return saas_queries

    def saas_instance_ping(self):
        """
        Take a dictionary of {ping_name: sql_query} and run each
        query to then upload to a table in raw.
        """
        saas_queries = self._get_instance_queries()

        connection = self.loader_engine.connect()

        results_all = {}

        for key, query in saas_queries.items():
            logging.info(f"Running ping {key}...")
            try:
                results = pd.read_sql(sql=query, con=connection)
                counter_value = results.loc[0, "counter_value"]
                data_to_write = str(counter_value)
            except SQLAlchemyError as e:
                error = str(e.__dict__["orig"])
                data_to_write = error

            results_all[key] = data_to_write

        connection.close()
        self.loader_engine.dispose()

        ping_to_upload = pd.DataFrame(columns=["query_map", "run_results", "ping_date"])

        ping_to_upload.loc[0] = [saas_queries, json.dumps(results_all), self.end_date]

        dataframe_uploader(
            ping_to_upload, self.loader_engine, "gitlab_dotcom", "saas_usage_ping"
        )

        self.loader_engine.dispose()

    def _get_namespace_queries(self) -> List[Dict]:
        """
        can be updated to query an end point or query other functions
        to generate:
        {
            { couner_name: ping_name,
              counter_query: sql_query,
              time_window_query: true,
              level: namespace,
            }
        }
        """
        with open(
            os.path.join(
                os.path.dirname(__file__), "usage_ping_namespace_queries.json"
            )
        ) as f:
            saas_queries = json.load(f)

        return saas_queries

    def saas_namespace_ping(self):
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
                continue

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

        connection.close()
        self.loader_engine.dispose()


if __name__ == "__main__":
    logging.basicConfig(stream=sys.stdout, level=20)
    Fire(UsagePing)
    logging.info("Done with pings.")
