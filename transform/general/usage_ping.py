import datetime
import json
import re
import os
from os import environ as env
from typing import Dict, List

import pandas as pd
import sqlparse
import sql_metadata

from fire import Fire
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


class UsagePing(object):
    def __init__(self, ping_date=None):
        self.config_vars = env.copy()
        self.sysadmin_engine = snowflake_engine_factory(self.config_vars, "SYSADMIN")
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
                os.path.dirname(__file__), "saas_usage_pings/all_sql_queries.json"
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

        connection = self.sysadmin_engine.connect()

        results_all = {"testing": "datemath"}

        for key, query in saas_queries.items():
                try:
                    results = pd.read_sql(sql=query, con=connection)
                    counter_value = results["counter_value"].values[0]
                    data_to_write = str(counter_value)
                except SQLAlchemyError as e:
                    error = str(e.__dict__["orig"])
                    data_to_write = error

                results_all[key] = data_to_write

        connection.close()
        self.sysadmin_engine.dispose()

        ping_to_upload = pd.DataFrame(columns=["query_map", "run_results", "ping_date"])

        ping_to_upload.loc[0] = [saas_queries, json.dumps(results_all), self.end_date]

        dataframe_uploader(
            ping_to_upload, self.loader_engine, "gitlab_dotcom", "saas_usage_ping"
        )

        self.loader_engine.dispose()

    def _get_level_queries(self) -> Dict:
        """
        can be updated to query an end point or query other functions
        to generate:
        {
            ping_name: 
            {
              query_base: sql_query,
              level: namespace,
              between: true
            }
        }
        """
        # with open(
        #     os.path.join(
        #         os.path.dirname(__file__), "saas_usage_pings/levels_sql_queries.json"
        #     )
        # ) as f:
        #     saas_queries = json.load(f)

        saas_queries = {
            "counts_monthly.deployments": {
                "query_base": "SELECT namespaces_xf.namespace_id as id, 123 as parent_namespace,  COUNT(deployments.id) AS counter_value  FROM prep.gitlab_dotcom.gitlab_dotcom_deployments_dedupe_source AS deployments  LEFT JOIN prep.gitlab_dotcom.gitlab_dotcom_projects_dedupe_source AS projects ON projects.id = deployments.project_id  LEFT JOIN prep.gitlab_dotcom.gitlab_dotcom_namespaces_dedupe_source AS namespaces ON projects.namespace_id = namespaces.id LEFT JOIN prod.legacy.gitlab_dotcom_namespaces_xf AS namespaces_xf ON namespaces.id = namespaces_xf.namespace_id WHERE deployments.created_at BETWEEN between_start_date AND between_end_date GROUP BY 1",
                "level": "namespace",
                "between": True,
            },
            "bad_query": {
                "query_base": "SELECT a bad query",
                "level": "project",
                "between": False,
            },
        }

        return saas_queries

    def saas_level_ping(self):
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
        saas_queries = self._get_level_queries()

        connection = self.sysadmin_engine.connect()

        for key, query_dict in saas_queries.items():
            base_query = query_dict.get("query_base")

            if query_dict.get("between", False):
                base_query = base_query.replace(
                    "between_end_date", f"'{str(self.end_date)}'"
                )
                base_query = base_query.replace(
                    "between_start_date", f"'{str(self.start_date_28)}'"
                )
            try:
                # Expecting [id, parent_namespace, counter_value]
                print(base_query)
                results = pd.read_sql(sql=base_query, con=connection)
            except SQLAlchemyError as e:
                error = str(e.__dict__["orig"])
                results = pd.DataFrame(
                    columns=["id", "parent_namespace", "counter_value"]
                )
                results.loc[0] = ["Error", "Error", error]

            results["ping_name"] = key
            results["level"] = query_dict.get("level", None)
            results["query_ran"] = base_query
            results["ping_date"] = self.end_date

            print(key, results)

            dataframe_uploader(
                results, self.sysadmin_engine, "gitlab_dotcom_level", "saas_usage_ping"
            )

        connection.close()
        self.sysadmin_engine.dispose()


if __name__ == "__main__":
    Fire(UsagePing)
