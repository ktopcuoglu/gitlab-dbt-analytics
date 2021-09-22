from os import environ as env

from gitlabdata.orchestration_utils import (
    snowflake_engine_factory,
)
from fire import Fire
from query_utils import rollup_table_clone
from typing import Dict
import logging
import sys


def rollup_table_clones(
    table_name: str,
    db_name: str = "RAW",
    schema: str = "FULL_TABLE_CLONES",
    conn_dict: Dict[str, str] = None,
):
    logging.info(f"Rolling up table {table_name} clones")
    engine = snowflake_engine_factory(conn_dict or env, "LOADER", schema)
    rollup_table_clone(engine, db_name, schema, table_name)
    logging.info("Table clones rolled up")


if __name__ == "__main__":
    logging.basicConfig(stream=sys.stdout, level=20)
    logging.getLogger("snowflake.connector.cursor").disabled = True
    Fire(
        {
            "rollup_full_table_clones": rollup_table_clones,
        }
    )
