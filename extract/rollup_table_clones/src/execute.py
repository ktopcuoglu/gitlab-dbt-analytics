from os import environ as env

from gitlabdata.orchestration_utils import (
    snowflake_engine_factory,
)
from fire import Fire
from query_utils import rollup_table_clone
from typing import Dict

config_dict = env.copy()

def rollup_table_clones(
        table_name: str,
        db_name: str = "RAW",
        schema: str = "FULL_TABLE_CLONES",
        gapi_keyfile: str = None,
        conn_dict: Dict[str, str] = None,
    ):
    engine = snowflake_engine_factory(conn_dict or env, "LOADER", schema)
    rollup_table_clone(engine, db_name, schema, table_name)

if __name__ == "main":
    Fire(
        {
            "rollup_full_table_clones": rollup_table_clones,
        }
    )