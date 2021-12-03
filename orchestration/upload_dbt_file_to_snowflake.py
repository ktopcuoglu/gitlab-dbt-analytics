import logging
import os
import sys
from os import environ as env

from gitlabdata.orchestration_utils import (
    snowflake_engine_factory,
    snowflake_stage_load_copy_remove,
)


def get_file_name(config_name):
    if config_name == "freshness":
        return "target/sources.json"
    elif config_name == "manifest":
        return "target/manifest.json"
    else:
        return "target/run_results.json"


def get_table_name(config_name, snowflake_database):
    if config_name == "freshness":
        return f'"{snowflake_database}".dbt.sources'  # rename to source_freshness
    elif config_name == "source_tests":
        return f'"{snowflake_database}".dbt.source_tests_run_results'
    elif config_name == "snapshots":
        return f'"{snowflake_database}".dbt.snapshots_run_results'
    elif config_name == "test":
        return f'"{snowflake_database}".dbt.test_run_results'
    elif config_name == "manifest":
        return f'"{snowflake_database}".dbt.manifest'
    else:
        return f'"{snowflake_database}".dbt.run_results'


if __name__ == "__main__":
    config_name = sys.argv[1]
    file_name = get_file_name(config_name)
    config_dict = env.copy()
    snowflake_database = config_dict["SNOWFLAKE_LOAD_DATABASE"].upper()
    snowflake_engine = snowflake_engine_factory(config_dict, "LOADER")
    if os.path.exists(file_name):
        snowflake_stage_load_copy_remove(
            file_name,
            f"{snowflake_database}.dbt.dbt_load",
            get_table_name(config_name, snowflake_database),
            snowflake_engine,
        )
    else:
        logging.error(
            f"Dbt File {file_name} is missing. Check if dbt run completed successfully"
        )
