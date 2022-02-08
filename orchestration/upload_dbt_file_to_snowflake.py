import logging
import os
import sys
from os import environ as env
from reduce_file_size import reduce_manifest_file, save_json_file, load_json_file

from gitlabdata.orchestration_utils import (
    snowflake_engine_factory,
    snowflake_stage_load_copy_remove,
)

COLUMN_LIMIT_SIZE_SNOWFLAKE_MB = (
    14  # actually, it is 16MB, but to avoid corner case should put 14
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
        if config_name == "manifest":
            file_size_mb = (
                os.path.getsize(file_name) / 1024 / 1024
            )  # give me size in MBs

            if file_size_mb >= COLUMN_LIMIT_SIZE_SNOWFLAKE_MB:
                logging.info(
                    f"manifest file {file_name} "
                    f"is bigger than "
                    f"{COLUMN_LIMIT_SIZE_SNOWFLAKE_MB}, "
                    f"should be reduced."
                )

                raw_json = load_json_file(source_file=file_name)
                reduced_json = reduce_manifest_file(raw_json=raw_json)
                save_json_file(reduced_json=reduced_json, target_file=file_name)

                logging.info(f"manifest file {file_name} " f"reduced successfully.")

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
