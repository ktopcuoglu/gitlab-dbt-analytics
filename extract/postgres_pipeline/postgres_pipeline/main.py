import datetime
import logging
import os
import sys
from typing import Dict, Any

from fire import Fire
from gitlabdata.orchestration_utils import (
    snowflake_engine_factory,
    query_executor,
    append_to_xcom_file,
)
from sqlalchemy.engine.base import Engine
from postgres_pipeline_table import PostgresPipelineTable
from utils import (
    check_if_schema_changed,
    chunk_and_upload,
    get_engines,
    id_query_generator,
    manifest_reader,
)


SCHEMA = "tap_postgres"


def swap_temp_table(engine: Engine, real_table: str, temp_table: str) -> None:
    """
    Drop the real table and rename the temp table to take the place of the
    real table.
    """

    if engine.has_table(real_table):
        logging.info(
            f"Swapping the temp table: {temp_table} with the real table: {real_table}"
        )
        swap_query = f"ALTER TABLE IF EXISTS tap_postgres.{temp_table} SWAP WITH tap_postgres.{real_table}"
        query_executor(engine, swap_query)
    else:
        logging.info(f"Renaming the temp table: {temp_table} to {real_table}")
        rename_query = f"ALTER TABLE IF EXISTS tap_postgres.{temp_table} RENAME TO tap_postgres.{real_table}"
        query_executor(engine, rename_query)

    drop_query = f"DROP TABLE IF EXISTS tap_postgres.{temp_table}"
    query_executor(engine, drop_query)


def filter_manifest(manifest_dict: Dict, load_only_table: str = None) -> None:
    # When load_only_table specified reduce manifest to keep only relevant table config
    if load_only_table and load_only_table in manifest_dict["tables"].keys():
        manifest_dict["tables"] = {
            load_only_table: manifest_dict["tables"][load_only_table]
        }


def main(file_path: str, load_type: str, load_only_table: str = None) -> None:
    """
    Read data from a postgres DB and upload it directly to Snowflake.
    """

    # Process the manifest
    logging.info(f"Reading manifest at location: {file_path}")
    manifest_dict = manifest_reader(file_path)
    # When load_only_table specified reduce manifest to keep only relevant table config
    filter_manifest(manifest_dict, load_only_table)

    postgres_engine, snowflake_engine = get_engines(manifest_dict["connection_info"])
    logging.info(snowflake_engine)

    for table in manifest_dict["tables"]:
        logging.info(f"Processing Table: {table}")
        table_dict = manifest_dict["tables"][table]
        current_table = PostgresPipelineTable(table_dict)

        # Check if the schema has changed or the table is new
        schema_changed = current_table.check_if_schema_changed(
            postgres_engine, snowflake_engine
        )

        # Call the correct function based on the load_type
        loaded = current_table.do_load(
            load_type, postgres_engine, snowflake_engine, schema_changed
        )
        logging.info(f"Finished upload for table: {table}")

        # Drop the original table and rename the temp table
        if schema_changed and loaded:
            swap_temp_table(
                snowflake_engine,
                current_table.get_target_table_name(),
                current_table.get_temp_target_table_name(),
            )

        count_query = f"SELECT COUNT(*) FROM {current_table.get_target_table_name()}"
        count = 0

        try:
            count = query_executor(snowflake_engine, count_query)[0][0]
        except:
            pass  # likely that the table doesn't exist -- don't want an error here to stop the task

        append_to_xcom_file(
            {current_table.get_target_table_name(): count, "load_ran": loaded}
        )


def dq_main(file_path: str, load_type: str, load_only_table: str = None) -> None:
    """
    Read data from a postgres DB and upload it directly to Snowflake.
    """

    # Process the manifest
    logging.info(f"Reading manifest at location: {file_path}")
    manifest_dict = manifest_reader(file_path)
    # When load_only_table specified reduce manifest to keep only relevant table c
    filter_manifest(manifest_dict, load_only_table)

    postgres_engine, snowflake_engine = get_engines(manifest_dict["connection_info"])
    logging.info(snowflake_engine)

    for table in manifest_dict["tables"]:
        logging.info(f"Processing Table: {table}")
        table_dict = manifest_dict["tables"][table]
        current_table = PostgresPipelineTable(table_dict)

        # Call the correct function based on the load_type
        loaded = current_table.do_load(
            load_type, postgres_engine, snowflake_engine, False
        )
        logging.info(f"Finished upload for table: {table}")

        try:
            count = query_executor(snowflake_engine, count_query)[0][0]
        except:
            pass  # likely that the table doesn't exist -- don't want an error her

        append_to_xcom_file(
            {current_table.get_target_table_name(): count, "load_ran": loaded}
        )


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    logging.getLogger("snowflake.connector.cursor").disabled = True
    logging.getLogger("snowflake.connector.connection").disabled = True
    Fire({"tap": main,
         "tap_dq":dq_main})
