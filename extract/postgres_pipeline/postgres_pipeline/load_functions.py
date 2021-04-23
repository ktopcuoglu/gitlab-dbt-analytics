import datetime
import logging
import os
import sys
from typing import Dict, Any, Optional

from gitlabdata.orchestration_utils import (
    snowflake_engine_factory,
    query_executor,
    append_to_xcom_file,
)
from sqlalchemy.engine.base import Engine

from utils import (
    chunk_and_upload,
    get_engines,
    id_query_generator,
    manifest_reader,
)


def get_last_load_time() -> Optional[datetime.datetime]:
    last_load_tstamp = os.environ["LAST_LOADED"]
    if last_load_tstamp != "":
        return datetime.datetime.strptime(last_load_tstamp, "%Y-%m-%dT%H:%M:%S%z")
    else:
        return None


def load_incremental(
    source_engine: Engine,
    target_engine: Engine,
    source_table_name: str,
    table_dict: Dict[Any, Any],
    table_name: str,
) -> bool:
    """
    Load tables incrementally based off of the execution date.
    """

    raw_query = table_dict["import_query"]
    additional_filter = table_dict.get("additional_filtering", "")

    env = os.environ.copy()

    """
      If postgres replication is too far behind for gitlab_com, then data will not be replicated in this DAGRun that
      will not be replicated in future DAGruns -- thus forcing the DE team to backfill.
      This block of code raises an Exception whenever replication is far enough behind that data will be missed.
    """
    if table_dict["export_schema"] == "gitlab_com":

        replication_check_query = "select pg_last_xact_replay_timestamp();"

        replication_timestamp = query_executor(source_engine, replication_check_query)[
            0
        ][0]

        last_load_time = get_last_load_time()

        hours_looking_back = int(env["HOURS"])

        try:
            execution_date = datetime.datetime.strptime(
                env["EXECUTION_DATE"], "%Y-%m-%dT%H:%M:%S%z"
            )
        except ValueError:
            execution_date = datetime.datetime.strptime(
                env["EXECUTION_DATE"], "%Y-%m-%dT%H:%M:%S.%f%z"
            )

        if last_load_time is not None:
            this_run_beginning_timestamp = last_load_time - datetime.timedelta(
                minutes=30
            )  # Allow for 30 minute overlap to ensure late arriving data is not skipped
        else:
            this_run_beginning_timestamp = execution_date - datetime.timedelta(
                hours=hours_looking_back
            )

        logging.info(f"Replication is at {replication_timestamp}")

        end_timestamp = min(
            replication_timestamp,
            execution_date,
            this_run_beginning_timestamp + datetime.timedelta(hours=hours_looking_back),
        )

        if this_run_beginning_timestamp > end_timestamp:
            raise Exception(
                "beginning timestamp is after end timestamp -- shouldn't be possible -- erroring"
            )

        append_to_xcom_file(
            {"max_data_available": end_timestamp.strftime("%Y-%m-%dT%H:%M:%S%z")}
        )

        env["BEGIN_TIMESTAMP"] = this_run_beginning_timestamp.strftime(
            "%Y-%m-%dT%H:%M:%S"
        )
        env["END_TIMESTAMP"] = end_timestamp.strftime("%Y-%m-%dT%H:%M:%S")

    # If _TEMP exists in the table name, skip it because it needs a full sync
    # If a temp table exists then it needs to finish syncing so don't load incrementally
    if "_TEMP" == table_name[-5:]:
        logging.info(
            f"Table {source_table_name} needs to be backfilled due to schema change, aborting incremental load."
        )
        return False
    query = f"{raw_query.format(**env)} {additional_filter}"

    chunk_and_upload(query, source_engine, target_engine, table_name, source_table_name)

    return True


def sync_incremental_ids(
    source_engine: Engine,
    target_engine: Engine,
    table: str,
    table_dict: Dict[Any, Any],
    table_name: str,
) -> bool:
    """
    Sync incrementally-loaded tables based on their IDs.
    """

    raw_query = table_dict["import_query"]
    additional_filtering = table_dict.get("additional_filtering", "")
    primary_key = table_dict["export_table_primary_key"]
    # If temp isn't in the name, we don't need to full sync.
    # If a temp table exists, we know the sync didn't complete successfully
    if "_TEMP" != table_name[-5:]:
        logging.info(f"Table {table} doesn't need a full sync.")
        return False

    load_ids(
        additional_filtering,
        primary_key,
        raw_query,
        source_engine,
        table,
        table_name,
        target_engine,
    )
    return True


def load_scd(
    source_engine: Engine,
    target_engine: Engine,
    source_table_name: str,
    table_dict: Dict[Any, Any],
    table_name: str,
    is_append_only: bool = False,
) -> bool:
    """
    Load tables that are slow-changing dimensions.
    """

    # If the schema has changed for the SCD table, treat it like a backfill
    if "_TEMP" == table_name[-5:] or target_engine.has_table(f"{table_name}_TEMP"):
        logging.info(
            f"Table {source_table_name} needs to be recreated to due to schema change. Recreating...."
        )
        backfill = True
    else:
        backfill = False

    raw_query = table_dict["import_query"]
    additional_filter = table_dict.get("additional_filtering", "")
    advanced_metadata = table_dict.get("advanced_metadata", False)

    logging.info(f"Processing table: {source_table_name}")
    query = f"{raw_query} {additional_filter}"

    if is_append_only:
        load_ids(
            additional_filter,
            table_dict["export_table_primary_key"],
            raw_query,
            source_engine,
            source_table_name,
            table_name,
            target_engine,
            backfill=backfill,
        )
        return True

    logging.info(query)
    chunk_and_upload(
        query,
        source_engine,
        target_engine,
        table_name,
        source_table_name,
        advanced_metadata,
        backfill,
    )
    return True


def load_ids(
    additional_filtering: str,
    primary_key: str,
    raw_query: str,
    source_engine: Engine,
    source_table_name: str,
    table_name: str,
    target_engine: Engine,
    id_range: int = 750_000,
    backfill: bool = True,
) -> None:
    """ Load a query by chunks of IDs instead of all at once."""

    # Create a generator for queries that are chunked by ID range
    id_queries = id_query_generator(
        source_engine,
        primary_key,
        raw_query,
        target_engine,
        source_table_name,
        table_name,
        id_range=id_range,
    )
    # Iterate through the generated queries
    for query in id_queries:
        filtered_query = f"{query} {additional_filtering} ORDER BY {primary_key}"
        logging.info(filtered_query)
        chunk_and_upload(
            filtered_query,
            source_engine,
            target_engine,
            table_name,
            source_table_name,
            backfill=backfill,
        )
        backfill = False  # this prevents it from seeding rows for every chunk


def check_new_tables(
    source_engine: Engine,
    target_engine: Engine,
    table: str,
    table_dict: Dict[Any, Any],
    table_name: str,
) -> bool:
    """
    Load a set amount of rows for each new table in the manifest. A table is
    considered new if it doesn't already exist in the data warehouse.
    """

    raw_query = table_dict["import_query"].split("WHERE")[0]
    additional_filtering = table_dict.get("additional_filtering", "")
    advanced_metadata = table_dict.get("advanced_metadata", False)
    primary_key = table_dict["export_table_primary_key"]

    # Figure out if the table exists
    if "_TEMP" != table_name[-5:] and not target_engine.has_table(f"{table_name}_TEMP"):
        logging.info(f"Table {table} already exists and won't be tested.")
        return False

    # If the table doesn't exist, load whatever the table has
    query = f"{raw_query} WHERE {primary_key} IS NOT NULL {additional_filtering} LIMIT 100000"
    chunk_and_upload(
        query,
        source_engine,
        target_engine,
        table_name,
        table,
        advanced_metadata,
        backfill=True,
    )

    return True
