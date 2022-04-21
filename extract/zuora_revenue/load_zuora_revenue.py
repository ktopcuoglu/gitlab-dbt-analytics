import sys
import logging

from os import environ as env
from fire import Fire
from typing import Dict
from yaml import load, FullLoader
from datetime import datetime
from google.cloud import storage
from google.oauth2 import service_account
from google.cloud.storage.bucket import Bucket
from sqlalchemy.engine.base import Connection, Engine

from gitlabdata.orchestration_utils import (
    snowflake_engine_factory,
    query_executor,
)


def get_gcs_bucket(bucket_name: str) -> Bucket:
    """Do the auth and return a usable gcs bucket object."""

    scope = ["https://www.googleapis.com/auth/cloud-platform"]
    keyfile = load(env["GCP_SERVICE_CREDS"], Loader=FullLoader)
    credentials = service_account.Credentials.from_service_account_info(keyfile)
    scoped_credentials = credentials.with_scopes(scope)
    storage_client = storage.Client(credentials=scoped_credentials)
    return storage_client.get_bucket(bucket_name)


def move_to_processed(bucket: str, table_name: str, list_of_files: list):
    """
    Move the file to process folder.
    """
    # Get the gcloud storage client and authenticate
    source_bucket = get_gcs_bucket(bucket)
    destination_bucket = get_gcs_bucket(bucket)
    now = datetime.now()
    load_day = now.strftime("%d-%m-%Y")
    logging.info(list_of_files)
    for file_name in list_of_files:
        try:
            blob_name = "/".join(file_name.split("/")[3:])
            source_blob = source_bucket.blob(blob_name)
            file_name = file_name.split("/")[-1]
            destination_file_name = (
                f"RAW_DB/processed/{load_day}/{table_name}/{file_name}"
            )
            source_bucket.copy_blob(
                source_blob, destination_bucket, destination_file_name
            )
        except:
            logging.error(
                f"Source file {file_name} not found, Please ensure the direcotry is empty for next \
                            run else the file will be over written"
            )
            sys.exit(1)
        try:
            source_blob.delete()
        except:
            logging.error(
                f"{file_name} is not found , throwing this as error to ensure that we are not overwriting the files."
            )
            sys.exit(1)


def show_extraction_status(bucket: str, table_name: str):
    """
    This function is responsible for showing the extraction log in airflow task.
    It download todays run log and show it in the airflow task,post that move it to the processed folder.
    """
    log_file_name = f"RAW_DB/staging/{table_name}/{table_name}_{(datetime.now()).strftime('%d-%m-%Y')}.log"
    file_name = log_file_name.split("/")[-1]
    source_bucket = get_gcs_bucket(bucket)
    blob = source_bucket.blob(log_file_name)
    destination_bucket = get_gcs_bucket(bucket)
    now = datetime.now()
    load_day = now.strftime("%d-%m-%Y")
    destination_file_name = f"RAW_DB/processed/{load_day}/{table_name}/{file_name}"
    if blob.exists():
        logging.info(
            f"There has been successful run for table {table_name}.Below is the log content."
        )
        blob.download_to_filename(file_name)
        with open(file_name, "r") as log_file:
            logging.info(log_file.read())
        logging.info("Moving the log file to processed folder.")
        source_bucket.copy_blob(blob, destination_bucket, destination_file_name)
        logging.info("Deleteing the log file from staging folder")
        blob.delete()
    else:
        logging.error(
            f"Un successful extraction for table {table_name}.Please check the server"
        )
        sys.exit(1)


def table_truncate_to_daily_load(
    schema: str,
    table_name: str,
    conn_dict: Dict[str, str] = None,
) -> None:
    """
    To do the truncate and commit in the same session explicit connection has been created.
    Directly calling the truncate statement lead to the rollback of it and because of which the table was getting duplicate records.
    """
    begin_work = "begin work;"
    truncate_table = f"""TRUNCATE TABLE {table_name}"""
    end_work = "commit work"
    logging.info(truncate_table)
    engine = snowflake_engine_factory(conn_dict or env, "LOADER", schema)
    try:
        connection = engine.connect()
        connection.execute(begin_work)
        results = connection.execute(truncate_table).fetchall()
        connection.execute(end_work)
    finally:
        connection.close()
        engine.dispose()
    logging.info(results)


def zuora_revenue_load(
    bucket: str,
    schema: str,
    table_name: str,
    conn_dict: Dict[str, str] = None,
) -> None:

    """
    This function is responsible for checking if there has been extraction done today for this table.
    If Yes then it will load all the file present in the GCS folder under processed  for particular table and give number of rows loaded.
    Post that it will move all the file from GCS staging to processed folder for auditing purpose.
    """
    # Check if extraction is present for the table
    show_extraction_status(bucket, table_name)
    # Set some vars
    engine = snowflake_engine_factory(conn_dict or env, "LOADER", schema)

    # Truncate the table before every load
    table_truncate_to_daily_load(schema, table_name, conn_dict)

    upload_query = f"""
        copy into {table_name}
        from @zuora_revenue_staging/RAW_DB/staging/{table_name}/
        pattern= '.*{table_name}_.*[.]csv'
    """

    results = query_executor(engine, upload_query)
    total_rows = 0
    list_of_files = []
    logging.info(results)
    for result in results:
        if "0 files processed" in result[0]:
            logging.info(result[0])
            sys.exit(0)
        elif result[1] == "LOADED":
            total_rows += result[2]
            list_of_files.append(result[0])
        else:
            logging.error(result[0])
            sys.exit(1)
    logging.info(f"Loaded {total_rows} rows from {len(results)} files")
    logging.info(
        "Data file has been loaded. Move all the file to processed folder,to keep the directory clean."
    )
    move_to_processed(bucket, table_name, list_of_files)


if __name__ == "__main__":
    logging.basicConfig(stream=sys.stdout, level=20)
    logging.getLogger("snowflake.connector.cursor").disabled = True
    # Copy all environment variables to dict.
    config_dict = env.copy()
    Fire(
        {
            "zuora_load": zuora_revenue_load,
        }
    )
