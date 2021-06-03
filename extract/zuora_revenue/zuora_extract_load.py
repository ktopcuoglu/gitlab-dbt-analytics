import sys
from os import environ as env
import time
import subprocess
import logging
from fire import Fire
from typing import Dict, Tuple, List
from yaml import load, safe_load, YAMLError

import pandas as pd
from datetime import datetime

from google.cloud import storage
from google.oauth2 import service_account
from google.cloud.storage.bucket import Bucket
from sqlalchemy.engine.base import Engine

from gitlabdata.orchestration_utils import (
    snowflake_engine_factory,
    query_executor,
)

"""
def zuora_revenue_extract(table_name: str) -> None:
    subprocess.run("pip install paramiko", shell=True, check=True)
    import paramiko

    logging.basicConfig(stream=sys.stdout, level=20)
    logging.info("Prepare the authentication URL and set the command for execution")
    zuora_revenue_auth_code = f'Basic {env["ZUORA_REVENUE_AUTH_CODE"]}'
    zuora_revenue_api_url = env["ZUORA_REVENUE_API_URL"]
    zuora_revenue_bucket_name = env["ZUORA_REVENUE_GCS_NAME"]
    zuora_revenue_compute_ip = env["ZUORA_REVENUE_COMPUTE_IP"]
    zuora_revenue_compute_username = env["ZUORA_REVENUE_COMPUTE_USERNAME"]
    zuora_revenue_compute_password = env["ZUORA_REVENUE_COMPUTE_PASSWORD"]
    connection = paramiko.SSHClient()
    connection.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    extract_command = f"$python_env;cd /home/vedprakash/zuora_revenue/zuora_revenue/src/;python3 zuora_revenue_extract.py -table_name {table_name} \
                     -bucket_name {zuora_revenue_bucket_name} -api_dns_name {zuora_revenue_api_url} -api_auth_code '{zuora_revenue_auth_code}'"
    try:
        connection.connect(
            hostname=zuora_revenue_compute_ip,
            username=zuora_revenue_compute_username,
            password=zuora_revenue_compute_password,
        )
        logging.info("Connected")
        stdin, stdout, stderr = connection.exec_command(extract_command)
        exit_code = stdout.channel.recv_exit_status()
        stdout_raw = []
        for line in stdout:
            logging.info(line)

        stderr_raw = []
        for line in stderr:
            logging.info(line)

        logging.info(f"exit_code:{exit_code}")
        if exit_code == 0:
            logging.info("The extraction completed successfully")
        else:
            logging.error("Error in extraction")
            sys.exit(1)
        connection.close()
        del connection, stdin, stdout, stderr
    except Exception:
        raise
"""


def get_gcs_bucket(bucket_name: str) -> Bucket:
    """Do the auth and return a usable gcs bucket object."""

    scope = ["https://www.googleapis.com/auth/cloud-platform"]
    keyfile = load(env["GCP_SERVICE_CREDS"])
    credentials = service_account.Credentials.from_service_account_info(keyfile)
    scoped_credentials = credentials.with_scopes(scope)
    storage_client = storage.Client(credentials=scoped_credentials)
    return storage_client.get_bucket(bucket_name)


def move_to_processed(bucket: str, table_name: str, list_of_files: list):
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
            logging.info(log_file.readlines())
        source_bucket.copy_blob(blob, destination_bucket, destination_file_name)
    else:
        logging.error(
            f"Un successful extraction for table {table_name}.Please check the server"
        )
        sys.exit(1)


def zuora_revenue_load(
    bucket: str,
    schema: str,
    table_name: str,
    conn_dict: Dict[str, str] = None,
) -> None:
    # Check if extraction is present for the table
    show_extraction_status(bucket, table_name)
    # Set some vars
    engine = snowflake_engine_factory(conn_dict or env, "LOADER", schema)

    upload_query = f"""
        copy into {table_name}
        from @zuora_revenue_staging/RAW_DB/staging/{table_name}
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
    log_result = f"Loaded {total_rows} rows from {len(results)} files"
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
            # "zuora_extract": zuora_revenue_extract,
            "zuora_load": zuora_revenue_load,
        }
    )
