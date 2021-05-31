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
from sqlalchemy.engine.base import Engine

from gitlabdata.orchestration_utils import (
    snowflake_engine_factory,
    query_executor,
)


def zuora_revenue_extract(table_name: str) -> None:
    subprocess.run("pip install paramiko",shell=True, check=True)
    import paramiko 
    logging.basicConfig(stream=sys.stdout, level=20)
    logging.info("Prepare the authentication URL and set the command for execution")
    zuora_revenue_auth_code=env["ZUORA_REVENUE_AUTH_CODE"]
    zuora_revenue_api_url = env["ZUORA_REVENUE_API_URL"]
    zuora_revenue_bucket_name=env["ZUORA_REVENUE_GCS_NAME"]
    zuora_revenue_compute_ip=env["ZUORA_REVENUE_COMPUTE_IP"]
    zuora_revenue_compute_username=env["ZUORA_REVENUE_COMPUTE_USERNAME"]
    zuora_revenue_compute_password=env["ZUORA_REVENUE_COMPUTE_PASSWORD"]
    connection = paramiko.SSHClient()
    connection.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    extract_command=f"$python_env;python3 /home/vedprakash/zuora_revenue/zuora_revenue/src/zuora_revenue_extract.py -table_name {table_name} \
                     -bucket_name {zuora_revenue_bucket_name} -api_dns_name {zuora_revenue_api_url} -api_auth_code {zuora_revenue_auth_code}"
    try:
        connection.connect(hostname=zuora_revenue_compute_ip, username=zuora_revenue_compute_username, password=zuora_revenue_compute_password)
        print('Connected')
        stdin,stdout,stderr=connection.exec_command(extract_command)
        exit_code = stdout.channel.recv_exit_status()
        stdout_raw = []
        for line in stdout:
            logging.info(line)

        stderr_raw = []
        for line in stderr:
            logging.info(line)

        logging.info(f"exit_code:{exit_code}")
        if exit_code == '0':
            logging.info("The extraction completed successfully")
        else:
            logging.error("Error in extraction")
            sys.exit(1)
        connection.close()
        del connection, stdin, stdout, stderr
    except Exception:
        raise

def move_to_processed(
    bucket: str, table_name: str, list_of_files: list, gapi_keyfile: str = None
):
    # Get the gcloud storage client and authenticate
    scope = ["https://www.googleapis.com/auth/cloud-platform"]
    keyfile = load(gapi_keyfile or env["GCP_SERVICE_CREDS"])
    credentials = service_account.Credentials.from_service_account_info(keyfile)
    scoped_credentials = credentials.with_scopes(scope)
    storage_client = storage.Client(credentials=scoped_credentials)
    source_bucket = storage_client.bucket(bucket)
    destination_bucket = storage_client.bucket(bucket)
    now = datetime.now()
    load_day = now.strftime("%m-%d-%Y")
    print(list_of_files)
    for file_name in list_of_files:
        try:
            blob_name = '/'.join(file_name.split("/")[3:])
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


def zuora_revenue_load(
    bucket: str,
    schema: str,
    table_name: str,
    conn_dict: Dict[str, str] = None,
) -> None:
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
            "zuora_extract": zuora_revenue_extract,
            "zuora_load": zuora_revenue_load,
        }
    )
