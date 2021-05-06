import os
from datetime import datetime, timedelta
from yaml import load, safe_load, YAMLError
from airflow import DAG
from os import environ as env
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow_utils import (
    DATA_IMAGE,
    clone_and_setup_extraction_cmd,
    gitlab_defaults,
    slack_failed_task,
)
from kube_secrets import (
    GCP_SERVICE_CREDS,
    ZUORA_REVENUE_GCS_NAME,
    ZUORA_REVENUE_API_URL,
    ZUORA_REVENUE_AUTH_CODE,
    SNOWFLAKE_ACCOUNT,
    SNOWFLAKE_LOAD_PASSWORD,
    SNOWFLAKE_LOAD_ROLE,
    SNOWFLAKE_LOAD_USER,
    SNOWFLAKE_LOAD_WAREHOUSE,
)
from kubernetes_helpers import get_affinity, get_toleration

# Load the env vars into a dict and set Secrets
env = os.environ.copy()
GIT_BRANCH = env["GIT_BRANCH"]
pod_env_vars = {
    "SNOWFLAKE_LOAD_DATABASE": "RAW"
    if GIT_BRANCH == "master"
    else f"{GIT_BRANCH.upper()}_RAW",
    "CI_PROJECT_DIR": "/analytics",
}

# Default arguments for the DAG
default_args = {
    "catchup": False,
    "depends_on_past": False,
    "on_failure_callback": slack_failed_task,
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
    "sla": timedelta(hours=24),
    "sla_miss_callback": slack_failed_task,
    "start_date": datetime(2019, 1, 1),
    "dagrun_timeout": timedelta(hours=6),
}

airflow_home = env["AIRFLOW_HOME"]

# Get all the table name for which tasks for loading needs to be created
with open(
    f"{airflow_home}/analytics/extract/zuora_revenue/zuora_revenue_table_name.yml", "r"
) as file:
    try:
        stream = safe_load(file)
    except YAMLError as exc:
        print(exc)

    table_name_list = [
        "{table_name}".format(table_name=tab)
        for sheet in stream["table_info"]
        for tab in sheet["table_name"]
    ]

runs = []

# Create the DAG  with one load happening at once
dag = DAG(
    "zuora_revenue_load_extract",
    default_args=default_args,
    schedule_interval="0 10 * * 0",
    concurrency=1,
)

for table_name in table_name_list:
    # Set the command for the container
    container_cmd = f"""
        {clone_and_setup_extraction_cmd} &&
        python3 zuora_revenue/zuora_extract_load.py zuora_load --bucket $ZUORA_REVENUE_BUCKET_NAME --schema_name zuora_revenue --table_name {table_name}
        """
    # Task 1
    zuora_revenue_load_run = KubernetesPodOperator(
        **gitlab_defaults,
        image=DATA_IMAGE,
        task_id=f"{table_name}_LOAD",
        name=f"{table_name}_LOAD",
        secrets=[
            ZUORA_REVENUE_GCS_NAME,
            GCP_SERVICE_CREDS,
            SNOWFLAKE_ACCOUNT,
            SNOWFLAKE_LOAD_ROLE,
            SNOWFLAKE_LOAD_USER,
            SNOWFLAKE_LOAD_WAREHOUSE,
            SNOWFLAKE_LOAD_PASSWORD,
        ],
        env_vars=pod_env_vars,
        affinity=get_affinity(False),
        tolerations=get_toleration(False),
        arguments=[container_cmd],
        dag=dag,
    )
    runs.append(zuora_revenue_load_run)
