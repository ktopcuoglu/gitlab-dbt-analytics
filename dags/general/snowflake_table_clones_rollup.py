import logging
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow_utils import (
    DATA_IMAGE,
    clone_repo_cmd,
    gitlab_defaults,
    slack_failed_task,
    gitlab_pod_env_vars,
    clone_and_setup_extraction_cmd,
)
from kube_secrets import (
    SNOWFLAKE_ACCOUNT,
    SNOWFLAKE_LOAD_ROLE,
    SNOWFLAKE_LOAD_USER,
    SNOWFLAKE_LOAD_WAREHOUSE,
    SNOWFLAKE_LOAD_PASSWORD,
    SNOWFLAKE_PASSWORD,
    SNOWFLAKE_USER,
)

# Load the env vars into a dict and set env vars
env = os.environ.copy()
GIT_BRANCH = env["GIT_BRANCH"]

# CLONE_DATE will be used to set the timestamp of when clone should
# CLONE_NAME_DATE date formatted to string to be used for clone name
# tomorrow_ds -  the day after the execution date as YYYY-MM-DD
# ds_nodash - the execution date as YYYYMMDD
pod_env_vars = {
    "CLONE_NAME_DATE": "{{ ds_nodash }}",
    "SNOWFLAKE_SYSADMIN_ROLE": "TRANSFORMER",
}

pod_env_vars = {**gitlab_pod_env_vars, **pod_env_vars}
pod_env_vars["BRANCH_NAME"] = env["GIT_BRANCH"].upper()

secrets = [
    SNOWFLAKE_ACCOUNT,
    SNOWFLAKE_LOAD_ROLE,
    SNOWFLAKE_LOAD_USER,
    SNOWFLAKE_LOAD_WAREHOUSE,
    SNOWFLAKE_LOAD_PASSWORD,
    SNOWFLAKE_PASSWORD,
    SNOWFLAKE_USER,
]

# Default arguments for the DAG
default_args = {
    "catchup": False,
    "depends_on_past": False,
    "on_failure_callback": slack_failed_task,
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
    "start_date": datetime(2020, 6, 7),
    "dagrun_timeout": timedelta(hours=2),
}

# Create the DAG
dag = DAG(
    "snowflake_table_clone_rollup",
    default_args=default_args,
    schedule_interval="0 12 9 * *",
)
tables_to_rollup = [
    "MART_ARR",
]


for table in tables_to_rollup:
    # Set the command for the container
    container_cmd = f"""
        {clone_and_setup_extraction_cmd} &&
        cd rollup_table_clones/src &&
        python3 execute.py rollup_full_table_clones --table_name {table}
    """

    cleaned_table_name = table.replace("_", "-").lower()

    make_clone = KubernetesPodOperator(
        **gitlab_defaults,
        image=DATA_IMAGE,
        task_id=f"{cleaned_table_name}-clone-rollup",
        name=f"{cleaned_table_name}-clone-rollup",
        secrets=secrets,
        env_vars=pod_env_vars,
        arguments=[container_cmd],
        dag=dag,
    )
