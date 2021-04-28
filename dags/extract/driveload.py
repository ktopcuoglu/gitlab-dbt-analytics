import os
from datetime import datetime, timedelta
from yaml import load, safe_load, YAMLError

from airflow import DAG
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow_utils import (
    DATA_IMAGE,
    clone_and_setup_extraction_cmd,
    gitlab_defaults,
    gitlab_pod_env_vars,
    slack_failed_task,
)
from kube_secrets import (
    GCP_SERVICE_CREDS,
    SNOWFLAKE_ACCOUNT,
    SNOWFLAKE_LOAD_PASSWORD,
    SNOWFLAKE_LOAD_ROLE,
    SNOWFLAKE_LOAD_USER,
    SNOWFLAKE_LOAD_WAREHOUSE,
    SNOWFLAKE_LOAD_DATABASE,
)
from kubernetes_helpers import get_affinity, get_toleration

# Load the env vars into a dict and set Secrets
env = os.environ.copy()
GIT_BRANCH = env["GIT_BRANCH"]
pod_env_vars = gitlab_pod_env_vars

# Default arguments for the DAG
default_args = {
    "catchup": False,
    "depends_on_past": False,
    "on_failure_callback": slack_failed_task,
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
    "start_date": datetime(2019, 1, 1),
    "dagrun_timeout": timedelta(hours=2),
}

airflow_home = env["AIRFLOW_HOME"]

with open(f"{airflow_home}/analytics/extract/sheetload/drives.yml", "r") as file:
    try:
        stream = safe_load(file)
    except YAMLError as exc:
        print(exc)

    folders = [folder for folder in stream["folders"]]

runs = []

# Create the DAG
dag = DAG(
    "driveload",
    default_args=default_args,
    schedule_interval="0 2 */2 * *",
    concurrency=1,
)

for folder in folders:
    folder_name = folder.get("folder_name")
    table_name = folder.get("table_name")

    # Set the command for the container
    container_cmd = f"""
        {clone_and_setup_extraction_cmd} &&
        cd sheetload/ &&
        python3 sheetload.py drive --drive_file drives.yml --table_name {table_name}
    """

    cleaned_folder_name = folder_name.replace("_", "-")

    # Task 1
    folder_run = KubernetesPodOperator(
        **gitlab_defaults,
        image=DATA_IMAGE,
        task_id=f"{cleaned_folder_name}-driveload",
        name=f"{cleaned_folder_name}-driveload",
        secrets=[
            GCP_SERVICE_CREDS,
            SNOWFLAKE_ACCOUNT,
            SNOWFLAKE_LOAD_ROLE,
            SNOWFLAKE_LOAD_USER,
            SNOWFLAKE_LOAD_WAREHOUSE,
            SNOWFLAKE_LOAD_PASSWORD,
            SNOWFLAKE_LOAD_DATABASE,
        ],
        env_vars=pod_env_vars,
        affinity=get_affinity(False),
        tolerations=get_toleration(False),
        arguments=[container_cmd],
        dag=dag,
    )
    runs.append(folder_run)
