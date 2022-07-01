"""
DAG for gcs external
"""
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow_utils import (
    DATA_IMAGE,
    gitlab_defaults,
    slack_failed_task,
    gitlab_pod_env_vars,
    clone_and_setup_extraction_cmd,
)
from kube_secrets import (
    SNOWFLAKE_ACCOUNT,
    SNOWFLAKE_LOAD_DATABASE,
    SNOWFLAKE_LOAD_ROLE,
    SNOWFLAKE_LOAD_USER,
    SNOWFLAKE_LOAD_WAREHOUSE,
    SNOWFLAKE_LOAD_PASSWORD,
)
from kubernetes_helpers import get_affinity, get_toleration

# Load the env vars into a dict and set Secrets
env = os.environ.copy()
GIT_BRANCH = env["GIT_BRANCH"]

# Default arguments for the DAG
default_args = {
    "catchup": False,
    "depends_on_past": False,
    "on_failure_callback": slack_failed_task,
    "owner": "airflow",
    "retries": 0,
    "retry_delay": timedelta(minutes=1),
    "start_date": datetime(2021, 11, 1),
    "dagrun_timeout": timedelta(hours=1),
}

# Create the DAG
dag = DAG("el_gcs_external", default_args=default_args, schedule_interval="0 3 * * *")

airflow_home = env["AIRFLOW_HOME"]

TASK_IDENTIFIER = "gcs-external-load"

run_load_command = f"""
  {clone_and_setup_extraction_cmd} &&
  export SNOWFLAKE_LOAD_WAREHOUSE="LOADING_XL" &&
  python3 /analytics/extract/gcs_external/src/gcs_external.py
  """

run_load = KubernetesPodOperator(
    **gitlab_defaults,
    image=DATA_IMAGE,
    task_id=TASK_IDENTIFIER,
    name=TASK_IDENTIFIER,
    secrets=[
        SNOWFLAKE_ACCOUNT,
        SNOWFLAKE_LOAD_DATABASE,
        SNOWFLAKE_LOAD_ROLE,
        SNOWFLAKE_LOAD_USER,
        SNOWFLAKE_LOAD_WAREHOUSE,
        SNOWFLAKE_LOAD_PASSWORD,
    ],
    env_vars={**gitlab_pod_env_vars, "PATH_DATE": "{{ yesterday_ds }}"},
    affinity=get_affinity(False),
    tolerations=get_toleration(False),
    arguments=[run_load_command],
    dag=dag,
)
