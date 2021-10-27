import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow_utils import (
    DATA_IMAGE,
    clone_and_setup_extraction_cmd,
    gitlab_defaults,
    slack_failed_task,
    gitlab_pod_env_vars
)
from kube_secrets import (
    GCP_SERVICE_CREDS,
    SNOWFLAKE_ACCOUNT,
    SNOWFLAKE_LOAD_PASSWORD,
    SNOWFLAKE_LOAD_ROLE,
    SNOWFLAKE_LOAD_USER,
    SNOWFLAKE_LOAD_WAREHOUSE,
)
from kubernetes_helpers import get_affinity, get_toleration
# Load the env vars into a dict and set Secrets
env = os.environ.copy()
pod_env_vars = gitlab_pod_env_vars

# Default arguments for the DAG
default_args = {
    "catchup": False,
    "depends_on_past": False,
    "on_failure_callback": slack_failed_task,
    "owner": "airflow",
    "retries": 0,
    "retry_delay": timedelta(minutes=1),
    "sla": timedelta(hours=24),
    "sla_miss_callback": slack_failed_task,
    "start_date": datetime(2021, 6, 1),
    "dagrun_timeout": timedelta(hours=6),
}

airflow_home = env["AIRFLOW_HOME"]


# Create the DAG
dag = DAG(
    "zuora_data_query_extract_and_load", default_args=default_args, schedule_interval="0 */2 * * *"
)

# BambooHR Extract
bamboohr_extract_cmd = f"""
    {clone_and_setup_extraction_cmd} &&
    python zuora_data_query/src/api.py
"""

# having both xcom flag flavors since we're in an airflow version where one is being deprecated
bamboohr_extract = KubernetesPodOperator(
    **gitlab_defaults,
    image=DATA_IMAGE,
    task_id="bamboohr-extract",
    name="bamboohr-extract",
    secrets=[
        SNOWFLAKE_ACCOUNT,
        SNOWFLAKE_LOAD_ROLE,
        SNOWFLAKE_LOAD_USER,
        SNOWFLAKE_LOAD_WAREHOUSE,
        SNOWFLAKE_LOAD_PASSWORD,
    ],
    env_vars=pod_env_vars,
    affinity=get_affinity(False),
    tolerations=get_toleration(False),
    arguments=[bamboohr_extract_cmd],
    do_xcom_push=True,
    dag=dag,
)
