import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from kubernetes_helpers import get_affinity, get_toleration
from airflow_utils import (
    DATA_IMAGE,
    clone_and_setup_extraction_cmd,
    gitlab_defaults,
    slack_failed_task,
    gitlab_pod_env_vars,
)
from kube_secrets import (
    SNOWFLAKE_ACCOUNT,
    SNOWFLAKE_LOAD_DATABASE,
    SNOWFLAKE_LOAD_PASSWORD,
    SNOWFLAKE_LOAD_ROLE,
    SNOWFLAKE_LOAD_USER,
    SNOWFLAKE_LOAD_WAREHOUSE,
)


# Load the env vars into a dict and set Secrets
env = os.environ.copy()

# Default arguments for the DAG
default_args = {
    "catchup": False,
    "depends_on_past": False,
    "on_failure_callback": slack_failed_task,
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
    "sla": timedelta(hours=12),
    "sla_miss_callback": slack_failed_task,
    "start_date": datetime(2019, 1, 1),
    "dagrun_timeout": timedelta(hours=6),
}

# Create the DAG
dag = DAG(
    "keyhole_twitter_extract",
    default_args=default_args,
    schedule_interval="00 16 * * MON",
)

# Keyhole Twitter Extract
keyhole_twitter_extract_cmd = f"""
    {clone_and_setup_extraction_cmd} &&
    python3 keyhole_twitter/src/execute.py && 
    python3 sheetload/sheetload.py csv --filename social_twitter_impressions.csv --schema keyhole_twitter --tablename impressions
"""

keyhole_twitter_extract_cmd = KubernetesPodOperator(
    **gitlab_defaults,
    image=DATA_IMAGE,
    task_id="twitter-impressions-extract",
    name="twitter-impressions-extract",
    secrets=[
        SNOWFLAKE_ACCOUNT,
        SNOWFLAKE_LOAD_DATABASE,
        SNOWFLAKE_LOAD_ROLE,
        SNOWFLAKE_LOAD_USER,
        SNOWFLAKE_LOAD_WAREHOUSE,
        SNOWFLAKE_LOAD_PASSWORD,
    ],
    env_vars=gitlab_pod_env_vars,
    affinity=get_affinity(False),
    tolerations=get_toleration(False),
    arguments=[keyhole_twitter_extract_cmd],
    dag=dag,
)
