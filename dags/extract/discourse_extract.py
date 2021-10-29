import logging
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow_utils import (
    DATA_IMAGE,
    clone_and_setup_extraction_cmd,
    gitlab_defaults,
    slack_failed_task,
)

from kube_secrets import (
    DISCOURSE_API_TOKEN,
    SNOWFLAKE_ACCOUNT,
    SNOWFLAKE_LOAD_DATABASE,
    SNOWFLAKE_LOAD_PASSWORD,
    SNOWFLAKE_LOAD_ROLE,
    SNOWFLAKE_LOAD_USER,
    SNOWFLAKE_LOAD_WAREHOUSE,
)

from kubernetes_helpers import get_affinity, get_toleration

env = os.environ.copy()
pod_env_vars = {"CI_PROJECT_DIR": "/analytics"}

default_args = {
    "catchup": True,
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

dag = DAG(
    dag_id="discourse_extract",
    description="Monthly extract of Discourse analytics data",
    default_args=default_args,
    schedule_interval="0 7 1 * *",
)

# don't add a newline at the end of this because it gets added to in the K8sPodOperator arguments
extract_command = f"""{clone_and_setup_extraction_cmd} && 
    cd discourse/ && 
    python src/execute.py --reports_yml reports.yml --start_date $START_DATE --end_date $END_DATE --months_ago 1"""
logging.info(extract_command)

kubernetes_operator = KubernetesPodOperator(
    **gitlab_defaults,
    image=DATA_IMAGE,
    task_id="discourse-extract",
    name="discourse-extract",
    secrets=[
        DISCOURSE_API_TOKEN,
        SNOWFLAKE_ACCOUNT,
        SNOWFLAKE_LOAD_DATABASE,
        SNOWFLAKE_LOAD_ROLE,
        SNOWFLAKE_LOAD_USER,
        SNOWFLAKE_LOAD_WAREHOUSE,
        SNOWFLAKE_LOAD_PASSWORD,
    ],
    affinity=get_affinity(False),
    tolerations=get_toleration(False),
    env_vars={
        **pod_env_vars,
        **{
            "START_DATE": "{{ execution_date.isoformat() }}",
            "END_DATE": "{{ next_execution_date.isoformat() }}",
        },
    },
    arguments=[extract_command],
    dag=dag,
)
