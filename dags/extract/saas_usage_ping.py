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

# tomorrow_ds -  the day after the execution date as YYYY-MM-DD
# ds - the execution date as YYYY-MM-DD
pod_env_vars = {
    "RUN_DATE": "{{ ds }}",
    "SNOWFLAKE_SYSADMIN_ROLE": "TRANSFORMER",
}

pod_env_vars = {**gitlab_pod_env_vars, **pod_env_vars}
pod_env_vars["BRANCH_NAME"] = env["GIT_BRANCH"].upper()

logging.info(pod_env_vars)

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
#  DAG will be triggered at 06:59am UTC which is 23:59 PM PST
dag = DAG(
    "saas_usage_ping", default_args=default_args, schedule_interval="0 7 * * *"
)

# Instance Level Usage Ping
instance_cmd = f"""
    {clone_repo_cmd} &&
    export PYTHONPATH="$CI_PROJECT_DIR/orchestration/:$PYTHONPATH" &&
    cd analytics/extract/ &&
    python3 usage_ping.py saas_instance_ping
"""

instance_ping = KubernetesPodOperator(
    **gitlab_defaults,
    image=DATA_IMAGE,
    task_id="saas-instance-usage-ping",
    name="saas-instance-usage-ping",
    secrets=secrets,
    env_vars=pod_env_vars,
    arguments=[instance_cmd],
    dag=dag,
)

# Namespace, Group, Project, User Level Usage Ping
levels_cmd = f"""
    {clone_repo_cmd} &&
    export PYTHONPATH="$CI_PROJECT_DIR/orchestration/:$PYTHONPATH" &&
    cd analytics/extract/ &&
    python3 usage_ping.py saas_level_ping --ping_date=$RUN_DATE
"""

levels_ping = KubernetesPodOperator(
    **gitlab_defaults,
    image=DATA_IMAGE,
    task_id="saas-levels-usage-ping",
    name="saas-levels-usage-ping",
    secrets=secrets,
    env_vars=pod_env_vars,
    arguments=[levels_cmd],
    dag=dag,
)
