import logging
import os
from datetime import date, datetime, timedelta


from airflow import DAG
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow_utils import (
    DATA_IMAGE,
    clone_repo_cmd,
    gitlab_defaults,
    slack_failed_task,
    gitlab_pod_env_vars,
    clone_and_setup_extraction_cmd,
    partitions,
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

secrets = [
    SNOWFLAKE_ACCOUNT,
    SNOWFLAKE_LOAD_ROLE,
    SNOWFLAKE_LOAD_USER,
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
    "retries": 0,
    "start_date": datetime(2020, 6, 7),
    "dagrun_timeout": timedelta(hours=8),
}

# Create the DAG
#  Sunday at 0900 UTC
dag = DAG("saas_usage_ping_backfill", default_args=default_args, schedule_interval=None)


def generate_task(vars_dict):

    run_date = date(year=int(vars_dict["year"]), month=int(vars_dict["month"]), day=1)

    run_date_formatted = run_date.isoformat()

    pod_env_vars = {
        "RUN_DATE": run_date_formatted,
        "SNOWFLAKE_SYSADMIN_ROLE": "TRANSFORMER",
        "SNOWFLAKE_LOAD_WAREHOUSE": "USAGE_PING",
    }

    pod_env_vars = {**gitlab_pod_env_vars, **pod_env_vars}

    # Namespace, Group, Project, User Level Usage Ping
    namespace_cmd = f"""
        {clone_repo_cmd} &&
        cd analytics/extract/saas_usage_ping/ &&
        python3 usage_ping.py backfill --ping_date=$RUN_DATE
    """

    KubernetesPodOperator(
        **gitlab_defaults,
        image=DATA_IMAGE,
        task_id=f"saas-namespace-usage-ping-{vars_dict['year']}-{vars_dict['month']}",
        name=f"saas-namespace-usage-ping-{vars_dict['year']}-{vars_dict['month']}",
        secrets=secrets,
        env_vars=pod_env_vars,
        arguments=[namespace_cmd],
        dag=dag,
    )


for month in partitions(
    date.today() - timedelta(days=365),
    date.today().replace(day=1) - timedelta(days=1),
    "month",
):
    generate_task(month)
