"""
DAG for Snowplow -> PostHog backfilling
"""

import os
from datetime import date, datetime

from airflow import DAG
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow_utils import (
    clone_and_setup_extraction_cmd,
    DBT_IMAGE,
    gitlab_defaults,
    gitlab_pod_env_vars,
    partitions,
    slack_failed_task,
)
from kube_secrets import (
    GIT_DATA_TESTS_PRIVATE_KEY,
    GIT_DATA_TESTS_CONFIG,
    SALT,
    SALT_EMAIL,
    SALT_IP,
    SALT_NAME,
    SALT_PASSWORD,
    SNOWFLAKE_ACCOUNT,
    SNOWFLAKE_PASSWORD,
    SNOWFLAKE_TRANSFORM_ROLE,
    SNOWFLAKE_TRANSFORM_SCHEMA,
    SNOWFLAKE_TRANSFORM_WAREHOUSE,
    SNOWFLAKE_USER,
    SNOWFLAKE_LOAD_PASSWORD,
    SNOWFLAKE_LOAD_ROLE,
    SNOWFLAKE_LOAD_USER,
    SNOWFLAKE_LOAD_WAREHOUSE,
)



# Load the env vars into a dict and set Secrets
env = os.environ.copy()
GIT_BRANCH = env["GIT_BRANCH"]
pod_env_vars = {**gitlab_pod_env_vars, **{}}


task_secrets = [
    GIT_DATA_TESTS_PRIVATE_KEY,
    GIT_DATA_TESTS_CONFIG,
    SALT,
    SALT_EMAIL,
    SALT_IP,
    SALT_NAME,
    SALT_PASSWORD,
    SNOWFLAKE_ACCOUNT,
    SNOWFLAKE_PASSWORD,
    SNOWFLAKE_TRANSFORM_ROLE,
    SNOWFLAKE_TRANSFORM_SCHEMA,
    SNOWFLAKE_TRANSFORM_WAREHOUSE,
    SNOWFLAKE_USER,
    SNOWFLAKE_LOAD_PASSWORD,
    SNOWFLAKE_LOAD_ROLE,
    SNOWFLAKE_LOAD_USER,
    SNOWFLAKE_LOAD_WAREHOUSE,
]

start_date = datetime(2022, 1, 1, 0, 0, 0)
DAG_NAME = "snowplow_posthog_backfill"
SCHEMA_NAME = "gitlab_events"

# Default arguments for the DAG
default_args = {
    "catchup": False,
    "depends_on_past": False,
    "on_failure_callback": slack_failed_task,
    "owner": "airflow",
    "start_date": start_date,
}


# Create the DAG
def generate_dbt_command(vars_dict: dict, dag_name: str):
    """
    Generate generic command separated per time frame
    to create tasks
    """

    generated_command = f"""
    {clone_and_setup_extraction_cmd} &&
    python3 snowplow_posthog/backfill.py snowplow_posthog_backfill --month {vars_dict['year']}{vars_dict['month']}
"""

    return KubernetesPodOperator(
        **gitlab_defaults,
        image=DBT_IMAGE,
        task_id=f"{dag_name}-{vars_dict['year']}-{vars_dict['month']}",
        name=f"{dag_name}-{vars_dict['year']}-{vars_dict['month']}",
        secrets=task_secrets,
        env_vars=pod_env_vars,
        arguments=[generated_command],
        dag=dag,
    )


dag = DAG(
    DAG_NAME,
    default_args=default_args,
    schedule_interval=None,
    concurrency=2,
)

for month in partitions(start_date.date(), date.today(), "month"):
    generate_dbt_command(month, dag_name=DAG_NAME)
