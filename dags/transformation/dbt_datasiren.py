import os
from datetime import date, datetime, timedelta

from airflow import DAG
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow_utils import (
    DBT_IMAGE,
    dbt_install_deps_nosha_cmd,
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

# Default arguments for the DAG
default_args = {
    "catchup": False,
    "depends_on_past": False,
    "on_failure_callback": slack_failed_task,
    "params": {"slack_channel_override": "#dbt-runs"},
    "owner": "airflow",
    "start_date": datetime(2020, 12, 8, 0, 0, 0),
}

# Runs every Saturday at 530
dag_schedule = "30 5 * * 6"

# Create the DAG
dag = DAG("dbt_datasiren", default_args=default_args, schedule_interval=dag_schedule)

dbt_datasiren_command = f"""
        {dbt_install_deps_nosha_cmd} &&
        dbt run --profiles-dir profile --target prod --models tag:datasiren; ret=$?;
        python ../../orchestration/upload_dbt_file_to_snowflake.py results; exit $ret
        """

KubernetesPodOperator(
    **gitlab_defaults,
    image=DBT_IMAGE,
    task_id=f"dbt-datasiren",
    name=f"dbt-datasiren",
    secrets=task_secrets,
    env_vars=pod_env_vars,
    arguments=[dbt_datasiren_command],
    dag=dag,
)

dbt_datasiren_audit_results_command = f"""
        {dbt_install_deps_nosha_cmd} &&
        dbt run --profiles-dir profile --target prod --models datasiren_audit_results+; ret=$?;
        python ../../orchestration/upload_dbt_file_to_snowflake.py results; exit $ret
        """

KubernetesPodOperator(
    **gitlab_defaults,
    image=DBT_IMAGE,
    task_id=f"dbt-datasiren-audit-results",
    name=f"dbt-datasiren-audit-results",
    secrets=task_secrets,
    env_vars=pod_env_vars,
    arguments=[dbt_datasiren_audit_results_command],
    dag=dag,
)
