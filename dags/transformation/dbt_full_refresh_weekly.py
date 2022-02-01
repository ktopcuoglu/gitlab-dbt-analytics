"""
## Info about DAG
This DAG perform  full refresh of all the model and will be running only on Sunday.Currently only model excluded from full refresh is +gitlab_dotcom_usage_data_events+.
"""

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow_utils import (
    DBT_IMAGE,
    clone_repo_cmd,
    dbt_install_deps_cmd,
    gitlab_defaults,
    gitlab_pod_env_vars,
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

# This value is set based on the commit hash setter task in dbt_snapshot
pull_commit_hash = """export GIT_COMMIT="{{ var.value.dbt_hash }}" """
# Define all the  required secret

secrets_list = [
    GIT_DATA_TESTS_PRIVATE_KEY,
    GIT_DATA_TESTS_CONFIG,
    SALT,
    SALT_EMAIL,
    SALT_IP,
    SALT_NAME,
    SALT_PASSWORD,
    SNOWFLAKE_ACCOUNT,
    SNOWFLAKE_USER,
    SNOWFLAKE_PASSWORD,
    SNOWFLAKE_LOAD_PASSWORD,
    SNOWFLAKE_LOAD_ROLE,
    SNOWFLAKE_LOAD_USER,
    SNOWFLAKE_LOAD_WAREHOUSE,
    SNOWFLAKE_TRANSFORM_ROLE,
    SNOWFLAKE_TRANSFORM_WAREHOUSE,
    SNOWFLAKE_TRANSFORM_SCHEMA,
]
# Default arguments for the DAG
default_args = {
    "catchup": False,
    "depends_on_past": False,
    "on_failure_callback": slack_failed_task,
    "owner": "airflow",
    "retries": 0,
    "sla": timedelta(hours=8),
    "sla_miss_callback": slack_failed_task,
    "start_date": datetime(2019, 1, 1, 0, 0, 0),
    "trigger_rule": TriggerRule.ALL_DONE,
    "dagrun_timeout": timedelta(hours=6),
}

# Create the DAG
dag = DAG(
    "dbt_full_refresh_weekly",
    description="This DAG runs weekly on sunday running full refresh of all the model",
    default_args=default_args,
    schedule_interval="45 8 * * SUN",
)
dag.doc_md = __doc__

# dbt-full-refresh
dbt_full_refresh_cmd = f"""
    {pull_commit_hash} &&
    {dbt_install_deps_cmd} &&
    export SNOWFLAKE_TRANSFORM_WAREHOUSE="TRANSFORMING_XL" &&
    dbt run --profiles-dir profile --target prod --full-refresh --exclude tag:datasiren common.dim_ip_to_geo; ret=$?;
    python ../../orchestration/upload_dbt_file_to_snowflake.py results; exit $ret
"""
dbt_full_refresh = KubernetesPodOperator(
    **gitlab_defaults,
    image=DBT_IMAGE,
    task_id="dbt-full-refresh",
    name="dbt-full-refresh",
    secrets=secrets_list,
    env_vars=pod_env_vars,
    arguments=[dbt_full_refresh_cmd],
    dag=dag,
)

# dbt-test
dbt_test_cmd = f"""
    {pull_commit_hash} &&
    {dbt_install_deps_cmd} &&
    dbt test --profiles-dir profile --target prod --exclude tag:datasiren snowplow legacy.snapshots source:gitlab_dotcom source:salesforce source:zuora workspaces.*; ret=$?;
    python ../../orchestration/upload_dbt_file_to_snowflake.py manifest; $ret
    python ../../orchestration/upload_dbt_file_to_snowflake.py test; exit $ret
"""
dbt_test = KubernetesPodOperator(
    **gitlab_defaults,
    image=DBT_IMAGE,
    task_id="dbt-test",
    name="dbt-test",
    trigger_rule="all_done",
    secrets=secrets_list,
    env_vars=pod_env_vars,
    arguments=[dbt_test_cmd],
    dag=dag,
)

# dbt-workspaces-test
dbt_workspaces_test_command = f"""
    {pull_commit_hash} &&
    {dbt_install_deps_cmd} &&
    dbt test --profiles-dir profile --target prod --models workspaces.* ; ret=$?;
    python ../../orchestration/upload_dbt_file_to_snowflake.py test; exit $ret
"""
dbt_workspaces_test = KubernetesPodOperator(
    **gitlab_defaults,
    image=DBT_IMAGE,
    task_id="dbt-workspaces-test",
    name="dbt-workspaces-test",
    trigger_rule="all_done",
    secrets=secrets_list,
    env_vars=pod_env_vars,
    arguments=[dbt_workspaces_test_command],
    dag=dag,
)
# dbt-results
dbt_results_cmd = f"""
    {pull_commit_hash} &&
    {dbt_install_deps_cmd} &&
    dbt run --profiles-dir profile --target prod --models sources.dbt+ ; ret=$?;
    python ../../orchestration/upload_dbt_file_to_snowflake.py results; exit $ret
"""
dbt_results = KubernetesPodOperator(
    **gitlab_defaults,
    image=DBT_IMAGE,
    task_id="dbt-results",
    name="dbt-results",
    trigger_rule="all_done",
    secrets=secrets_list,
    env_vars=pod_env_vars,
    arguments=[dbt_results_cmd],
    dag=dag,
)

dbt_full_refresh >> dbt_test >> dbt_workspaces_test >> dbt_results
